# -*- coding: utf-8 -*-
"""
機械課 部品所要日程（Pythonのみ）
- child_requirements.csv（日別所要）を使い、欠品日・納品期日・不足数・注残・塗装できる数を算出

DB接続:
- BAAN系: DSN=TRKBAAN （テーブル名は TTIROU001002 等）
- ASS系 : DSN=ASS     （tbl_ItemTheory, 外部倉庫在庫）

出力:
- 機械課_所要日程.csv (UTF-8-sig)
"""

from __future__ import annotations
from datetime import date
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import pyodbc


# =========================
# 設定
# =========================

# BAAN（TRKBAAN）
CONN_BAAN = "DSN=TRKBAAN;UID=odbc;PWD=odbc;Trusted_Connection=No;APP=Microsoft Office;"

# ASS（棚理論/外部倉庫）
CONN_ASS = "DSN=ASS;UID=ass;PWD=ass;DATABASE=ASS"

# 入出力
INPUT_CHILD_REQ_CSV = r"\\192.168.134.32\share\00_データ\01_所要計算(組立自動日程)\child_requirements.csv"
OUTPUT_CSV = r"\\192.168.134.32\share\00_データ\01_所要計算(組立自動日程)\kikaikaSchedule.csv"

# CSVエンコーディング優先順（あなたの好み）
ENCODING_CANDIDATES = ["utf-8-sig", "cp932", "shift_jis", "latin1"]


# =========================
# ユーティリティ
# =========================

def read_csv_robust(path: str) -> pd.DataFrame:
    last_err = None
    for enc in ENCODING_CANDIDATES:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"CSVを読み込めません: {path} / last_error={last_err}")

def normalize_item(x: object) -> str:
    if pd.isna(x):
        return ""
    return str(x).replace(" ", "").replace("\u3000", "")

def parse_date_col(col: str) -> Optional[date]:
    try:
        return pd.to_datetime(col).date()
    except Exception:
        return None

def fetch_df(conn_str: str, sql: str, params: Tuple = ()) -> pd.DataFrame:
    with pyodbc.connect(conn_str) as conn:
        return pd.read_sql(sql, conn, params=params)

def nearest_workday(workdays: List[date], target: date) -> date:
    if not workdays:
        return target
    if target <= workdays[0]:
        return workdays[0]
    if target >= workdays[-1]:
        return workdays[-1]

    import bisect
    i = bisect.bisect_left(workdays, target)
    before = workdays[i - 1]
    after = workdays[i]
    if (target - before) <= (after - target):
        return before
    return after

def delivery_date_from_shortage(workdays: List[date], shortage: date, back_days: int = 5) -> date:
    # 欠品日を稼働日に丸めた上で、そこから back_days 稼働日前
    s = nearest_workday(workdays, shortage)

    wd = np.array(workdays, dtype="datetime64[D]")
    idx = int(np.searchsorted(wd, np.datetime64(s), side="left"))
    due_idx = max(0, idx - back_days)
    return workdays[due_idx]

def compute_shortage(req_row: pd.Series, inv_total: float, date_colnames: List[str], date_cols: List[date]) -> Tuple[Optional[date], float]:
    cum = 0.0
    shortage_date = None
    for d, col in zip(date_cols, date_colnames):
        cum += float(req_row.get(col, 0.0))
        if shortage_date is None and (inv_total - cum) <= 0:
            shortage_date = d
    shortage_qty = inv_total - cum
    return shortage_date, shortage_qty

# =========================
# ロード処理
# =========================

def load_machine_parts() -> pd.DataFrame:
    """
    機械課部品（MITM, 工程複数, 品名, CWAR）
    条件:
      - TTIITM001002.T$CITG = "    01"
      - TTIITM001002.T$KITM = 2
      - TTIROU001002.T$CWOC in 041..050
    """
    sql = """
    SELECT
        r102.[T$MITM] AS MITM,
        r001.[T$CWOC] AS CWOC,
        i001.[T$DSCA] AS ITEM_NAME,
        i001.[T$CWAR] AS CWAR
    FROM TTIROU001002 r001
    INNER JOIN TTIROU102002 r102
        ON r001.[T$CWOC] = r102.[T$CWOC]
    LEFT JOIN TTIITM001002 i001
        ON r102.[T$MITM] = i001.[T$ITEM]
    WHERE
        i001.[T$CITG] = '    01'
        AND i001.[T$KITM] = 2
        AND r001.[T$CWOC] IN ('041','042','043','044','045','046','047','048','049','050')
    """
    df = fetch_df(CONN_BAAN, sql)
    df["MITM_N"] = df["MITM"].map(normalize_item)

    agg = (
        df.groupby("MITM_N", as_index=False)
          .agg(
              品番=("MITM", "first"),
              品名=("ITEM_NAME", "first"),
              CWAR=("CWAR", "first"),
              工程=("CWOC", lambda s: ",".join(sorted(set(map(str, s))))),
          )
    )
    return agg

def load_stock_stoc(machine_parts: pd.DataFrame) -> pd.DataFrame:
    """
    STOC: TTDINV001002 から、機械課対象の品番だけ取得する（爆発回避）
    join条件は (ITEM, CWAR) 完全一致。
    """

    mp = machine_parts.copy()
    mp["ITEM_N"] = mp["品番"].map(normalize_item)

    # 機械課対象の (ITEM, CWAR) をユニーク化
    keys = mp[["ITEM_N", "CWAR"]].dropna().drop_duplicates()
    if keys.empty:
        return pd.DataFrame({"MITM_N": mp["MITM_N"].unique(), "STOC": 0})

    # AccessみたいにIN句で全部投げると長すぎになるので分割して取る
    # ここは「ITEM_N」ではなく、実DBのITEM文字列で絞りたいが、
    # normalizeしてるので、まずは品番（元）で絞るのが確実。
    # → machine_parts の「品番」を使う
    items = machine_parts["品番"].dropna().astype(str).unique().tolist()

    chunksize = 800  # SQL Server/ODBCの制限に引っかかりにくいサイズ
    inv_list = []

    with pyodbc.connect(CONN_BAAN) as conn:
        for i in range(0, len(items), chunksize):
            chunk = items[i:i + chunksize]
            placeholders = ",".join(["?"] * len(chunk))
            sql = f"""
            SELECT
                [T$ITEM] AS ITEM,
                [T$CWAR] AS CWAR,
                [T$STOC] AS STOC
            FROM TTDINV001002
            WHERE [T$ITEM] IN ({placeholders})
            """
            part = pd.read_sql(sql, conn, params=chunk)
            inv_list.append(part)

    if not inv_list:
        out = keys.copy()
        out["STOC"] = 0
        return out.rename(columns={"ITEM_N": "MITM_N"})[["MITM_N", "STOC"]]

    inv = pd.concat(inv_list, ignore_index=True)
    inv["ITEM_N"] = inv["ITEM"].map(normalize_item)

    # (ITEM_N, CWAR) で集約（複数行ある場合に備えて）
    inv = inv.groupby(["ITEM_N", "CWAR"], as_index=False).agg(STOC=("STOC", "sum"))

    # machine_parts（品番）に (ITEM_N, CWAR) で結合
    merged = mp.merge(inv, on=["ITEM_N", "CWAR"], how="left")
    merged["STOC"] = merged["STOC"].fillna(0)

    return merged[["MITM_N", "STOC"]]


def load_theory_and_external() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    ASS:
      - tbl_ItemTheory: Item, Theory_Cnt
      - 外部倉庫在庫: 品番, 在庫数  ※列名は環境で異なる場合があるので必要なら修正
    """
    sql_theory = """
    SELECT Item AS ITEM, Theory_Cnt AS THEORY_CNT
    FROM tbl_ItemTheory
    """
    th = fetch_df(CONN_ASS, sql_theory)
    th["ITEM_N"] = th["ITEM"].map(normalize_item)
    th_sum = th.groupby("ITEM_N", as_index=False).agg(棚理論=("THEORY_CNT", "sum"))

    sql_ext = """
    SELECT 品番 AS ITEM, 在庫数 AS QTY
    FROM 外部倉庫在庫
    """
    ext = fetch_df(CONN_ASS, sql_ext)
    ext["ITEM_N"] = ext["ITEM"].map(normalize_item)
    ext_sum = ext.groupby("ITEM_N", as_index=False).agg(外部=("QTY", "sum"))

    return th_sum, ext_sum

def load_mps_calendar() -> List[date]:
    sql = """
    SELECT [T$DATE] AS D
    FROM TTIROU400002
    WHERE [T$CTOD] = 'MPS'
    ORDER BY [T$DATE]
    """
    cal = fetch_df(CONN_BAAN, sql)
    return pd.to_datetime(cal["D"]).dt.date.tolist()

def load_mfg_open_orders(mitm_set: set[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    製造オーダー残:
      - TTISFC001002, OSTA < 7
      - 注残 = QRDR - QDLV
      - MITMで合算（注残）
    """
    sql = """
    SELECT
        [T$PDNO] AS PDNO,
        [T$OPRO] AS OPRO,
        [T$MITM] AS MITM,
        [T$OSTA] AS OSTA,
        [T$QRDR] AS QRDR,
        [T$QDLV] AS QDLV
    FROM TTISFC001002
    WHERE [T$OSTA] < 7
    """
    df = fetch_df(CONN_BAAN, sql)
    df["MITM_N"] = df["MITM"].map(normalize_item)
    df = df[df["MITM_N"].isin(mitm_set)].copy()

    df["注残"] = df["QRDR"].fillna(0) - df["QDLV"].fillna(0)
    df["OPRO"] = df["OPRO"].astype(str)  # ゼロ埋め保持
    zan_sum = df.groupby("MITM_N", as_index=False).agg(注残=("注残", "sum"))
    return df, zan_sum

def load_paintable(mfg_orders: pd.DataFrame) -> pd.DataFrame:
    """
    塗装できる数（MITMごと）:
      A = Σ注残（mfg_orders）
      B = ΣOQUA（TTDPUR041002）
      C = ΣDQUA（TTDPUR045002）※分納合算
      母材未納残 = ΣOQUA - ΣDQUA
      塗装できる数 = Σ注残 - 母材未納残 = A - (B - C) = A - B + C
      ※0未満は0に丸め（暴走防止）
    """
    sql_041 = """
    SELECT
        [T$PDNO] AS PDNO,
        [T$OPNO] AS OPNO,
        [T$PONO] AS PONO,
        [T$ORNO] AS ORNO,
        [T$OQUA] AS OQUA
    FROM TTDPUR041002 WHERE [T$PDNO] > 0
    """
    p041 = fetch_df(CONN_BAAN, sql_041)
    p041["OPNO"] = p041["OPNO"].astype(str)

    sql_045 = """
    SELECT
        [T$PDNO] AS PDNO,
        [T$PONO] AS PONO,
        [T$ORNO] AS ORNO,
        [T$DQUA] AS DQUA
    FROM TTDPUR045002 WHERE [T$PDNO] > 0
    """
    p045 = fetch_df(CONN_BAAN, sql_045)
    p045_sum = p045.groupby(["PONO", "ORNO"], as_index=False).agg(DQUA_SUM=("DQUA", "sum"))

    base = mfg_orders[["PDNO", "OPRO", "MITM_N", "注残"]].copy()
    j = base.merge(p041, left_on=["PDNO", "OPRO"], right_on=["PDNO", "OPNO"], how="left")
    j = j.merge(p045_sum, on=["PONO", "ORNO"], how="left")

    j["OQUA"] = j["OQUA"].fillna(0)
    j["DQUA_SUM"] = j["DQUA_SUM"].fillna(0)

    g = (
        j.groupby("MITM_N", as_index=False)
         .agg(
             A_注残=("注残", "sum"),
             B_OQUA=("OQUA", "sum"),
             C_DQUA=("DQUA_SUM", "sum"),
         )
    )
    g["母材未納残"] = g["B_OQUA"] - g["C_DQUA"]
    g["塗装できる数"] = (g["A_注残"] - g["母材未納残"]).clip(lower=0)

    return g[["MITM_N", "塗装できる数"]]

def load_child_requirements() -> Tuple[pd.DataFrame, List[date], List[str]]:
    """
    child_requirements.csv
      - 先頭列または sItem/品番 等が品番列
      - 残りの日付列を所要として読み込み、MITM_Nで合算
    """
    df = read_csv_robust(INPUT_CHILD_REQ_CSV)

    cand_cols = ["sItem", "MITM", "品番", "Item", "item", "child", "子品番"]
    item_col = next((c for c in cand_cols if c in df.columns), df.columns[0])

    df["MITM_N"] = df[item_col].map(normalize_item)

    date_pairs: List[Tuple[date, str]] = []
    for c in df.columns:
        if c in (item_col, "MITM_N"):
            continue
        d = parse_date_col(str(c))
        if d is not None:
            date_pairs.append((d, c))
    if not date_pairs:
        raise RuntimeError("child_requirements.csv に日付列が見つかりません（列名が日付になっている必要があります）")

    date_pairs.sort(key=lambda x: x[0])
    date_cols = [d for d, _ in date_pairs]
    date_colnames = [c for _, c in date_pairs]

    req = df[["MITM_N"] + date_colnames].copy()
    for c in date_colnames:
        req[c] = pd.to_numeric(req[c], errors="coerce").fillna(0)
    req = req.groupby("MITM_N", as_index=False).sum(numeric_only=True)
    return req, date_cols, date_colnames


# =========================
# メイン
# =========================

def main() -> None:
    # 1) 機械課部品
    mp = load_machine_parts()
    mitm_set = set(mp["MITM_N"].tolist())

    # 2) 在庫(STOC) + 棚理論 + 外部
    stoc = load_stock_stoc(mp)
    theory, external = load_theory_and_external()

    base = mp.merge(stoc, on="MITM_N", how="left")
    base["STOC"] = base["STOC"].fillna(0)

    base = base.merge(theory, left_on="MITM_N", right_on="ITEM_N", how="left").drop(columns=["ITEM_N"])
    base["棚理論"] = base["棚理論"].fillna(0)

    base = base.merge(external, left_on="MITM_N", right_on="ITEM_N", how="left").drop(columns=["ITEM_N"])
    base["外部"] = base["外部"].fillna(0)

    base["在庫合計"] = base["STOC"] + base["棚理論"] + base["外部"]

    # 3) 所要（日別）
    req, date_cols, date_colnames = load_child_requirements()
    req = req[req["MITM_N"].isin(mitm_set)].copy()

    # 4) 稼働日（MPS）
    workdays = load_mps_calendar()
    today = date.today()

    # 5) 注残（製造オーダー残）+ 塗装できる数
    mfg_orders, zan_sum = load_mfg_open_orders(mitm_set)
    paintable = load_paintable(mfg_orders)

    # 6) 結合（MITM_N 1行）
    out = base.merge(req, on="MITM_N", how="left")
    out[date_colnames] = out[date_colnames].fillna(0)

    out = out.merge(zan_sum, on="MITM_N", how="left")
    out["注残"] = out["注残"].fillna(0)

    out = out.merge(paintable, on="MITM_N", how="left")
    out["塗装できる数"] = out["塗装できる数"].fillna(0)

    # 7) 欠品日・納品期日・不足数
    shortage_dates: List[Optional[date]] = []
    delivery_dates: List[Optional[date]] = []
    shortage_qtys: List[float] = []

    for _, r in out.iterrows():
        inv_total = float(r["STOC"])
        sdate, sqty = compute_shortage(r, inv_total, date_colnames, date_cols)
        shortage_dates.append(sdate)
        shortage_qtys.append(sqty)

        if sdate is None:
            delivery_dates.append(None)
        else:
            due = delivery_date_from_shortage(workdays, sdate, back_days=5)
            if due < today:
                due = today
            delivery_dates.append(due)

    out["欠品日"] = shortage_dates
    out["納品期日"] = delivery_dates
    out["不足数"] = shortage_qtys
    
    # 工程区分：050 を含んでいたら「塗装」
    out["区分"] = np.where(
        out["工程"].astype(str).str.contains(r"\b050\b"),
        "塗装",
        ""
    )

    
    # 日別所要の合計（期間合計）
    out["所要の合計"] = out[date_colnames].sum(axis=1)
    # 不足が出ない（不足数 >= 0）のアイテムは除外
    out = out[out["不足数"] < 0].copy()


    # 8) フィールド順（要求順）
    out.rename(
        columns={
            "品番": "品番（MITM）",
            "品名": "品名（tiitm001.T$DSCA）",
            "工程": "工程（CWOC複数）",
            "STOC": "在庫（STOC）",
        },
        inplace=True,
    )

    fixed_cols = [
        "品番（MITM）",
        "品名（tiitm001.T$DSCA）",
        "工程（CWOC複数）",
        "区分",            # ← 追加
        "在庫（STOC）",
        "棚理論",
        "外部",
        "欠品日",
        "納品期日",
        "不足数",
        "注残",
        "塗装できる数",
        "所要の合計",   # ← 追加
    ]

    final_cols = fixed_cols + date_colnames
    final_cols = [c for c in final_cols if c in out.columns]
    
    out_final = out[final_cols].copy()
    # 欠品日 → 品番 の順でソート
    out_final = out_final.sort_values(
        by=["欠品日", "品番（MITM）"],
        ascending=[True, True],
        na_position="last"   # 欠品日がNoneのものは最後（※今は除外済み想定）
    )


    out_final.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"OK: {OUTPUT_CSV} を出力しました")
    print("kikaika rows:", len(out_final))
    
    
def debug_paintable_one_item(
    target_item: str = "47017250000202",
    out_csv: str = r"\\192.168.134.32\share\00_データ\01_所要計算(組立自動日程)\debug_paintable_47017250000202.csv"
) -> str:
    """
    塗装できる数の検証用：
    - target_item だけに絞る
    - PUR45をサマリせずに全件ぶら下げてCSV出力
    """
    target_n = normalize_item(target_item)

    # 1) 製造オーダー残（SFC）を取って対象品番だけ残す
    sql_sfc = """
    SELECT
        [T$PDNO] AS PDNO,
        [T$OPRO] AS OPRO,
        [T$MITM] AS MITM,
        [T$OSTA] AS OSTA,
        [T$QRDR] AS QRDR,
        [T$QDLV] AS QDLV
    FROM TTISFC001002
    WHERE [T$OSTA] < 7
    """
    sfc = fetch_df(CONN_BAAN, sql_sfc)
    sfc["MITM_N"] = sfc["MITM"].map(normalize_item)
    sfc = sfc[sfc["MITM_N"] == target_n].copy()

    if sfc.empty:
        pd.DataFrame([{
            "INFO": "対象品番の製造オーダー残が0件でした",
            "TARGET_ITEM": target_item
        }]).to_csv(out_csv, index=False, encoding="utf-8-sig")
        return out_csv

    sfc["注残"] = sfc["QRDR"].fillna(0) - sfc["QDLV"].fillna(0)

    # 2) PUR041（母材の発注/紐づけ）取得
    sql_041 = """
    SELECT
        [T$PDNO] AS PDNO,
        [T$OPNO] AS OPNO,
        [T$PONO] AS PONO,
        [T$ORNO] AS ORNO,
        [T$OQUA] AS OQUA
    FROM TTDPUR041002
    WHERE [T$PDNO] = 40084171
    """
    p041 = fetch_df(CONN_BAAN, sql_041)
    p041["OPNO"] = p041["OPNO"].astype(str)

    # 3) PUR045（入荷実績）取得 ※サマリしない（全件）
    sql_045 = """
    SELECT
        [T$PDNO] AS PDNO,
        [T$PONO] AS PONO,
        [T$ORNO] AS ORNO,
        [T$DQUA] AS DQUA,
        [T$SRNB] AS SRNB,
        [T$DATE] AS DDAT
    FROM TTDPUR045002
    WHERE [T$PDNO] = 40084171 AND [T$SRNB] = 1 
    """
    p045 = fetch_df(CONN_BAAN, sql_045)

    # 4) 結合（SFC -> PUR041 -> PUR45(全件)）
    base = sfc[["PDNO", "OPRO", "MITM", "MITM_N", "注残"]].copy()
    base["OPRO"] = base["OPRO"].astype(str)

    j = base.merge(
        p041,
        left_on=["PDNO", "OPRO"],
        right_on=["PDNO", "OPNO"],
        how="left"
    )

    # PUR45を「全件」ぶら下げ（PONO, ORNOで結合）
    j = j.merge(
        p045,
        on=["PONO", "ORNO"],
        how="left",
        suffixes=("", "_PUR45")
    )

    # null対策
    j["OQUA"] = j["OQUA"].fillna(0)
    j["DQUA"] = j["DQUA"].fillna(0)

    # 5) “行レベル”で見たい計算材料を作る
    #   - ここは検証用なので、行ごとの見やすさ優先
    j["PUR45有無"] = np.where(j["DQUA"].notna(), 1, 0)

    # 6) 集約（現行ロジック相当：A=注残合計, B=OQUA合計, C=DQUA合計）
    A = float(base["注残"].sum())
    B = float(j.drop_duplicates(subset=["PDNO", "PONO"])["OQUA"].sum())
    # ↑ PUR45を全件ぶら下げるとOQUAが重複するので、ここは重複排除して集約（重要）
    C = float(j["DQUA"].fillna(0).sum())

    mother_undelivered = B - C
    paintable = max(0.0, A - mother_undelivered)  # = max(0, A - (B - C))

    # 7) 先頭にサマリ行を付けてCSV出力
    summary = pd.DataFrame([{
        "SUMMARY_TARGET_ITEM": target_item,
        "A_注残_SUM": A,
        "B_OQUA_SUM(重複排除後)": B,
        "C_DQUA_SUM(PUR45全件合算)": C,
        "母材未納残(B-C)": mother_undelivered,
        "塗装できる数(A-(B-C))_clip0": paintable,
        "SFC行数": len(base),
        "JOIN後行数(PUR45全件ぶら下げ)": len(j),
    }])

    # 出力列（見たいものだけ）
    cols = [
        "MITM", "PDNO", "OPRO", "注残",
        "PONO", "ORNO", "OQUA",
        "DQUA", "DDAT",
    ]
    for c in cols:
        if c not in j.columns:
            j[c] = None

    detail = j[cols].copy()
    # 見やすく
    detail.sort_values(by=["PDNO", "OPRO", "PONO", "ORNO", "DDAT"], inplace=True, na_position="last")

    # 1ファイルにまとめて出したいので、summaryを先頭に付ける（列合わせ）
    # summaryにない列を追加
    for c in detail.columns:
        if c not in summary.columns:
            summary[c] = None
    summary = summary[detail.columns.tolist()]

    out = pd.concat([summary, detail], ignore_index=True)
    out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"OK: debug csv -> {out_csv}")
    return out_csv


# （ファイル上部〜中略：元のまま）

# def run() -> str:
#     main()
#     return OUTPUT_CSV

# if __name__ == "__main__":
#     main()
    
if __name__ == "__main__":
    debug_paintable_one_item()

