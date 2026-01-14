# -*- coding: utf-8 -*-
"""
機械課 部品所要日程（Pythonのみ）
- child_requirements.csv（日別所要）を使い、欠品日・納品期日・不足数・注残・着手可を算出

DB接続:
- BAAN系: DSN=TRKBAAN （テーブル名は TTIROU001002 等）
- ASS系 : DSN=ASS     （tbl_ItemTheory, 外部倉庫在庫）

出力:
- kikaikaSchedule.csv (UTF-8-sig)
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

# CSVエンコーディング優先順
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

def to_int_str(s: pd.Series) -> pd.Series:
    """
    10 / 10.0 / '10 ' / Decimal('10') を全部 '10' に寄せる
    変換できないものは '-1'
    """
    return (
        pd.to_numeric(s, errors="coerce")
          .fillna(-1)
          .astype(int)
          .astype(str)
    )

def chunked_list(lst: List, size: int) -> List[List]:
    return [lst[i:i+size] for i in range(0, len(lst), size)]


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

    keys = mp[["ITEM_N", "CWAR"]].dropna().drop_duplicates()
    if keys.empty:
        return pd.DataFrame({"MITM_N": mp["MITM_N"].unique(), "STOC": 0})

    items = machine_parts["品番"].dropna().astype(str).unique().tolist()

    chunksize = 800
    inv_list = []

    with pyodbc.connect(CONN_BAAN) as conn:
        for chunk in chunked_list(items, chunksize):
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
    inv = inv.groupby(["ITEM_N", "CWAR"], as_index=False).agg(STOC=("STOC", "sum"))

    merged = mp.merge(inv, on=["ITEM_N", "CWAR"], how="left")
    merged["STOC"] = merged["STOC"].fillna(0)

    return merged[["MITM_N", "STOC"]]

def load_theory_and_external() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    ASS:
      - tbl_ItemTheory: Item, Theory_Cnt
      - 外部倉庫在庫: 品番, 在庫数
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
    WHERE [T$OSTA] < 6
    """
    df = fetch_df(CONN_BAAN, sql)
    df["MITM_N"] = df["MITM"].map(normalize_item)
    df = df[df["MITM_N"].isin(mitm_set)].copy()

    df["注残"] = df["QRDR"].fillna(0) - df["QDLV"].fillna(0)

    # ★重要：JOIN用キーを作る（10 / 10.0 / '10 ' を統一）
    df["PDNO"] = pd.to_numeric(df["PDNO"], errors="coerce").astype("Int64")
    df["OPRO_K"] = to_int_str(df["OPRO"])

    zan_sum = df.groupby("MITM_N", as_index=False).agg(注残=("注残", "sum"))
    return df, zan_sum

def load_paintable(mfg_orders: pd.DataFrame) -> pd.DataFrame:
    """
    着手可（MITMごと）:
      A = Σ注残（mfg_orders）
      B = ΣOQUA（TTDPUR041002）
      C = ΣDQUA（TTDPUR045002）※分納合算
      母材未納残 = ΣOQUA - ΣDQUA
      着手可 = Σ注残 - 母材未納残 = A - (B - C) = A - B + C
      ※0未満は0に丸め
    """
    if mfg_orders.empty:
        return pd.DataFrame(columns=["MITM_N", "着手可"])

    # 対象PDNOだけに絞って取得（軽量化）
    pdnos = mfg_orders["PDNO"].dropna().astype(int).unique().tolist()
    if not pdnos:
        return pd.DataFrame(columns=["MITM_N", "着手可"])

    chunksize = 800

    p041_list = []
    p045_list = []

    with pyodbc.connect(CONN_BAAN) as conn:
        for chunk in chunked_list(pdnos, chunksize):
            placeholders = ",".join(["?"] * len(chunk))

            sql_041 = f"""
            SELECT
                [T$PDNO] AS PDNO,
                [T$OPNO] AS OPNO,
                [T$PONO] AS PONO,
                [T$ORNO] AS ORNO,
                [T$OQUA] AS OQUA
            FROM TTDPUR041002
            WHERE [T$PDNO] IN ({placeholders})
            """
            p041_list.append(pd.read_sql(sql_041, conn, params=chunk))

            sql_045 = f"""
            SELECT
                [T$PDNO] AS PDNO,
                [T$PONO] AS PONO,
                [T$ORNO] AS ORNO,
                [T$DQUA] AS DQUA
            FROM TTDPUR045002
            WHERE [T$PDNO] IN ({placeholders})
            """
            p045_list.append(pd.read_sql(sql_045, conn, params=chunk))

    p041 = pd.concat(p041_list, ignore_index=True) if p041_list else pd.DataFrame()
    p045 = pd.concat(p045_list, ignore_index=True) if p045_list else pd.DataFrame()

    if p041.empty:
        # 母材情報が取れない場合は、着手可=注残（母材未納残=0扱い）にしたいならここを調整
        g = mfg_orders.groupby("MITM_N", as_index=False).agg(A_注残=("注残", "sum"))
        g["着手可"] = g["A_注残"].clip(lower=0)
        return g[["MITM_N", "着手可"]]

    # 型/キー統一
    p041["PDNO"] = pd.to_numeric(p041["PDNO"], errors="coerce").astype("Int64")
    p041["OPNO_K"] = to_int_str(p041["OPNO"])
    p041["PONO"] = p041["PONO"].astype(str)
    p041["ORNO"] = p041["ORNO"].astype(str)
    p041["OQUA"] = pd.to_numeric(p041["OQUA"], errors="coerce").fillna(0)

    if not p045.empty:
        p045["PDNO"] = pd.to_numeric(p045["PDNO"], errors="coerce").astype("Int64")
        p045["PONO"] = p045["PONO"].astype(str)
        p045["ORNO"] = p045["ORNO"].astype(str)
        p045["DQUA"] = pd.to_numeric(p045["DQUA"], errors="coerce").fillna(0)

        # ★混線防止：PDNOも含めてサマリ
        p045_sum = (
            p045.groupby(["PDNO", "PONO", "ORNO"], as_index=False)
                .agg(DQUA_SUM=("DQUA", "sum"))
        )
    else:
        p045_sum = pd.DataFrame(columns=["PDNO", "PONO", "ORNO", "DQUA_SUM"])

    base = mfg_orders[["PDNO", "OPRO_K", "MITM_N", "注残"]].copy()

    # ★ここが今回の主修正：OPRO_K ↔ OPNO_K でJOIN
    j = base.merge(
        p041,
        left_on=["PDNO", "OPRO_K"],
        right_on=["PDNO", "OPNO_K"],
        how="left"
    )

    j = j.merge(
        p045_sum,
        on=["PDNO", "PONO", "ORNO"],
        how="left"
    )

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
    g["着手可"] = (g["A_注残"] - g["母材未納残"]).clip(lower=0)

    return g[["MITM_N", "着手可"]]

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

    base["在庫合計"] = base["棚理論"] + base["外部"]

    # 3) 所要（日別）
    req, date_cols, date_colnames = load_child_requirements()
    req = req[req["MITM_N"].isin(mitm_set)].copy()

    # 4) 稼働日（MPS）
    workdays = load_mps_calendar()
    today = date.today()

    # 5) 注残（製造オーダー残）+ 着手可
    mfg_orders, zan_sum = load_mfg_open_orders(mitm_set)
    paintable = load_paintable(mfg_orders)

    # 6) 結合（MITM_N 1行）
    out = base.merge(req, on="MITM_N", how="left")
    out[date_colnames] = out[date_colnames].fillna(0)

    out = out.merge(zan_sum, on="MITM_N", how="left")
    out["注残"] = out["注残"].fillna(0)

    out = out.merge(paintable, on="MITM_N", how="left")
    out["着手可"] = out["着手可"].fillna(0)

    # 7) 欠品日・納品期日・不足数
    shortage_dates: List[Optional[date]] = []
    delivery_dates: List[Optional[date]] = []
    shortage_qtys: List[float] = []

    for _, r in out.iterrows():
        inv_total = float(r["STOC"])  # ※ここは元のまま（必要なら「在庫合計」に変更してOK）
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

    # 8) フィールド順
    out.rename(
        columns={
            "品番": "品番（MITM）",
            "品名": "品名（tiitm001.T$DSCA）",
            "工程": "工程（CWOC複数）",
            "STOC": "在庫（baan）",
        },
        inplace=True,
    )

    fixed_cols = [
        "品番（MITM）",
        "品名（tiitm001.T$DSCA）",
        "工程（CWOC複数）",
        "区分",
        "在庫（baan）",
        "棚理論",
        "外部",
        "欠品日",
        "納品期日",
        "不足数",
        "注残",
        "着手可",
        "所要の合計",
    ]

    final_cols = fixed_cols + date_colnames
    final_cols = [c for c in final_cols if c in out.columns]

    out_final = out[final_cols].copy()

    # 欠品日 → 品番 の順でソート
    out_final = out_final.sort_values(
        by=["欠品日", "品番（MITM）"],
        ascending=[True, True],
        na_position="last"
    )

    out_final.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"OK: {OUTPUT_CSV} を出力しました")
    print("kikaika rows:", len(out_final))


def run() -> str:
    main()
    return OUTPUT_CSV


if __name__ == "__main__":
    main()
