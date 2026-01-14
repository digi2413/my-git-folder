# -*- coding: utf-8 -*-
"""
塗装できる数（母材 未納残）検証スクリプト（ターミナル出力用）

狙い：
- 品番(MITM)を指定して、対象のSFC(PDNO/OPRO)を全件抽出
- 対象PDNOのPUR041（母材発注）とPUR045（入荷実績）を取得
- SFC→PUR041→PUR045_SUM のJOIN結果をターミナルに出す
- OPRO/OPNO の型ズレ（10 vs 10.0 vs '10 ' 等）でJOINが死ぬのを防ぐ
"""

from __future__ import annotations

import argparse
import pandas as pd
import pyodbc

CONN_BAAN = "DSN=TRKBAAN;UID=odbc;PWD=odbc;Trusted_Connection=No;APP=Microsoft Office;"


def fetch_df(sql: str, params=()) -> pd.DataFrame:
    with pyodbc.connect(CONN_BAAN) as conn:
        return pd.read_sql(sql, conn, params=params)


def show(title: str, df: pd.DataFrame, n: int = 50) -> None:
    print("\n" + "=" * 100)
    print(title)
    print(f"rows={len(df)}")
    if len(df) == 0:
        return
    with pd.option_context(
        "display.max_rows", n,
        "display.max_columns", 200,
        "display.width", 240
    ):
        print(df.head(n))


def normalize_item(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).replace(" ", "").replace("\u3000", "")


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


def debug_item(item: str) -> None:
    item_n = normalize_item(item)
    print(f"DEBUG ITEM: {item} (norm={item_n})")

    # -------------------------
    # 1) SFC（対象品番）
    # -------------------------
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
      AND REPLACE(REPLACE([T$MITM],' ',''),N'　','') = ?
    """
    sfc = fetch_df(sql_sfc, (item_n,))
    if len(sfc) == 0:
        print("SFC 0件。終了。")
        return

    sfc["注残"] = sfc["QRDR"].fillna(0) - sfc["QDLV"].fillna(0)

    # 正規化キー（重要）
    sfc["PDNO"] = pd.to_numeric(sfc["PDNO"], errors="coerce").astype("Int64")
    sfc["OPRO_K"] = to_int_str(sfc["OPRO"])

    show("SFC（対象品番の全行）", sfc.sort_values(["PDNO", "OPRO_K"]), n=200)

    # OPROのズレ確認（ターミナル用）
    print("\n" + "-" * 100)
    print("OPRO / OPRO_K unique")
    print("OPRO raw :", sorted(set(map(repr, sfc["OPRO"].dropna().unique().tolist()))))
    print("OPRO_K   :", sorted(set(sfc["OPRO_K"].dropna().unique().tolist())))

    # 対象PDNO
    pdnos = sfc["PDNO"].dropna().astype(int).unique().tolist()
    placeholders = ",".join(["?"] * len(pdnos))

    # -------------------------
    # 2) PUR041（母材発注紐づけ）
    # -------------------------
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
    p041 = fetch_df(sql_041, tuple(pdnos))

    if len(p041):
        p041["PDNO"] = pd.to_numeric(p041["PDNO"], errors="coerce").astype("Int64")
        p041["OPNO_K"] = to_int_str(p041["OPNO"])  # ←結合用
        p041["PONO"] = p041["PONO"].astype(str)
        p041["ORNO"] = p041["ORNO"].astype(str)
        p041["OQUA"] = pd.to_numeric(p041["OQUA"], errors="coerce").fillna(0)

    show("PUR041（対象PDNO分）", p041.sort_values(["PDNO", "OPNO_K", "PONO", "ORNO"]), n=300)

    print("\n" + "-" * 100)
    print("OPNO / OPNO_K unique")
    if len(p041):
        print("OPNO raw :", sorted(set(map(repr, p041["OPNO"].dropna().unique().tolist()))))
        print("OPNO_K   :", sorted(set(p041["OPNO_K"].dropna().unique().tolist())))
    else:
        print("PUR041 0件")

    # -------------------------
    # 3) PUR045（入荷実績）※全件
    # -------------------------
    sql_045 = f"""
    SELECT
        [T$PDNO] AS PDNO,
        [T$PONO] AS PONO,
        [T$ORNO] AS ORNO,
        [T$DQUA] AS DQUA,
        [T$DATE] AS DDAT
    FROM TTDPUR045002
    WHERE [T$PDNO] IN ({placeholders})
    """
    p045 = fetch_df(sql_045, tuple(pdnos))

    if len(p045):
        p045["PDNO"] = pd.to_numeric(p045["PDNO"], errors="coerce").astype("Int64")
        p045["PONO"] = p045["PONO"].astype(str)
        p045["ORNO"] = p045["ORNO"].astype(str)
        p045["DQUA"] = pd.to_numeric(p045["DQUA"], errors="coerce").fillna(0)
        p045["DDAT"] = pd.to_datetime(p045["DDAT"], errors="coerce")

    show("PUR045（対象PDNO分：全件）", p045.sort_values(["PDNO", "PONO", "ORNO", "DDAT"]), n=400)

    # PUR045 サマリ（PDNO, PONO, ORNO単位）
    p045_sum = (
        p045.groupby(["PDNO", "PONO", "ORNO"], as_index=False)
            .agg(DQUA_SUM=("DQUA", "sum"))
    )

    # -------------------------
    # 4) JOIN（SFC → PUR041 → PUR045_SUM）
    # -------------------------
    base = sfc[["PDNO", "OPRO_K", "注残"]].copy()

    if len(p041) == 0:
        print("\nPUR041が0件のため、結合できません。")
        return

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
    j["DQUA_SUM"] = j["DQUA_SUM"].fillna(0)

    show("JOIN後（SFC×PUR041×PUR45_SUM）", j.sort_values(["PDNO", "OPRO_K", "PONO", "ORNO"]), n=400)

    # -------------------------
    # 5) キー一致率 / 欠落チェック
    # -------------------------
    missing_p041 = j["OQUA"].isna().sum()
    print("\n" + "-" * 100)
    print("JOIN健全性チェック")
    print(f"JOIN後 行数: {len(j)}")
    print(f"PUR041が取れてない行数(OQUAがNaN): {missing_p041}")
    if missing_p041:
        show("PUR041欠落行（OQUA NaN）", j[j["OQUA"].isna()].sort_values(["PDNO","OPRO_K"]).head(200), n=200)

    # -------------------------
    # 6) 集約比較（重複加算の検知）
    # -------------------------
    A = float(sfc["注残"].sum())

    # B: OQUA合計（JOIN後そのままだと重複し得る）
    B_bad = float(j["OQUA"].fillna(0).sum())

    # B_good: キーで重複排除してからOQUA合計
    # ここは「PDNO,OPRO,PONO,ORNO」が母材の1単位とみなせる前提
    B_good = float(
        j.drop_duplicates(subset=["PDNO", "OPRO_K", "PONO", "ORNO"])["OQUA"].fillna(0).sum()
    )

    # C: DQUA合計（PUR45サマリ合計）
    C = float(p045_sum["DQUA_SUM"].sum()) if len(p045_sum) else 0.0

    # 参考：1753-01-01系のダミーを除外したC_valid
    C_valid = 0.0
    if len(p045):
        p045_valid = p045.copy()
        p045_valid = p045_valid[p045_valid["ORNO"] != "0"]
        p045_valid = p045_valid[p045_valid["DDAT"].notna() & (p045_valid["DDAT"] > pd.Timestamp("1900-01-01"))]
        C_valid = float(p045_valid["DQUA"].sum()) if len(p045_valid) else 0.0

    print("\n" + "-" * 100)
    print("集約比較（塗装できる数の材料）")
    print(f"A(注残SUM) = {A}")
    print(f"B_bad(OQUA SUM / JOIN後そのまま) = {B_bad}")
    print(f"B_good(OQUA SUM / キー重複排除後) = {B_good}")
    print(f"C(DQUA_SUM SUM / PUR45サマリ) = {C}")
    print(f"C_valid(ORNO!=0 & DDAT>1900 のDQUA合計) = {C_valid}")

    paintable_bad = max(0.0, A - (B_bad - C))
    paintable_good = max(0.0, A - (B_good - C))
    print(f"paintable_bad  = max(0, A - (B_bad - C))  = {paintable_bad}")
    print(f"paintable_good = max(0, A - (B_good - C)) = {paintable_good}")

    print("\nDEBUG END")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--item", default="47017250000202", help="検証したい品番(MITM)")
    args = ap.parse_args()
    debug_item(args.item)


if __name__ == "__main__":
    main()
