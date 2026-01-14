import pandas as pd
import datetime as dt
import os
import re
import pyodbc
import shutil

# =========================
# 設定
# =========================
CONN_SCHED = r"DSN=AssemblySched;UID=digiassy;PWD=digiassy;"
CONN_BOM   = r"DRIVER={SQL Server};SERVER=192.168.134.11;UID=BomMstUser;PWD=BomMstUser;DATABASE=002Stock;"

TODAY = dt.date.today()
HORIZON_DAYS = 60
END_DATE = TODAY + dt.timedelta(days=HORIZON_DAYS)

OUTPUT_DIR = r"\\192.168.134.32\share\00_データ\01_所要計算(組立自動日程)"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "child_requirements.csv")

BACKUP_DIR = r"\\192.168.134.32\share\00_データ\01_所要計算(組立自動日程)\アーカイブ"


# =========================
# ユーティリティ
# =========================
def get_month_last_day(year: int, month: int) -> int:
    """その月の末日(28/29/30/31)"""
    if month == 12:
        next_month_first = dt.date(year + 1, 1, 1)
    else:
        next_month_first = dt.date(year, month + 1, 1)
    last_day = next_month_first - dt.timedelta(days=1)
    return last_day.day


def ym_day_to_date_safe(ym: str, daycol: str):
    """
    "2025/10" + "計01" -> date(2025,10,1)
    存在しない日付(2月31日など)のときは None を返す
    """
    try:
        year_str, month_str = ym.split("/")
        year = int(year_str)
        month = int(month_str)
    except Exception:
        return None

    # "計01" → 1
    try:
        day = int(daycol.replace("計", ""))
    except Exception:
        return None

    last_day_num = get_month_last_day(year, month)
    if day > last_day_num:
        return None

    try:
        return dt.date(year, month, day)
    except ValueError:
        return None


def month_range(ym_str: str):
    """'2025/10' -> (2025-10-01, 2025-10-31)"""
    y, m = ym_str.split("/")
    y = int(y)
    m = int(m)
    first_day = dt.date(y, m, 1)
    if m == 12:
        next_month_first = dt.date(y + 1, 1, 1)
    else:
        next_month_first = dt.date(y, m + 1, 1)
    last_day = next_month_first - dt.timedelta(days=1)
    return first_day, last_day


def month_overlaps_target(ym_str: str, target_start: dt.date, target_end: dt.date):
    """
    年月(1ヶ月分)が、[target_start, target_end] のどこか1日でも重なれば True
    """
    try:
        m_start, m_end = month_range(ym_str)
    except Exception:
        return False
    return not (m_end < target_start or m_start > target_end)


# =========================
# run()
# =========================
def run() -> str:
    # =========================
    # 1) 組立スケジュール読込 → 日別に展開
    # =========================

    # スケジュール全件取得
    with pyodbc.connect(CONN_SCHED) as conn_sched:
        df_sched_raw = pd.read_sql_query("SELECT * FROM trn_AssySchedule", conn_sched)

    # 列名そろえる
    df_sched_raw = df_sched_raw.rename(columns={
        "年月度": "YearMonth",  # '2025/12' みたいな文字列
        "PLU": "Item"          # 親の品番
    })

    # YearMonthが無い行は落とす
    df_sched_raw = df_sched_raw.dropna(subset=["YearMonth"]).copy()

    # "計01"～"計31" の列を拾う
    day_cols = [c for c in df_sched_raw.columns if re.match(r"^計\d{2}$", str(c))]

    # 60日先の範囲と重なってるYearMonthだけに絞る
    df_sched_raw = df_sched_raw[
        df_sched_raw["YearMonth"].apply(lambda ym: month_overlaps_target(str(ym), TODAY, END_DATE))
    ].copy()

    # meltで「親品番×日付列」→「親品番×日付(1日単位)」にほどく
    df_sched_long = df_sched_raw.melt(
        id_vars=["YearMonth", "Item"],
        value_vars=day_cols,
        var_name="DayCol",
        value_name="Qty"
    )

    # Qtyを数値化、0以下/NaNは捨てる
    df_sched_long["Qty"] = pd.to_numeric(df_sched_long["Qty"], errors="coerce").fillna(0)
    df_sched_long = df_sched_long[df_sched_long["Qty"] > 0].copy()

    # "2025/12" + "計03" → 2025-12-03
    df_sched_long["ProdDate"] = [
        ym_day_to_date_safe(str(ym), str(dc))
        for ym, dc in zip(df_sched_long["YearMonth"], df_sched_long["DayCol"])
    ]

    # 実在しない日付(None)は除外
    df_sched_long = df_sched_long.dropna(subset=["ProdDate"]).copy()

    # 期間(TODAY～END_DATE)のみに限定
    mask_period = (
        (df_sched_long["ProdDate"] >= TODAY) &
        (df_sched_long["ProdDate"] <= END_DATE)
    )
    df_sched_long = df_sched_long.loc[mask_period].copy()

    # ここから使うカラムだけに整理
    df_sched = df_sched_long[["ProdDate", "Item", "Qty"]].reset_index(drop=True)

    # Itemに入ってる親品番をstripしておく（BOM側は末尾スペースあるので）
    df_sched["Item"] = df_sched["Item"].astype(str).str.strip()

    # =========================
    # 2) BOM読込 → 親→子展開の準備
    # =========================

    # BOMマスタ読込
    # 重要：Item(親), sItem(子), bomValue(構成数量) を取る
    with pyodbc.connect(CONN_BOM) as conn_bom:
        df_bom = pd.read_sql_query(
            "SELECT Item, sItem, bomValue FROM bomMaster",
            conn_bom
        )

    # 末尾スペース等クリーニング
    df_bom["Item"] = df_bom["Item"].astype(str).str.strip()
    df_bom["sItem"] = df_bom["sItem"].astype(str).str.strip()
    df_bom["bomValue"] = pd.to_numeric(df_bom["bomValue"], errors="coerce").fillna(0)

    # 親ごとに子部品リストをキャッシュ
    bom_cache = {}
    for parent_item, subdf in df_bom.groupby("Item"):
        bom_cache[parent_item] = subdf[["sItem", "bomValue"]].reset_index(drop=True)

    # =========================
    # 3) スケジュール×BOM を子部品ごとに展開
    # =========================

    records = []

    for row in df_sched.itertuples():
        prod_date = row.ProdDate      # この日に
        parent_plu = row.Item         # この親を
        prod_qty = row.Qty            # これだけ作る

        # 親品番がBOMにいなければスキップ
        if parent_plu not in bom_cache:
            continue

        bom_rows = bom_cache[parent_plu].copy()
        # 子必要数 = 親生産数 × 子の使用数
        bom_rows["needQty"] = bom_rows["bomValue"] * prod_qty
        bom_rows["ProdDate"] = prod_date
        bom_rows["ParentPLU"] = parent_plu

        records.append(bom_rows[["ProdDate", "sItem", "needQty"]])

    if len(records) == 0:
        # マッチする親がなかった場合
        result_df = pd.DataFrame(columns=["sItem"])
    else:
        exploded = pd.concat(records, ignore_index=True)

        # 子部品×日付で合計
        grouped = (
            exploded.groupby(["sItem", "ProdDate"], as_index=False)["needQty"]
            .sum()
        )

        # 横持ち（日付ごとに列にする）
        pivot_df = grouped.pivot_table(
            index="sItem",
            columns="ProdDate",
            values="needQty",
            aggfunc="sum",
            fill_value=0
        )

        # 列（日付）を "YYYY-MM-DD" 文字列にする
        pivot_df.columns = [d.strftime("%Y-%m-%d") for d in pivot_df.columns]

        # 欲しい全日（今日～60日後）を並べる
        all_days = [TODAY + dt.timedelta(days=i) for i in range(HORIZON_DAYS + 1)]
        all_days_str = [d.strftime("%Y-%m-%d") for d in all_days]

        pivot_df = pivot_df.reindex(columns=all_days_str, fill_value=0)

        result_df = pivot_df.reset_index()

    # =========================
    # 4) CSV出力
    # =========================
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    # =========================
    # 5) バックアップ保存
    # =========================
    os.makedirs(BACKUP_DIR, exist_ok=True)

    # バックアップファイル名：child_requirements_YYYYMMDD_HHMMSS.csv
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(BACKUP_DIR, f"child_requirements_{timestamp}.csv")

    shutil.copy2(OUTPUT_FILE, backup_file)
    print("Backup saved:", backup_file)

    print("Done:", OUTPUT_FILE)
    print("result rows:", len(result_df))
    print("result cols:", len(result_df.columns))

    return OUTPUT_FILE


if __name__ == "__main__":
    run()
