import pyodbc
import pandas as pd

# データベースへの接続情報
odbc_conn_AssemblySched = (
    r'DSN=AssemblySched;'  # DSN設定はシステム依存です
    r'UID=digiassy;'
    r'PWD=digiassy;'
)

odbc_conn_baan = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ=\\bardbsrv\baantable\baandb.mdb;'  # パスを適切に置き換える
)

# 一括更新用にSQLバッファを用意
update_queries = []

# データベース接続の名前を変更し、異なる名前を使用
with pyodbc.connect(odbc_conn_AssemblySched) as conn_sched, pyodbc.connect(odbc_conn_baan) as conn_baan:
    cursor_sched = conn_sched.cursor()
    cursor_baan = conn_baan.cursor()

    # パフォーマンス向上のためにfast_executemanyを有効化
    cursor_sched.fast_executemany = True

    # Combined_Assembly_Schedule.xlsxファイルを読み込む
    combined_schedule_path = r'\\tiss-ntsrv\TISS-PCS\第三組立課日程（包装機)\Combined_Assembly_Schedule.xlsx'
    combined_schedule = pd.read_excel(combined_schedule_path)

    # 'Assembly Start Date'を日付形式に変換し、'年月度'と'日付'を抽出
    combined_schedule['Assembly Start Date'] = pd.to_datetime(combined_schedule['Assembly Start Date'], errors='coerce')
    combined_schedule.dropna(subset=['Assembly Start Date'], inplace=True)  # 無効な日付の行を除外

    combined_schedule['年月度'] = combined_schedule['Assembly Start Date'].dt.strftime('%Y/%m')
    combined_schedule['日付'] = combined_schedule['Assembly Start Date'].dt.day
    # PLUを文字列型に変換
    combined_schedule['PLU'] = combined_schedule['PLU'].astype(str)

    # 日付ごとにグループ化してカウントを集計
    assembly_schedule_grouped = combined_schedule.groupby(['年月度', 'PLU', '日付']).size().reset_index(name='count')

    # データベースのテーブルを更新または挿入
    for _, row in assembly_schedule_grouped.iterrows():
        year_month = row['年月度']
        plu = row['PLU']
        day = row['日付']
        count = row['count']

        # 対応するテーブルの行を検索
        query = "SELECT * FROM trn_AssySchedule WHERE 年月度 = ? AND PLU = ?"
        cursor_sched.execute(query, (year_month, plu))
        matching_row = cursor_sched.fetchone()

        if not matching_row:
            # マスタデータを抽出
            Mst_SQL = "SELECT * FROM mst_PLU WHERE PLU = ?"
            cursor_sched.execute(Mst_SQL, (plu,))
            PLU_Mst_row = cursor_sched.fetchone()

            if PLU_Mst_row:
                NAME, MODEL, LV2, WC, TEAM = PLU_Mst_row[1:6]  # インデックス1, 2, 3, 4, 5を取得

                insert_query = """
                INSERT INTO trn_AssySchedule (
                    年月度, PLU, ﾜｰｸｾﾝﾀｰ, ﾁｰﾑ, Family名, LV2品目, 品名, 
                    計01, 計02, 計03, 計04, 計05, 計06, 計07, 計08, 計09, 
                    計10, 計11, 計12, 計13, 計14, 計15, 計16, 計17, 計18, 計19, 
                    計20, 計21, 計22, 計23, 計24, 計25, 計26, 計27, 計28, 計29, 
                    計30, 計31
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor_sched.execute(insert_query, (year_month, plu, WC, TEAM, MODEL, LV2, NAME) + (0,) * 31)

        day_column = f'計{str(day).zfill(2)}'
        update_sql = f"UPDATE trn_AssySchedule SET {day_column} = ? WHERE 年月度 = ? AND PLU = ?"
        update_queries.append((update_sql, (count, year_month, plu)))

        # カレンダーテーブルを取得し、SQLインジェクションを避けるためパラメータ化
        year, month = year_month.split('/')
        calendar_sql = """
        SELECT [T$DATE], [T$CTOD]
        FROM tirou400
        WHERE YEAR([T$DATE]) = ? AND MONTH([T$DATE]) = ?
        ORDER BY [T$DATE]
        """
        calendar_df = pd.read_sql(calendar_sql, conn_baan, params=(year, month))

        # 挿入したデータの更新処理
        for index, row in calendar_df.iterrows():
            date = row['T$DATE']
            ctot = row['T$CTOD']

            # {date}をDD形式に変換
            date = date.strftime('%d')  # または日付型であれば date.strftime('%d')

            # 稼と区のカラム名を設定
            k_column = f"稼{date}"
            ku_column = f"区{date}"


            if ctot == "MPS":
                k_value = 0
                ku_value = "W"
            else:
                k_value = 1
                ku_value = "H"
            
            # UPDATE文を使ってtrn_AssyScheduleを更新
            update_sql = f"""
            UPDATE trn_AssySchedule
            SET [{k_column}] = ?, [{ku_column}] = ?
            WHERE 年月度 = '{year_month}' AND PLU = '{plu}'
            """
            cursor_sched.execute(update_sql, (k_value, ku_value))

    # 一括更新の実行
    try:
        for query, params in update_queries:
            cursor_sched.execute(query, params)

        # すべての変更をコミット
        conn_sched.commit()
        conn_baan.commit()

    except Exception as e:
        print(f"Error occurred: {e}")
        conn_sched.rollback()
        conn_baan.rollback()

print("完了")