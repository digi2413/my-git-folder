import pyodbc
import pandas as pd

# データベースへの接続情報
odbc_conn_AssemblySched = (
    r'DSN=AssemblySched;'
    r'UID=digiassy;'
    r'PWD=digiassy;'
)

odbc_conn_baan = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ=\\bardbsrv\baantable\baandb.mdb;'
)

combined_schedule_path = r'\\tiss-ntsrv\TISS-PCS\第三組立課日程（包装機)\Combined_Assembly_Schedule.xlsx'


def run() -> None:
    update_queries = []

    with pyodbc.connect(odbc_conn_AssemblySched) as conn_sched, pyodbc.connect(odbc_conn_baan) as conn_baan:
        cursor_sched = conn_sched.cursor()
        cursor_baan = conn_baan.cursor()

        cursor_sched.fast_executemany = True

        combined_schedule = pd.read_excel(combined_schedule_path)

        combined_schedule['Assembly Start Date'] = pd.to_datetime(
            combined_schedule['Assembly Start Date'],
            errors='coerce'
        )
        combined_schedule.dropna(subset=['Assembly Start Date'], inplace=True)

        combined_schedule['年月度'] = combined_schedule['Assembly Start Date'].dt.strftime('%Y/%m')
        combined_schedule['日付'] = combined_schedule['Assembly Start Date'].dt.day
        combined_schedule['PLU'] = combined_schedule['PLU'].astype(str)

        assembly_schedule_grouped = (
            combined_schedule
            .groupby(['年月度', 'PLU', '日付'])
            .size()
            .reset_index(name='count')
        )

        for _, row in assembly_schedule_grouped.iterrows():
            year_month = row['年月度']
            plu = row['PLU']
            day = row['日付']
            count = row['count']

            query = "SELECT * FROM trn_AssySchedule WHERE 年月度 = ? AND PLU = ?"
            cursor_sched.execute(query, (year_month, plu))
            matching_row = cursor_sched.fetchone()

            if not matching_row:
                Mst_SQL = "SELECT * FROM mst_PLU WHERE PLU = ?"
                cursor_sched.execute(Mst_SQL, (plu,))
                PLU_Mst_row = cursor_sched.fetchone()

                if PLU_Mst_row:
                    NAME, MODEL, LV2, WC, TEAM = PLU_Mst_row[1:6]

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
                    cursor_sched.execute(
                        insert_query,
                        (year_month, plu, WC, TEAM, MODEL, LV2, NAME) + (0,) * 31
                    )

            day_column = f'計{str(day).zfill(2)}'
            update_sql = f"UPDATE trn_AssySchedule SET {day_column} = ? WHERE 年月度 = ? AND PLU = ?"
            update_queries.append((update_sql, (count, year_month, plu)))

            year, month = year_month.split('/')
            calendar_sql = """
            SELECT [T$DATE], [T$CTOD]
            FROM tirou400
            WHERE YEAR([T$DATE]) = ? AND MONTH([T$DATE]) = ?
            ORDER BY [T$DATE]
            """
            calendar_df = pd.read_sql(calendar_sql, conn_baan, params=(year, month))

            for _, r in calendar_df.iterrows():
                d = r['T$DATE'].strftime('%d')
                ctot = r['T$CTOD']

                k_column = f"稼{d}"
                ku_column = f"区{d}"

                if ctot == "MPS":
                    k_value = 0
                    ku_value = "W"
                else:
                    k_value = 1
                    ku_value = "H"

                update_sql = f"""
                UPDATE trn_AssySchedule
                SET [{k_column}] = ?, [{ku_column}] = ?
                WHERE 年月度 = '{year_month}' AND PLU = '{plu}'
                """
                cursor_sched.execute(update_sql, (k_value, ku_value))

        try:
            for q, params in update_queries:
                cursor_sched.execute(q, params)

            conn_sched.commit()
            conn_baan.commit()

        except Exception as e:
            print(f"Error occurred: {e}")
            conn_sched.rollback()
            conn_baan.rollback()
            raise

    print("完了")


if __name__ == "__main__":
    run()
