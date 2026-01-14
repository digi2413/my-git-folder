import os
import shutil
import pandas as pd

# フォルダパス
input_folder = r'C:\Users\di2413\OneDrive - DIGIWORLD Cloud\32_第3組立\02_白金製作所'
output_folder = r'C:\Users\di2413\OneDrive - DIGIWORLD Cloud\32_第3組立\02_白金製作所\統合ファイル'
output_file = os.path.join(output_folder, '出力サンプル_統合.xlsx')
comp_folder = os.path.join(input_folder, 'comp')

# フィールドのマッピング
field_mapping = {
    'code': ['code', '品番', '部番', 'CODE', 'コード'],
    'request_count': ['依頼数', '10日分に', '週', '依頼数量'],
    'comment': ['コメント', 'ｺﾒﾝﾄ', '支給希望日']
}

# データを格納するリスト
data_frames = []

# データを抽出する関数
def extract_data(df, file_path):
    column_indices = {}
    
    # フィールド名を探す
    for key, values in field_mapping.items():
        for value in values:
            for col in df.columns:
                # フィールド名の部分一致検索
                if df[col].astype(str).str.contains(f".*{value}.*", case=False).any():
                    column_indices[key] = col
                    # ファイル名に "DG支給依頼" が含まれている場合に処理を停止
                    if "DG支給依頼" in os.path.basename(file_path):
                        break
            if key in column_indices:
                break

    # 必要な列が見つかった場合、データを抽出
    if 'code' in column_indices and 'request_count' in column_indices:
        columns_to_extract = [column_indices['code'], column_indices['request_count']]
        if 'comment' in column_indices:
            columns_to_extract.append(column_indices['comment'])

        selected_data = df[columns_to_extract].dropna(how='all')
        selected_data.columns = ['code', 'request_count'] + (['comment'] if 'comment' in column_indices else [])
        selected_data['request_count'] = selected_data['request_count'].apply(lambda x: str(x).strip())
        # selected_data['request_count'] = pd.to_numeric(selected_data['request_count'], errors='coerce').abs()

        # 空白を除去
        selected_data['code'] = selected_data['code'].str.replace(' ', '', regex=False)

        # 9桁もしくは14桁のcodeのみを残す
        selected_data = selected_data[selected_data['code'].str.len().isin([9, 14])]

        # フィルタリングしてフィールド名の行を削除
        selected_data = selected_data[selected_data['code'].str.contains('|'.join(field_mapping['code']), case=False) == False]
        selected_data['source_file'] = os.path.basename(file_path)  # ファイル名を追加

        data_frames.append(selected_data)
    else:
        print(f"必要なフィールドが見つかりませんでした: {file_path}")
    
    return False  # 処理を続行するためにFalseを返す

# フォルダ内の全ての .xlsx と .xls ファイルを処理
with os.scandir(input_folder) as entries:
    for entry in entries:
        if entry.is_file() and (entry.name.endswith('.xlsx') or entry.name.endswith('.xls')) and entry.path != output_file:
            try:
                if entry.name.endswith('.xls'):
                    df = pd.read_excel(entry.path, header=None, engine='xlrd')
                else:
                    df = pd.read_excel(entry.path, header=None)
                stop = extract_data(df, entry.path)
                if stop:
                    break
                # 処理が終わったファイルを comp フォルダに移動
                shutil.move(entry.path, os.path.join(comp_folder, entry.name))
            except Exception as e:
                print(f"エラーが発生しました: {entry.path}, エラー: {e}")

# 全てのデータを統合
if data_frames:
    merged_data = pd.concat(data_frames, ignore_index=True)

    # コメント列がない場合に対応
    if 'comment' not in merged_data.columns:
        merged_data['comment'] = None

    # ファイル名を先頭列に移動
    merged_data = merged_data[['source_file', 'code', 'request_count', 'comment']]

    # データをエクセルファイルに書き出し
    with pd.ExcelWriter(output_file, engine='openpyxl', mode='w') as writer:
        merged_data.to_excel(writer, index=False, sheet_name='統合データ')

    print("データが統合され、出力サンプル_統合.xlsxとして保存されました。")
else:
    print("統合するデータがありませんでした。")

print("完了")