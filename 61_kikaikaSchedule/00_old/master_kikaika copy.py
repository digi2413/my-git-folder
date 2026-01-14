# master_kikaika.py
# 目的：
# - 4ステップを import して順番に実行（subprocess無し）
# - 成功/失敗を集計してOutlookでメール送信
# - assyExcelInport の「採用ファイル」は print解析せず、戻り値で取得
# - 添付は「存在するものだけ」付ける
# - 失敗時は終了コード 1（タスクスケジューラ用）

import os
import sys
import traceback
from datetime import datetime

import win32com.client as win32

import assyExcelInport
import AssyScheduleUpdate
import child_requirements
import kikaikaSchedule


TO_ADDRESS = "koya.chida@digi.jp"

# 添付したい生成物（存在するものだけ添付する）
ATTACHMENTS = [
    r"\\tiss-ntsrv\TISS-PCS\第三組立課日程（包装機)\Combined_Assembly_Schedule.xlsx",
    r"\\192.168.134.32\share\00_データ\01_所要計算(組立自動日程)\child_requirements.csv",
    r"\\192.168.134.32\share\00_データ\01_所要計算(組立自動日程)\kikaikaSchedule.csv",
]

STEPS = [
    ("Excel取込（組立予定）", assyExcelInport.run),
    ("組立日程DB更新", AssyScheduleUpdate.run),
    ("子部品所要計算", child_requirements.run),
    ("機械課 所要日程出力", kikaikaSchedule.run),  # run化している前提（無いなら main に変更）
]


def send_mail_outlook(subject: str, body: str, to_address: str, attachments=None) -> None:
    outlook = win32.Dispatch("Outlook.Application")
    mail = outlook.CreateItem(0)
    mail.To = to_address
    mail.Subject = subject
    mail.Body = body

    if attachments:
        for f in attachments:
            if f and os.path.exists(f):
                mail.Attachments.Add(f)

    mail.Send()


def safe_mtime(path: str):
    try:
        return datetime.fromtimestamp(os.path.getmtime(path))
    except Exception:
        return None


def main():
    started = datetime.now()

    results = []
    all_success = True

    # assyExcelInport の戻り値（採用ファイルなど）
    assy_info = None

    # 1) ステップ実行（失敗しても最後まで回す）
    for step_name, func in STEPS:
        try:
            ret = func()
            if step_name == "Excel取込（組立予定）":
                assy_info = ret  # dict想定

            results.append((step_name, True, "OK"))
        except Exception:
            all_success = False
            # 読みやすさ優先で末尾だけ
            msg = traceback.format_exc()
            results.append((step_name, False, msg[-2000:]))

    # 2) 添付ファイル存在チェック（存在しない場合は失敗扱い）
    attach_status = []
    for f in ATTACHMENTS:
        exists = os.path.exists(f)
        attach_status.append((f, exists))
        if not exists:
            all_success = False

    # 3) 件名
    subject = "✅ 全処理成功（機械課日程）" if all_success else "❌ 処理失敗あり（機械課日程）"

    # 4) 本文
    lines = []
    lines.append(f"開始: {started.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"終了: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # 追加情報（assy採用ファイル）
    lines.append("■ 追加情報")
    picked_files = None
    if isinstance(assy_info, dict):
        picked_files = assy_info.get("picked_files")

    if picked_files and isinstance(picked_files, (list, tuple)):
        lines.append("- assyExcelInport 採用ファイル:")
        for p in picked_files:
            lines.append(f"  - {p}")
    else:
        lines.append("- assyExcelInport 採用ファイル: 取得できず（assyExcelInport.run()の戻り値を確認）")

    lines.append("")
    lines.append("■ ステップ結果")
    for step, ok, msg in results:
        lines.append(f"- {step}: {'成功' if ok else '失敗'} → {msg}")

    lines.append("")
    lines.append("■ 添付ファイル確認（存在するものだけ添付）")
    for f, ok in attach_status:
        name = os.path.basename(f)
        if ok:
            mt = safe_mtime(f)
            mt_txt = mt.strftime("%Y-%m-%d %H:%M:%S") if mt else "mtime不明"
            lines.append(f"- {name}: OK（更新: {mt_txt}）")
        else:
            lines.append(f"- {name}: NG（未生成）")

    body = "\n".join(lines)

    # 5) メール送信
    send_mail_outlook(subject, body, TO_ADDRESS, attachments=ATTACHMENTS)

    # 6) タスクスケジューラ用終了コード
    sys.exit(0 if all_success else 1)


if __name__ == "__main__":
    main()
