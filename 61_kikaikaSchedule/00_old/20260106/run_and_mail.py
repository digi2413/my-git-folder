# run_kikaika_and_mail.py
# 目的：
# - 4本のPythonを順番に実行
# - 成功/失敗を集計してOutlookで1通メール送信
# - 追加情報（assy採用ファイル/対象期間/件数）を本文に入れる
# - 生成CSV/XLSXを添付（無い場合は失敗扱いにするが、メール送信は続行）

import os
import re
import sys
import subprocess
from datetime import datetime, timedelta

import win32com.client as win32


# =========================
# 設定（ここだけ必要に応じて変更）
# =========================
SCRIPTS = [
    ("assyExcelInport.py", "Excel取込（組立予定）"),
    ("AssyScheduleUpdate.py", "組立日程DB更新"),
    ("child_requirements.py", "子部品所要計算"),
    ("kikaikaSchedule.py", "機械課 所要日程出力"),
]

TO_ADDRESS = "koya.chida@digi.jp"

# 添付したい生成物（存在しない場合は失敗扱いにする）
ATTACHMENTS = [
    r"\\tiss-ntsrv\TISS-PCS\第三組立課日程（包装機)\Combined_Assembly_Schedule.xlsx",
    r"\\192.168.134.32\share\00_データ\01_所要計算(組立自動日程)\child_requirements.csv",
    r"\\192.168.134.32\share\00_データ\01_所要計算(組立自動日程)\kikaikaSchedule.csv",
]

# assyExcelInport の対象期間表記（今のロジック：今日～4週間）
TARGET_DAYS = 28


# =========================
# Outlookメール送信
# =========================
def send_mail_outlook(subject: str, body: str, to_address: str, attachments=None) -> bool:
    try:
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
        print("📧 メール送信完了")
        return True
    except Exception as e:
        print(f"メール送信失敗: {e}")
        return False


# =========================
# 実行＋ログ取得
# =========================
def run_py(pyfile: str):
    """
    python script 実行して、(success, log_text, returncode) を返す
    """
    p = subprocess.run(
        [sys.executable, pyfile],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    log = (p.stdout or "")
    if p.stderr:
        log += "\n" + p.stderr
    return (p.returncode == 0), log, p.returncode


# =========================
# ログから情報抽出
# =========================
def extract_assy_files(log: str):
    """
    assyExcelInport.py の「採用ファイル:」以降の " - " 行を拾う想定
    例:
      採用ファイル:
       - \\server\...\AW ....xlsx / mtime=...
    """
    files = []
    in_block = False
    for line in log.splitlines():
        if "採用ファイル" in line:
            in_block = True
            continue
        if in_block:
            s = line.strip()
            if s.startswith("-"):
                files.append(s.lstrip("-").strip())
    return files


def extract_rows(log: str, key: str):
    """
    例:
      child_requirements.py: "result rows: 123"
      kikaikaSchedule.py    : "kikaika rows: 123"  ← kikaika側にprint追加が必要
    """
    m = re.search(rf"{re.escape(key)}\s*:\s*(\d+)", log)
    return int(m.group(1)) if m else None


# =========================
# メイン
# =========================
def main():
    started = datetime.now()

    results = []
    logs = {}
    rcs = {}

    # 1) スクリプト実行（失敗しても最後まで回す）
    for pyfile, label in SCRIPTS:
        success, log, rc = run_py(pyfile)
        logs[pyfile] = log
        rcs[pyfile] = rc

        # 本文用のメッセージ
        if success:
            detail = "OK"
        else:
            # 長すぎると読みにくいので末尾だけ
            detail = (log[-1500:] if log else f"returncode={rc}")

        results.append((label, success, detail))

    all_success = all(s for _, s, _ in results)

    # 2) 追加情報
    target_from = started.date()
    target_to = target_from + timedelta(days=TARGET_DAYS)

    assy_files = extract_assy_files(logs.get("assyExcelInport.py", ""))
    child_rows = extract_rows(logs.get("child_requirements.py", ""), "result rows")
    kika_rows = extract_rows(logs.get("kikaikaSchedule.py", ""), "kikaika rows")

    # 3) 添付ファイル存在チェック（無ければ失敗扱い。ただしメール送信は続行）
    missing_files = [f for f in ATTACHMENTS if not os.path.exists(f)]
    if missing_files:
        all_success = False

    # 4) 件名
    subject = "✅ 全処理成功（機械課日程）" if all_success else "❌ 処理失敗あり（機械課日程）"

    # 5) 本文
    lines = []
    lines.append(f"開始: {started.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"終了: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    lines.append("■ 追加情報")
    lines.append(f"- assyExcelInport 対象期間: {target_from} ～ {target_to}")
    if assy_files:
        lines.append("- assyExcelInport 採用ファイル:")
        for f in assy_files:
            lines.append(f"  - {f}")
    else:
        lines.append("- assyExcelInport 採用ファイル: 取得できず（ログ形式確認）")

    lines.append(f"- child_requirements 出力件数: {child_rows if child_rows is not None else '取得できず'}")
    lines.append(f"- kikaikaSchedule 出力件数: {kika_rows if kika_rows is not None else '取得できず（kikaika rows をprintしてね）'}")

    lines.append("")
    lines.append("■ 添付ファイル確認（無い場合もメールは送る＝テスト可）")
    for f in ATTACHMENTS:
        if os.path.exists(f):
            lines.append(f"OK : {os.path.basename(f)}")
        else:
            lines.append(f"NG : {os.path.basename(f)}（未生成）")

    lines.append("")
    lines.append("■ ステップ結果")
    lines.append("\n".join(
        f"{step}: {'成功' if success else '失敗'} → {msg}"
        for step, success, msg in results
    ))

    lines.append("")
    lines.append("■ 終了コード")
    for pyfile, _ in SCRIPTS:
        lines.append(f"- {pyfile}: {rcs.get(pyfile)}")

    body = "\n".join(lines)

    # 6) メール送信（添付は存在するものだけ付く）
    send_mail_outlook(subject, body, to_address=TO_ADDRESS, attachments=ATTACHMENTS)

    print("全処理完了（メール送信まで）")

if __name__ == "__main__":
    main()
