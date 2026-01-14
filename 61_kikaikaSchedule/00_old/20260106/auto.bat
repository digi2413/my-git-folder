@echo off
cd C:\Users\di2413\Desktop\MyPython\61_kikaikaSchedule
python assyExcelInport.py
python AssyScheduleUpdate.py
python child_requirements.py
python kikaikaSchedule.py
python run_and_mail.py

