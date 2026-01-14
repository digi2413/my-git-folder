@echo off
set ExcelFilePath="C:\Users\di2413\OneDrive - DIGIWORLD Cloud\23_第2組立\02_ツール\06_機械課日程\機械課日程.xlsm"  REM エクセルファイルのパスを設定
set VBAScript="import"  REM 実行するVBAマクロの名前を設定

REM 一時的なVBSファイルを作成してVBAを実行
echo Set objExcel = CreateObject("Excel.Application") > temp.vbs
echo objExcel.Visible = True >> temp.vbs
echo objExcel.Workbooks.Open %ExcelFilePath%, 0, False >> temp.vbs
echo objExcel.Run %VBAScript% >> temp.vbs
echo Set objExcel = Nothing >> temp.vbs

REM VBSファイルを実行
cscript /nologo temp.vbs

REM 一時的なVBSファイルを削除
del temp.vbs

set ExcelFilePath="C:\Users\di2413\OneDrive - DIGIWORLD Cloud\23_第2組立\02_ツール\06_機械課日程\機械課日程_塗装外.xlsm"  REM エクセルファイルのパスを設定
set VBAScript="import"  REM 実行するVBAマクロの名前を設定

REM 一時的なVBSファイルを作成してVBAを実行
echo Set objExcel = CreateObject("Excel.Application") > temp.vbs
echo objExcel.Visible = True >> temp.vbs
echo objExcel.Workbooks.Open %ExcelFilePath%, 0, False >> temp.vbs
echo objExcel.Run %VBAScript% >> temp.vbs
echo Set objExcel = Nothing >> temp.vbs

REM VBSファイルを実行
cscript /nologo temp.vbs

REM 一時的なVBSファイルを削除
del temp.vbs
