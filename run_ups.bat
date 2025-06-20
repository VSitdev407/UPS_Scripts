@echo off
echo processing...

REM 切換到目前 bat 所在資料夾
cd /d "%~dp0"

REM 啟用虛擬環境
call .venv\Scripts\activate.bat

REM 執行主程式
python main_v2.py

REM 停用虛擬環境（可選）
call .venv\Scripts\deactivate.bat

echo Done