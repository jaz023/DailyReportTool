@echo off
chcp 65001 >nul
echo ==========================================
echo Daily Report Generator
echo ==========================================

REM 切換到 bat 檔所在的資料夾
cd /d %~dp0

REM 檢查 python 是否存在
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] 找不到 Python，請先安裝 Python 3.x
    echo https://www.python.org/downloads/windows/
    pause
    exit /b
)

REM 執行主程式
python fill_report.py

echo.
echo ==========================================
echo 執行完成，請按任意鍵關閉
pause
