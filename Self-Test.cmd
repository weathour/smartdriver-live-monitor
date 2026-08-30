@echo off
cd /d "%~dp0"
py -3 smartdriver_monitor.py --self-test
set "status=%errorlevel%"
pause
exit /b %status%
