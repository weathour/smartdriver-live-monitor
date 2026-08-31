@echo off
cd /d "%~dp0"
py -3 smartdriver_monitor.py --self-test
if errorlevel 1 goto done
py -3 smartdriver_monitor.py --network-self-test
:done
set "status=%errorlevel%"
pause
exit /b %status%
