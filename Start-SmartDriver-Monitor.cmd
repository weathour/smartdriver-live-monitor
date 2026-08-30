@echo off
cd /d "%~dp0"
py -3 smartdriver_monitor.py
if errorlevel 1 (
  echo.
  echo Failed to start. Install Python 3 with Tcl/Tk, then run this file again.
  pause
)
