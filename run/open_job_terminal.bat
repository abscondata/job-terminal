@echo off
setlocal
title Job Terminal
cd /d %~dp0\..
if exist ..\..\.venv\Scripts\activate call ..\..\.venv\Scripts\activate
echo.
echo  Starting Job Terminal...
echo  Opening browser...
echo  Server running at http://127.0.0.1:8765
echo.
python scripts\start_job_terminal.py
if errorlevel 1 (
  echo.
  echo  ERROR: Job Terminal failed to start.
  echo  See the stack trace above.
  echo.
  pause
)
endlocal
