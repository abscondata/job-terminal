@echo off
title JobEngine Direction Discovery (Quick)
cd /d %~dp0\..
if exist ..\..\.venv\Scripts\activate call ..\..\.venv\Scripts\activate
echo.
echo  Running quick discovery (lane-based)...
echo.
python scripts\run_discovery.py --quick
echo.
echo  ================================
echo   Done. Open the report in paris_direction_engine\data\runs\...
echo  ================================
echo.
pause
