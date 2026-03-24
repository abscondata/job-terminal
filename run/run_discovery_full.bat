@echo off
title JobEngine Direction Discovery (Full)
cd /d %~dp0\..
if exist ..\..\.venv\Scripts\activate call ..\..\.venv\Scripts\activate
echo.
echo  Running full discovery (lane-based)...
echo.
python scripts\run_discovery.py
echo.
echo  ================================
echo   Done. Open the report in paris_direction_engine\data\runs\...
echo  ================================
echo.
pause
