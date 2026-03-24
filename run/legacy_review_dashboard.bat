@echo off
title JobEngine Legacy Review Dashboard
cd /d %~dp0\..
if exist ..\..\.venv\Scripts\activate call ..\..\.venv\Scripts\activate
echo.
echo  Launching legacy review dashboard...
echo.
python scripts\legacy_review_dashboard.py
