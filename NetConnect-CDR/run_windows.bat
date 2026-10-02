@echo off
title NetConnect CDR Analysis
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0simulate_all.ps1"
echo.
pause
