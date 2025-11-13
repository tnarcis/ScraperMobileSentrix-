@echo off
REM Created by Arslan Basharat
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0\.."

color 0a
call :init_vars
call :banner
call :log INFO "Initializing MobileSentrix V8 launchpad"
call :log INFO "Script crafted by Arslan Basharat"

call :select_python
if not exist "%PYTHON_EXE%" (
    call :log ERROR "Python interpreter not found at %PYTHON_EXE%"
    call :log INFO "Update the script or install the required interpreter, then retry."
    goto :shutdown
)

call :log INFO "Smoke-testing core modules"
"%PYTHON_EXE%" -c "import app, enhanced_scrapers, database, logger" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    call :log ERROR "Module import failed. Check %LOG_FILE% for stack traces."
    type "%LOG_FILE%"
    goto :shutdown
)

call :log SUCCESS "All systems look sharp"
call :log INFO "Flask UI: http://localhost:5000"
call :log INFO "Dashboard: http://localhost:5000/results"
call :log INFO "Streaming live output below (also logged to %LOG_FILE%)"
echo.

:set_restart
set "RESTART_COUNT=0"
call :log INFO "Auto-restart window allows %MAX_RESTARTS% retries"

:launch_server
set /a "ATTEMPT=RESTART_COUNT+1"
call :log INFO "Launch attempt !ATTEMPT!"
powershell -NoProfile -ExecutionPolicy Bypass -Command "& { param($exe,$script,$log) Write-Host '==================== LIVE SERVER FEED ===================='; & \"$exe\" $script 2>&1 | Tee-Object -FilePath $log -Append; Write-Host '==================== SERVER FEED ENDED ====================' }" "%PYTHON_EXE%" "app.py" "%LOG_FILE%"
set "SERVER_EXIT=%errorlevel%"

if "%SERVER_EXIT%" EQU "0" (
    call :log INFO "Server stopped by user request"
    goto :shutdown
)

call :log WARN "Server exited unexpectedly with code %SERVER_EXIT%"
set /a "RESTART_COUNT+=1"
if !RESTART_COUNT! LEQ %MAX_RESTARTS% (
    call :log INFO "Attempting restart in 5 seconds (retry !RESTART_COUNT! of %MAX_RESTARTS%)"
    timeout /t 5 /nobreak >nul
    goto :launch_server
)

call :log ERROR "Max restart attempts exhausted. Manual intervention required."

:shutdown
call :log INFO "Log file stored at %LOG_FILE%"
call :log INFO "Press any key to close the launchpad"
echo.
pause >nul
exit /b

:init_vars
for /f %%I in ('powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-Date -Format yyyy-MM-dd_HH-mm-ss"') do set "STAMP=%%I"
set "ROOT_DIR=%~dp0.."
set "LOG_DIR=%ROOT_DIR%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\startup-%STAMP%.log"
set "DEFAULT_PY=C:\Users\Lenove T\AppData\Local\Programs\Python\Python314\python.exe"
set "VENV_PY=%ROOT_DIR%\.venv\Scripts\python.exe"
set "MAX_RESTARTS=3"
goto :eof

:banner
echo.
echo ###############################################################
echo ##                 MOBILE SENTRIX V8 LAUNCHPAD               ##
echo ##       Spinning up vibes, scraping dreams, breaking limits ##
echo ###############################################################
echo.
goto :eof

:select_python
if exist "%VENV_PY%" (
    set "PYTHON_EXE=%VENV_PY%"
    call :log INFO "Using virtual environment interpreter"
) else (
    set "PYTHON_EXE=%DEFAULT_PY%"
    call :log WARN "Virtual environment missing. Falling back to system Python."
)
goto :eof

:log
set "LEVEL=%~1"
set "MESSAGE=%~2"
for /f %%I in ('powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-Date -Format HH:mm:ss"') do set "NOW=%%I"
set "LINE=[!NOW!] [%LEVEL%] %~2"
echo !LINE!
>> "%LOG_FILE%" echo !LINE!
goto :eof
