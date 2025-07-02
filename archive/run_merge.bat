@echo off
REM Batch file to run the Ulterra file merger
REM This can be scheduled to run automatically

echo Starting Ulterra File Merge...
cd /d "%~dp0"

REM Run the Python script
"c:/Users/Colin.anderson/OneDrive - Whitecap Resources/Python Scripting/Propietary Bits/.venv/Scripts/python.exe" auto_merge_ulterra.py

REM Check if the script ran successfully
if %errorlevel% equ 0 (
    echo Merge completed successfully!
) else (
    echo Merge failed with error code %errorlevel%
)

REM Uncomment the line below if you want the window to stay open
REM pause
