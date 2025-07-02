# PowerShell script to run the Ulterra file merger
# This can be scheduled to run automatically

Write-Host "Starting Ulterra File Merge..." -ForegroundColor Green

# Get the directory where this script is located
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $ScriptDir

# Run the Python script
$PythonPath = "c:/Users/Colin.anderson/OneDrive - Whitecap Resources/Python Scripting/Propietary Bits/.venv/Scripts/python.exe"
$ScriptPath = "auto_merge_ulterra.py"

try {
    & $PythonPath $ScriptPath
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Merge completed successfully!" -ForegroundColor Green
    } else {
        Write-Host "Merge failed with error code $LASTEXITCODE" -ForegroundColor Red
        exit $LASTEXITCODE
    }
} catch {
    Write-Host "Error running merge script: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Uncomment the line below if you want the window to stay open
# Read-Host "Press Enter to continue..."
