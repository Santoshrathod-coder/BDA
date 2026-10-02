# ====================================================================
# NetConnect CDR Analysis - Local Windows Simulator
# Runs the exact logic of Problems 1 to 5 directly on data/cdr.csv
# without requiring an active Hadoop cluster.
# ====================================================================

Write-Host "`n========================================================" -ForegroundColor Cyan
Write-Host " NETCONNECT CDR ANALYSIS - PROBLEMS 1 TO 5 EXECUTION " -ForegroundColor Cyan
Write-Host "========================================================`n" -ForegroundColor Cyan

$csvPath = "data\cdr.csv"
if (-not (Test-Path $csvPath)) {
    $csvPath = "NetConnect-CDR\data\cdr.csv"
}

if (-not (Test-Path $csvPath)) {
    Write-Error "Could not find data/cdr.csv in current directory!"
    exit 1
}

$lines = Get-Content $csvPath | Select-Object -Skip 1

# ----------------------------------------------------
# PROBLEM 1: TOWER CALL-DROP RATE
# ----------------------------------------------------
Write-Host ">>> RUNNING PROBLEM 1: TOWER CALL-DROP RATE <<<" -ForegroundColor Yellow
Write-Host "Formula: Drop Rate = Dropped Calls / Total Calls`n"

$p1 = $lines | ForEach-Object {
    $parts = $_.Split(',')
    [PSCustomObject]@{
        Tower = $parts[1].Trim()
        Dropped = if ($parts[4].Trim() -eq 'Y') { 1 } else { 0 }
    }
} | Group-Object Tower | ForEach-Object {
    $tot = $_.Count
    $drop = ($_.Group | Measure-Object Dropped -Sum).Sum
    $rate = [math]::Round([double]$drop / [double]$tot, 4)
    [PSCustomObject]@{
        "towerId" = $_.Name
        "totalCalls" = $tot
        "droppedCalls" = $drop
        "dropRate" = [string]::Format("{0:0.0000}", $rate)
    }
} | Sort-Object towerId

$p1 | Format-Table -AutoSize
Write-Host "Interpretation: Tower T02 has the highest drop rate (66.67%).`n" -ForegroundColor Green

# ----------------------------------------------------
# PROBLEM 2: AVERAGE CALL DURATION BY PLAN
# ----------------------------------------------------
Write-Host ">>> RUNNING PROBLEM 2: AVERAGE CALL DURATION BY DATA PLAN <<<" -ForegroundColor Yellow
Write-Host "Formula: Average Duration = Total Duration / Total Calls`n"

$p2 = $lines | ForEach-Object {
    $parts = $_.Split(',')
    [PSCustomObject]@{
        Plan = $parts[2].Trim()
        Duration = [int]$parts[3].Trim()
    }
} | Group-Object Plan | ForEach-Object {
    $avg = [math]::Round(($_.Group | Measure-Object Duration -Average).Average, 2)
    [PSCustomObject]@{
        "callerPlan" = $_.Name
        "averageDurationSec" = [string]::Format("{0:0.00}", $avg)
    }
} | Sort-Object callerPlan

$p2 | Format-Table -AutoSize
Write-Host "Interpretation: Premium users have highest duration (473.18s), Prepaid lowest (145.39s).`n" -ForegroundColor Green

# ----------------------------------------------------
# PROBLEM 3: TOTAL CALL VOLUME BY TOWER
# ----------------------------------------------------
Write-Host ">>> RUNNING PROBLEM 3: TOTAL CALL VOLUME BY TOWER <<<" -ForegroundColor Yellow
Write-Host "Formula: Call Volume = Total Number of Calls per Tower`n"

$p3 = $lines | ForEach-Object {
    $parts = $_.Split(',')
    $parts[1].Trim()
} | Group-Object | ForEach-Object {
    [PSCustomObject]@{
        "towerId" = $_.Name
        "totalCallVolume" = $_.Count
    }
} | Sort-Object towerId

$p3 | Format-Table -AutoSize
Write-Host "Interpretation: Tower T02 handled the highest volume (24 calls).`n" -ForegroundColor Green

# ----------------------------------------------------
# PROBLEM 4: PEAK CALL LOAD BY TIME OF DAY
# ----------------------------------------------------
Write-Host ">>> RUNNING PROBLEM 4: PEAK CALL LOAD BY TIME OF DAY <<<" -ForegroundColor Yellow
Write-Host "Extracting Hour (00-23) from timestamp...`n"

$hourly = $lines | ForEach-Object {
    $parts = $_.Split(',')
    $parts[5].Trim().Split(' ')[1].Split(':')[0]
} | Group-Object | ForEach-Object {
    [PSCustomObject]@{
        "Hour" = $_.Name
        "CallCount" = $_.Count
    }
} | Sort-Object Hour

$hourly | Format-Table -AutoSize
$peak = $hourly | Sort-Object CallCount -Descending | Select-Object -First 1
Write-Host "--------------------------------------------------------"
Write-Host "PEAK HOUR: $($peak.Hour) | TOTAL CALLS: $($peak.CallCount)" -ForegroundColor Magenta
Write-Host "--------------------------------------------------------`n"

# ----------------------------------------------------
# PROBLEM 5: HIGHEST DROPPED-CALL PROPORTION BY DATA PLAN
# ----------------------------------------------------
Write-Host ">>> RUNNING PROBLEM 5: DROPPED-CALL PROPORTION BY DATA PLAN <<<" -ForegroundColor Yellow
Write-Host "Formula: Drop Rate Proportion = Dropped Calls / Total Calls`n"

$p5 = $lines | ForEach-Object {
    $parts = $_.Split(',')
    [PSCustomObject]@{
        Plan = $parts[2].Trim()
        Dropped = if ($parts[4].Trim() -eq 'Y') { 1 } else { 0 }
    }
} | Group-Object Plan | ForEach-Object {
    $tot = $_.Count
    $drop = ($_.Group | Measure-Object Dropped -Sum).Sum
    $rate = [math]::Round([double]$drop / [double]$tot, 4)
    [PSCustomObject]@{
        "callerPlan" = $_.Name
        "totalCalls" = $tot
        "droppedCalls" = $drop
        "dropRateProportion" = [string]::Format("{0:0.0000}", $rate)
    }
} | Sort-Object callerPlan

$p5 | Format-Table -AutoSize
$worstPlan = $p5 | Sort-Object { [double]$_.dropRateProportion } -Descending | Select-Object -First 1
Write-Host "----------------------------------------------------------------------------------"
Write-Host "HIGHEST DROP RATE PLAN: $($worstPlan.callerPlan) (Total: $($worstPlan.totalCalls), Dropped: $($worstPlan.droppedCalls), Rate: $($worstPlan.dropRateProportion))" -ForegroundColor Magenta
Write-Host "----------------------------------------------------------------------------------`n"

Write-Host "Execution completed successfully!`n" -ForegroundColor Cyan
