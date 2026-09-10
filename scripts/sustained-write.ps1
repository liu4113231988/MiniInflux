param(
    [ValidateRange(1, 3600)][int]$Seconds = 300,
    [ValidateRange(1, 64)][int]$Concurrency = 4,
    [ValidateRange(1, 100000)][int]$BatchSize = 5000,
    [switch]$DiagnosticLog
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$run = Join-Path $root ('.benchmarks/sustained-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $run | Out-Null
$base = 'http://127.0.0.1:18086'
if ([System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners().Port -contains 18086) { throw 'Port 18086 already occupied' }
Add-Type -Path (Join-Path $PSScriptRoot 'SustainedWriteLoad.cs')
$settings = @{
    Auth__Enabled='false'; ASPNETCORE_ENVIRONMENT='Development'; Urls=$base;
    Data__Dir=(Join-Path $run 'data'); Http__BindAddress='127.0.0.1:18086';
    Data__QueryLogEnabled='false'; Http__LogEnabled='false'; Http__SuppressWriteLog='true';
    Logging__ConsoleEnabled='false'; Logging__FileEnabled=$DiagnosticLog.IsPresent.ToString();
    Logging__FilePath=(Join-Path $run 'service.log'); Logging__Level='Warning'; MiniInflux__WriteDiagnostics='true';
    MiniInflux__FlushThreshold='50000'
}
$saved = @{}
$proc = $null
function Query([string]$sql) {
    Invoke-RestMethod "$base/query?db=sustained&q=$([uri]::EscapeDataString($sql))" -Method Post -TimeoutSec 180
}
try {
    foreach ($key in $settings.Keys) { $saved[$key] = [Environment]::GetEnvironmentVariable($key); [Environment]::SetEnvironmentVariable($key, $settings[$key]) }
    $binary = Join-Path $root 'MiniInflux/bin/Release/net10.0/MiniInflux.dll'
    $proc = Start-Process dotnet -ArgumentList @('"' + $binary + '"') -WorkingDirectory $root -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $run 'stdout.log') -RedirectStandardError (Join-Path $run 'stderr.log')
    $ready = $false
    foreach ($attempt in 1..100) {
        try { Invoke-WebRequest "$base/ping" -TimeoutSec 1 | Out-Null; $ready = $true; break } catch { Start-Sleep -Milliseconds 200 }
    }
    if (-not $ready) { throw 'Server did not start' }
    Query 'CREATE DATABASE sustained' | Out-Null
    $before = Invoke-RestMethod "$base/debug/stats"
    $load = [SustainedWriteLoad]::new()
    $watch = [Diagnostics.Stopwatch]::StartNew()
    $task = $load.RunAsync("$base/write?db=sustained&precision=ns", $Seconds, $Concurrency, $BatchSize)
    $telemetry = [Collections.Generic.List[object]]::new()
    while (-not $task.IsCompleted) {
        $proc.Refresh()
        try {
            $stats = Invoke-RestMethod "$base/debug/stats" -TimeoutSec 10
            $telemetry.Add([pscustomobject]@{Seconds=$watch.Elapsed.TotalSeconds; WorkingSet=$proc.WorkingSet64; PrivateBytes=$proc.PrivateMemorySize64; CpuSeconds=$proc.TotalProcessorTime.TotalSeconds; Stats=$stats})
        } catch { $telemetry.Add([pscustomobject]@{Seconds=$watch.Elapsed.TotalSeconds; Error=$_.Exception.Message}) }
        $telemetry | ConvertTo-Json -Depth 12 | Set-Content (Join-Path $run 'telemetry.json')
        Start-Sleep -Seconds 2
    }
    $task.GetAwaiter().GetResult() | Out-Null
    $samples = $load.Samples.ToArray()
    $flush = [Diagnostics.Stopwatch]::StartNew()
    Invoke-RestMethod "$base/admin/api/maintenance/flush" -Method Post -ContentType 'application/json' -Body '' -TimeoutSec 180 | Out-Null
    $flush.Stop()
    $after = Invoke-RestMethod "$base/debug/stats"
    $count = Query 'SELECT count(value) FROM cpu'
    $samples | ConvertTo-Json -Depth 4 | Set-Content (Join-Path $run 'requests.json')
    $count | ConvertTo-Json -Depth 12 | Set-Content (Join-Path $run 'count.json')
    $stored = 0L
    foreach ($series in $count.results[0].series) { foreach ($row in $series.values) { $stored += [long]$row[1] } }
    $countChecks = @($stored)
    foreach ($check in 1..2) {
        $repeat = Query 'SELECT count(value) FROM cpu'
        if ($repeat.results[0].error) { throw $repeat.results[0].error }
        $repeatCount = 0L
        foreach ($series in $repeat.results[0].series) { foreach ($row in $series.values) { $repeatCount += [long]$row[1] } }
        $countChecks += $repeatCount
    }
    $success = ($samples | Measure-Object Points -Sum).Sum
    $errors = @($samples | Where-Object Error)
    $report = [pscustomobject]@{Seconds=$Seconds; Concurrency=$Concurrency; BatchSize=$BatchSize; SuccessfulPoints=$success; StoredPoints=$stored; CountChecks=$countChecks; FailedRequests=$errors.Count; Valid=(@($countChecks | Where-Object { $_ -ne $success }).Count -eq 0 -and $errors.Count -eq 0); FlushMs=$flush.Elapsed.TotalMilliseconds; Before=$before; After=$after; Path=$run; BinarySha256=(Get-FileHash $binary).Hash}
    $report | ConvertTo-Json -Depth 12 | Set-Content (Join-Path $run 'result.json')
    $report | Select-Object Seconds,Concurrency,SuccessfulPoints,StoredPoints,FailedRequests,Valid,FlushMs,Path | ConvertTo-Json
    if (-not $report.Valid) { throw "Write validation failed; evidence saved in $run" }
} finally {
    foreach ($key in $saved.Keys) { [Environment]::SetEnvironmentVariable($key, $saved[$key]) }
    if ($proc -and -not $proc.HasExited) { Stop-Process -Id $proc.Id -Force; $proc.WaitForExit() }
}
