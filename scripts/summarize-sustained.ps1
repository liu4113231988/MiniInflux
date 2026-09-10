param([Parameter(Mandatory)][string]$RunPath)
$ErrorActionPreference = 'Stop'
$r = Get-Content (Join-Path $RunPath 'result.json') -Raw | ConvertFrom-Json
$samples = @(Get-Content (Join-Path $RunPath 'requests.json') -Raw | ConvertFrom-Json)
$telemetry = @(Get-Content (Join-Path $RunPath 'telemetry.json') -Raw | ConvertFrom-Json | Where-Object Stats)
function Percentile($items, [double]$p) {
    $sorted = @($items | Sort-Object)
    if (-not $sorted.Count) { return $null }
    $sorted[[Math]::Max(0, [int][Math]::Ceiling($sorted.Count * $p) - 1)]
}
$windows = @(for ($start = 0; $start -lt $r.Seconds; $start += 30) {
    $end = [Math]::Min($start + 30, $r.Seconds)
    $s = @($samples | Where-Object { $_.EndSeconds -ge $start -and $_.EndSeconds -lt $end })
    $t = @($telemetry | Where-Object { $_.Seconds -ge $start -and $_.Seconds -lt $end })
    [pscustomobject]@{
        Start=$start; End=$end; Throughput=[Math]::Round(($s | Measure-Object Points -Sum).Sum / ($end - $start));
        P50Ms=(Percentile $s.LatencyMs .5); P95Ms=(Percentile $s.LatencyMs .95); P99Ms=(Percentile $s.LatencyMs .99);
        Failed=@($s | Where-Object Error).Count; PrepareP99Ms=(Percentile $s.PrepareMs .99);
        PeakPrivateMB=($t.PrivateBytes | Measure-Object -Maximum).Maximum / 1MB;
        PeakBuffer=($t.Stats.MemoryBufferPoints | Measure-Object -Maximum).Maximum;
        PeakQueue=($t.Stats.WriteQueuePendingRequests | Measure-Object -Maximum).Maximum;
        SegmentsAtEnd=($t | Select-Object -Last 1).Stats.SegmentCount;
        CompactionsAtEnd=($t | Select-Object -Last 1).Stats.CompactionCount
    }
})
$stages = [ordered]@{}
foreach ($p in $r.After.WritePhaseMilliseconds.PSObject.Properties) { $stages[$p.Name] = $p.Value - $r.Before.WritePhaseMilliseconds.($p.Name) }
$summary = [pscustomobject]@{
    RunPath=$RunPath; Concurrency=$r.Concurrency; Valid=$r.Valid; SuccessfulPoints=$r.SuccessfulPoints; StoredPoints=$r.StoredPoints;
    FailedRequests=$r.FailedRequests; FlushMs=$r.FlushMs;
    Throughput=$r.SuccessfulPoints / ($samples.EndSeconds | Measure-Object -Maximum).Maximum;
    P50Ms=(Percentile $samples.LatencyMs .5); P95Ms=(Percentile $samples.LatencyMs .95); P99Ms=(Percentile $samples.LatencyMs .99);
    PeakPrivateMB=($telemetry.PrivateBytes | Measure-Object -Maximum).Maximum / 1MB;
    PeakWorkingSetMB=($telemetry.WorkingSet | Measure-Object -Maximum).Maximum / 1MB;
    PeakBuffer=($telemetry.Stats.MemoryBufferPoints | Measure-Object -Maximum).Maximum;
    PeakQueue=($telemetry.Stats.WriteQueuePendingRequests | Measure-Object -Maximum).Maximum;
    TelemetryErrors=@(Get-Content (Join-Path $RunPath 'telemetry.json') -Raw | ConvertFrom-Json | Where-Object Error).Count;
    Stages=$stages; Windows=$windows
}
$summary | ConvertTo-Json -Depth 8 | Set-Content (Join-Path $RunPath 'summary.json')
$summary | ConvertTo-Json -Depth 8
