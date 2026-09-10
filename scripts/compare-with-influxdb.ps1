param(
    [ValidateRange(1, 100000000)][int]$Points = 200000,
    [ValidateRange(1, 10000000)][int]$BatchSize = 5000,
    [ValidateRange(1, 1024)][int]$Concurrency = 1,
    [string]$Epoch = '',
    [int]$QueryIterations = 5,
    [switch]$BufferOnly,
    [switch]$MiniOnly,
    [switch]$WriteDiagnostics,
    [string]$MiniBinaryPath = ''
)

$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Net.Http
if (-not ('HttpWriteBenchmark' -as [type])) {
    Add-Type -Path (Join-Path $PSScriptRoot 'HttpWriteBenchmark.cs')
}

function New-BasicConfig {
    param(
        [string]$Path,
        [string]$BindAddress,
        [string]$MetaDir,
        [string]$DataDir,
        [string]$WalDir
    )

    $metaDir = $MetaDir.Replace('\', '/')
    $dataDir = $DataDir.Replace('\', '/')
    $walDir = $WalDir.Replace('\', '/')

    @"
reporting-disabled = true

[meta]
  dir = "$metaDir"

[data]
  dir = "$dataDir"
  wal-dir = "$walDir"
  series-id-set-cache-size = 100

[http]
  enabled = true
  bind-address = "$BindAddress"
  auth-enabled = false
  log-enabled = false
  write-tracing = false
  pprof-enabled = false
  ping-auth-enabled = false

[logging]
  level = "error"
"@ | Set-Content -LiteralPath $Path
}

function Wait-HttpReady {
    param(
        [System.Net.Http.HttpClient]$Client,
        [string]$Url,
        [int]$TimeoutSeconds = 60
    )

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $response = $Client.GetAsync($Url).GetAwaiter().GetResult()
            try { if ($response.IsSuccessStatusCode) { return } }
            finally { $response.Dispose() }
        }
        catch {
        }

        Start-Sleep -Milliseconds 250
    }

    throw "Timed out waiting for $Url"
}

function Invoke-Query {
    param(
        [System.Net.Http.HttpClient]$Client,
        [string]$BaseUrl,
        [string]$Database,
        [string]$Query,
        [bool]$Debug = $false,
        [string]$Epoch = ''
    )

    $debugParam = if ($Debug) { '&debug=true' } else { '' }
    $epochParam = if ([string]::IsNullOrWhiteSpace($Epoch)) { '' } else { "&epoch=$([Uri]::EscapeDataString($Epoch))" }
    $queryUrl = "$BaseUrl/query?db=$Database&q=$([Uri]::EscapeDataString($Query))$debugParam$epochParam"
    $content = [System.Net.Http.StringContent]::new('', [System.Text.Encoding]::UTF8, 'application/x-www-form-urlencoded')
    try {
        $response = $Client.PostAsync($queryUrl, $content).GetAwaiter().GetResult()
        try {
            $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
            if (-not $response.IsSuccessStatusCode) { throw "Query failed: $($response.StatusCode) $body" }
            $parsed = $body | ConvertFrom-Json
            if ($parsed.error -or @($parsed.results | Where-Object { $_.error }).Count -gt 0) {
                throw "Query returned an error: $body"
            }
            return $body
        }
        finally { $response.Dispose() }
    }
    finally { $content.Dispose() }
}

function Measure-Query {
    param(
        [System.Net.Http.HttpClient]$Client,
        [string]$BaseUrl,
        [string]$Database,
        [string]$Query,
        [string]$Epoch = '',
        [int]$Iterations = 5
    )

    $body = ''
    $times = @()
    for ($i = 0; $i -lt [Math]::Max(1, $Iterations); $i++) {
        $watch = [System.Diagnostics.Stopwatch]::StartNew()
        $body = Invoke-Query -Client $Client -BaseUrl $BaseUrl -Database $Database -Query $Query -Epoch $Epoch
        $watch.Stop()
        $times += $watch.Elapsed.TotalMilliseconds
    }

    $ordered = @($times | Sort-Object)
    $median = $ordered[[int][Math]::Floor($ordered.Count / 2)]
    return [pscustomobject]@{
        Body = $body
        MedianMs = [Math]::Round($median, 2)
        SamplesMs = @($times | ForEach-Object { [Math]::Round($_, 2) })
    }
}

function New-LineProtocolBatch {
    param(
        [int]$StartIndex,
        [int]$EndIndex
    )

    $builder = [System.Text.StringBuilder]::new()
    $baseTimestamp = 1710000000000000000L
    for ($i = $StartIndex; $i -lt $EndIndex; $i++) {
        $hostName = '{0:d2}' -f ($i % 16)
        $region = if (($i % 2) -eq 0) { 'cn' } else { 'us' }
        $value = [Math]::Round(($i % 1000) / 10.0, 3)
        $load = $i % 100
        $timestamp = $baseTimestamp + ($i * 1000000000L)
        [void]$builder.Append("cpu,host=server$hostName,region=$region value=$value,load=${load}i $timestamp`n")
    }

    return $builder.ToString()
}

function Get-StorageStats {
    param([string]$DataPath)

    $segments = @(Get-ChildItem -LiteralPath $DataPath -Recurse -Filter '*.seg' -File -ErrorAction SilentlyContinue)
    $wals = @(Get-ChildItem -LiteralPath $DataPath -Recurse -Filter '*.wal' -File -ErrorAction SilentlyContinue)
    return [pscustomobject]@{
        SegmentFiles = $segments.Count
        SegmentBytes = ($segments | Measure-Object -Property Length -Sum).Sum
        WalFiles = $wals.Count
        WalBytes = ($wals | Measure-Object -Property Length -Sum).Sum
    }
}

function Measure-Server {
    param(
        [string]$Name,
        [string]$BaseUrl,
        [string]$Database,
        [int]$Points,
        [int]$BatchSize,
        [int]$Concurrency,
        [string]$Epoch = '',
        [int]$QueryIterations = 5
    )

    $handler = [System.Net.Http.HttpClientHandler]::new()
    $client = [System.Net.Http.HttpClient]::new($handler)
    $client.Timeout = [TimeSpan]::FromMinutes(5)
    try {
        Wait-HttpReady -Client $client -Url "$BaseUrl/ping"

        Invoke-Query -Client $client -BaseUrl $BaseUrl -Database $Database -Query "CREATE DATABASE $Database" | Out-Null

        # Warm parser/engine/client paths in a separate database; no warmup points enter the result.
        $warmupDb = $Database + '_warmup'
        Invoke-Query -Client $client -BaseUrl $BaseUrl -Database $warmupDb -Query "CREATE DATABASE $warmupDb" | Out-Null
        $warmPayload = [System.Text.Encoding]::UTF8.GetBytes((New-LineProtocolBatch -StartIndex 0 -EndIndex $BatchSize))
        $warmPayloads = [byte[][]]::new(5)
        $warmCounts = [int[]]::new(5)
        for ($i = 0; $i -lt 5; $i++) { $warmPayloads[$i] = $warmPayload; $warmCounts[$i] = $BatchSize }
        $warmup = [HttpWriteBenchmark]::RunAsync($client, "$BaseUrl/write?db=$warmupDb&precision=ns", $warmPayloads, $warmCounts, 1).GetAwaiter().GetResult()
        if ($warmup.FailedRequests -gt 0) { throw "Warmup failed: $($warmup.Errors -join '; ')" }
        Invoke-Query -Client $client -BaseUrl $BaseUrl -Database $warmupDb -Query "DROP DATABASE $warmupDb" | Out-Null

        $writeBatchCount = [int][Math]::Ceiling($Points / [double]$BatchSize)
        $payloads = [byte[][]]::new($writeBatchCount)
        $pointCounts = [int[]]::new($writeBatchCount)
        for ($batch = 0; $batch -lt $writeBatchCount; $batch++) {
            $start = $batch * $BatchSize
            $end = [Math]::Min($Points, $start + $BatchSize)
            $payloads[$batch] = [System.Text.Encoding]::UTF8.GetBytes((New-LineProtocolBatch -StartIndex $start -EndIndex $end))
            $pointCounts[$batch] = $end - $start
        }

        $phasesBefore = $null
        if ($Name -eq 'MiniInflux' -and $WriteDiagnostics) {
            $phasesBefore = $client.GetStringAsync("$BaseUrl/debug/stats").GetAwaiter().GetResult() | ConvertFrom-Json
        }
        $writeResult = [HttpWriteBenchmark]::RunAsync($client, "$BaseUrl/write?db=$Database&precision=ns", $payloads, $pointCounts, $Concurrency).GetAwaiter().GetResult()
        [Console]::Error.WriteLine("[$Name] write completed in $([Math]::Round($writeResult.Seconds, 3))s, peak concurrency $($writeResult.PeakConcurrency), failures $($writeResult.FailedRequests)")

        $flushMs = $null
        if ($Name -eq 'MiniInflux') {
            $flushWatch = [System.Diagnostics.Stopwatch]::StartNew()
            $flushContent = [System.Net.Http.StringContent]::new('', [System.Text.Encoding]::UTF8, 'application/json')
            try {
                $flushResponse = $client.PostAsync("$BaseUrl/admin/api/maintenance/flush", $flushContent).GetAwaiter().GetResult()
                try {
                    $flushBody = $flushResponse.Content.ReadAsStringAsync().GetAwaiter().GetResult()
                    if (-not $flushResponse.IsSuccessStatusCode) { throw "MiniInflux flush failed: $($flushResponse.StatusCode) $flushBody" }
                }
                finally { $flushResponse.Dispose() }
            }
            finally { $flushContent.Dispose() }
            $flushWatch.Stop()
            $flushMs = [Math]::Round($flushWatch.Elapsed.TotalMilliseconds, 2)
            [Console]::Error.WriteLine("[MiniInflux] flush completed in ${flushMs}ms")
        }

        $phaseMilliseconds = @{}
        $phaseCounts = @{}
        if ($null -ne $phasesBefore) {
            $phasesAfter = $client.GetStringAsync("$BaseUrl/debug/stats").GetAwaiter().GetResult() | ConvertFrom-Json
            if (-not $phasesAfter.WriteDiagnosticsEnabled) { throw 'Server write diagnostics are not enabled' }
            foreach ($property in $phasesAfter.WritePhaseMilliseconds.PSObject.Properties) {
                $phaseMilliseconds[$property.Name] = $property.Value - $phasesBefore.WritePhaseMilliseconds.($property.Name)
                $phaseCounts[$property.Name] = $phasesAfter.WritePhaseCounts.($property.Name) - $phasesBefore.WritePhaseCounts.($property.Name)
            }
        }
        $aggregateQuery = "SELECT mean(value),count(value) FROM cpu WHERE host='server00' AND region='cn'"
        $rawLimitQuery = "SELECT * FROM cpu WHERE host='server00' AND region='cn' ORDER BY time DESC LIMIT 1000"

        Invoke-Query -Client $client -BaseUrl $BaseUrl -Database $Database -Query $aggregateQuery -Epoch $Epoch | Out-Null
        Invoke-Query -Client $client -BaseUrl $BaseUrl -Database $Database -Query $rawLimitQuery -Epoch $Epoch | Out-Null

        $query1 = Measure-Query -Client $client -BaseUrl $BaseUrl -Database $Database -Query $aggregateQuery -Epoch $Epoch -Iterations $QueryIterations
        $query2 = Measure-Query -Client $client -BaseUrl $BaseUrl -Database $Database -Query $rawLimitQuery -Epoch $Epoch -Iterations $QueryIterations
        [Console]::Error.WriteLine("[$Name] queries completed")
        $query1Body = $query1.Body
        $query2Body = $query2.Body

        $countResult = (Invoke-Query -Client $client -BaseUrl $BaseUrl -Database $Database -Query 'SELECT count(value) FROM cpu') | ConvertFrom-Json
        $storedPoints = 0L
        foreach ($series in $countResult.results[0].series) {
            foreach ($row in $series.values) { $storedPoints += [long]$row[1] }
        }

        $query1Report = $null
        $query2Report = $null
        $debugQuery = $Name -eq 'MiniInflux'
        if ($debugQuery) {
            $query1DebugBody = Invoke-Query -Client $client -BaseUrl $BaseUrl -Database $Database -Query $aggregateQuery -Debug $true
            $query2DebugBody = Invoke-Query -Client $client -BaseUrl $BaseUrl -Database $Database -Query $rawLimitQuery -Debug $true
            $query1Parsed = $query1DebugBody | ConvertFrom-Json
            $query2Parsed = $query2DebugBody | ConvertFrom-Json
            $query1Report = $query1Parsed.report
            $query2Report = $query2Parsed.report
        }

        return [pscustomobject]@{
            Name = $Name
            Points = $Points
            BatchSize = $BatchSize
            Concurrency = $Concurrency
            WritePhaseMilliseconds = $phaseMilliseconds
            WritePhaseCounts = $phaseCounts
            PeakConcurrency = $writeResult.PeakConcurrency
            SuccessfulPoints = $writeResult.SuccessfulPoints
            StoredPoints = $storedPoints
            Valid = ($writeResult.FailedRequests -eq 0 -and $storedPoints -eq $Points)
            SuccessfulRequests = $writeResult.SuccessfulRequests
            FailedRequests = $writeResult.FailedRequests
            WriteErrors = $writeResult.Errors
            WriteLatencyP50Ms = $writeResult.P50Ms
            WriteLatencyP95Ms = $writeResult.P95Ms
            WriteLatencyP99Ms = $writeResult.P99Ms
            WriteLatencySamplesMs = $writeResult.SamplesMs
            WriteSeconds = [Math]::Round($writeResult.Seconds, 3)
            WriteThroughput = [Math]::Round($writeResult.SuccessfulPoints / [Math]::Max($writeResult.Seconds, 0.001), 2)
            WriteAndFlushThroughput = if ($null -ne $flushMs) { [Math]::Round($writeResult.SuccessfulPoints / [Math]::Max($writeResult.Seconds + $flushMs / 1000, 0.001), 2) } else { $null }
            FlushAfterWriteMs = $flushMs
            AggregateQueryMs = $query1.MedianMs
            RawLimitQueryMs = $query2.MedianMs
            AggregateQuerySamplesMs = $query1.SamplesMs
            RawLimitQuerySamplesMs = $query2.SamplesMs
            AggregateQueryBytes = ($query1Body | Measure-Object -Character).Characters
            RawLimitQueryBytes = ($query2Body | Measure-Object -Character).Characters
            AggregateQueryReport = $query1Report
            RawLimitQueryReport = $query2Report
        }
    }
    finally { $client.Dispose(); $handler.Dispose() }
}

$root = Split-Path -Parent $PSScriptRoot
$ports = if ($MiniOnly) { @(18086) } else { @(18086, 18087) }
foreach ($listener in [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()) {
    if ($listener.Port -in $ports) { throw "Benchmark port $($listener.Port) is already in use; no existing service will be used or stopped." }
}
$artifactsRoot = Join-Path $root '.benchmarks'
$runRoot = Join-Path $artifactsRoot ('write-' + [Guid]::NewGuid().ToString('N'))
$miniData = Join-Path $runRoot 'miniinflux-data'
$influxRoot = Join-Path $runRoot 'influxdb179'
$influxMeta = Join-Path $influxRoot 'meta'
$influxData = Join-Path $influxRoot 'data'
$influxWal = Join-Path $influxRoot 'wal'
$influxConfig = Join-Path $influxRoot 'influxdb.conf'

New-Item -ItemType Directory -Force -Path $artifactsRoot, $miniData, $influxRoot, $influxMeta, $influxData, $influxWal | Out-Null

New-BasicConfig -Path $influxConfig -BindAddress '127.0.0.1:18087' -MetaDir $influxMeta -DataDir $influxData -WalDir $influxWal

$miniProc = $null
$influxProc = $null
$savedEnvironment = @{}
foreach ($name in @('Auth__Enabled', 'ASPNETCORE_ENVIRONMENT', 'Urls', 'Data__Dir', 'Http__BindAddress',
    'Data__QueryLogEnabled', 'Http__LogEnabled', 'Http__SuppressWriteLog', 'Logging__ConsoleEnabled',
    'Logging__FileEnabled', 'MiniInflux__FlushThreshold', 'MiniInflux__WriteDiagnostics', 'DOTNET_CLI_HOME', 'DOTNET_SKIP_FIRST_TIME_EXPERIENCE')) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

try {
    $env:Auth__Enabled = 'false'
    $env:ASPNETCORE_ENVIRONMENT = 'Development'
    $env:Urls = 'http://127.0.0.1:18086'
    $env:Data__Dir = $miniData
    $env:Http__BindAddress = '127.0.0.1:18086'
    $env:Data__QueryLogEnabled = 'false'
    $env:Http__LogEnabled = 'false'
    $env:Http__SuppressWriteLog = 'true'
    $env:Logging__ConsoleEnabled = 'false'
    $env:Logging__FileEnabled = 'false'
    $env:MiniInflux__WriteDiagnostics = $WriteDiagnostics.IsPresent.ToString()
    $miniFlushThreshold = if ($BufferOnly) { [Math]::Max($Points * 2, 50000) } else { [Math]::Max(1, [Math]::Min(50000, [int][Math]::Floor($Points / 2.0))) }
    $env:MiniInflux__FlushThreshold = $miniFlushThreshold.ToString([System.Globalization.CultureInfo]::InvariantCulture)
    $env:DOTNET_CLI_HOME = (Join-Path $root '.dotnet_home')
    $env:DOTNET_SKIP_FIRST_TIME_EXPERIENCE = '1'

    if ([string]::IsNullOrWhiteSpace($MiniBinaryPath)) {
        dotnet build (Join-Path $root 'MiniInflux/MiniInflux.csproj') -c Release --no-restore -nologo | Out-Host
        if ($LASTEXITCODE -ne 0) { throw 'MiniInflux build failed' }
        $MiniBinaryPath = Join-Path $root 'MiniInflux/bin/Release/net10.0/MiniInflux.dll'
    }
    $binary = (Resolve-Path -LiteralPath $MiniBinaryPath).Path
    # Start the actual server, not dotnet run's parent process, so cleanup stops the right PID.
    $miniProc = Start-Process -FilePath 'dotnet' -ArgumentList @('"' + $binary + '"') -WorkingDirectory $root -WindowStyle Hidden -PassThru
    if (-not $MiniOnly) {
        $influxProc = Start-Process -FilePath 'D:\workingfold\Influxdb\influxdb-1.7.9\influxd.exe' -ArgumentList @('run', '-config', $influxConfig) -WorkingDirectory $root -WindowStyle Hidden -PassThru
    }

    $miniResult = Measure-Server -Name 'MiniInflux' -BaseUrl 'http://127.0.0.1:18086' -Database 'benchmini' -Points $Points -BatchSize $BatchSize -Concurrency $Concurrency -Epoch $Epoch -QueryIterations $QueryIterations
    $results = @($miniResult)
    if (-not $MiniOnly) {
        $results += Measure-Server -Name 'InfluxDB 1.7.9' -BaseUrl 'http://127.0.0.1:18087' -Database 'benchinflux' -Points $Points -BatchSize $BatchSize -Concurrency $Concurrency -Epoch $Epoch -QueryIterations $QueryIterations
    }
    $miniStorage = Get-StorageStats -DataPath $miniData

    $report = [pscustomobject]@{
        TimestampUtc = [DateTime]::UtcNow.ToString('o')
        Host = $env:COMPUTERNAME
        Points = $Points
        BatchSize = $BatchSize
        Concurrency = $Concurrency
        MiniInfluxBufferOnly = [bool]$BufferOnly
        MiniInfluxFlushThreshold = $miniFlushThreshold
        MiniInfluxStorage = $miniStorage
        MiniBinaryPath = $binary
        Results = $results
    }
    $json = $report | ConvertTo-Json -Depth 6
    $json | Set-Content -LiteralPath (Join-Path $runRoot 'result.json')
    $json
}
finally {
    foreach ($name in $savedEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process')
    }
    if ($miniProc -and -not $miniProc.HasExited) {
        Stop-Process -Id $miniProc.Id -Force
        $miniProc.WaitForExit()
    }

    if ($influxProc -and -not $influxProc.HasExited) {
        Stop-Process -Id $influxProc.Id -Force
        $influxProc.WaitForExit()
    }
}
