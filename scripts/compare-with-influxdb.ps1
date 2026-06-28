param(
    [int]$Points = 200000,
    [int]$BatchSize = 5000,
    [int]$Concurrency = 1
)

$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Net.Http

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
            if ($response.IsSuccessStatusCode) {
                return
            }
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
        [bool]$Debug = $false
    )

    $debugParam = if ($Debug) { '&debug=true' } else { '' }
    $queryUrl = "$BaseUrl/query?db=$Database&q=$([Uri]::EscapeDataString($Query))$debugParam"
    $response = $Client.PostAsync($queryUrl, [System.Net.Http.StringContent]::new('', [System.Text.Encoding]::UTF8, 'application/x-www-form-urlencoded')).GetAwaiter().GetResult()
    $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
    if (-not $response.IsSuccessStatusCode) {
        throw "Query failed: $($response.StatusCode) $body"
    }

    return $body
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

function Measure-Server {
    param(
        [string]$Name,
        [string]$BaseUrl,
        [string]$Database,
        [int]$Points,
        [int]$BatchSize,
        [int]$Concurrency
    )

    $handler = [System.Net.Http.HttpClientHandler]::new()
    $client = [System.Net.Http.HttpClient]::new($handler)
    $client.Timeout = [TimeSpan]::FromMinutes(5)

    Wait-HttpReady -Client $client -Url "$BaseUrl/ping"

    Invoke-Query -Client $client -BaseUrl $BaseUrl -Database $Database -Query "CREATE DATABASE $Database" | Out-Null

    $writeBatchCount = [int][Math]::Ceiling($Points / [double]$BatchSize)
    $writeChunks = for ($batch = 0; $batch -lt $writeBatchCount; $batch++) {
        $start = $batch * $BatchSize
        $end = [Math]::Min($Points, $start + $BatchSize)
        @{
            Index = $batch
            Payload = New-LineProtocolBatch -StartIndex $start -EndIndex $end
        }
    }

    $writeWatch = [System.Diagnostics.Stopwatch]::StartNew()
    foreach ($chunk in $writeChunks) {
        $content = [System.Net.Http.StringContent]::new($chunk.Payload, [System.Text.Encoding]::UTF8, 'text/plain')
        $response = $client.PostAsync("$BaseUrl/write?db=$Database&precision=ns", $content).GetAwaiter().GetResult()
        $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
        if (-not $response.IsSuccessStatusCode) {
            throw "Write batch $($chunk.Index) failed: $($response.StatusCode) $body"
        }
    }
    $writeWatch.Stop()

    $aggregateQuery = "SELECT mean(value),count(value) FROM cpu WHERE host='server00' AND region='cn'"
    $rawLimitQuery = "SELECT * FROM cpu WHERE host='server00' AND region='cn' ORDER BY time DESC LIMIT 1000"

    Invoke-Query -Client $client -BaseUrl $BaseUrl -Database $Database -Query $aggregateQuery | Out-Null
    Invoke-Query -Client $client -BaseUrl $BaseUrl -Database $Database -Query $rawLimitQuery | Out-Null

    $query1Watch = [System.Diagnostics.Stopwatch]::StartNew()
    $query1Body = Invoke-Query -Client $client -BaseUrl $BaseUrl -Database $Database -Query $aggregateQuery
    $query1Watch.Stop()

    $query2Watch = [System.Diagnostics.Stopwatch]::StartNew()
    $query2Body = Invoke-Query -Client $client -BaseUrl $BaseUrl -Database $Database -Query $rawLimitQuery
    $query2Watch.Stop()

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
        WriteSeconds = [Math]::Round($writeWatch.Elapsed.TotalSeconds, 3)
        WriteThroughput = [Math]::Round($Points / [Math]::Max($writeWatch.Elapsed.TotalSeconds, 0.001), 2)
        AggregateQueryMs = [Math]::Round($query1Watch.Elapsed.TotalMilliseconds, 2)
        RawLimitQueryMs = [Math]::Round($query2Watch.Elapsed.TotalMilliseconds, 2)
        AggregateQueryBytes = ($query1Body | Measure-Object -Character).Characters
        RawLimitQueryBytes = ($query2Body | Measure-Object -Character).Characters
        AggregateQueryReport = $query1Report
        RawLimitQueryReport = $query2Report
    }
}

$root = Split-Path -Parent $PSScriptRoot
$artifactsRoot = Join-Path $root '.benchmarks'
$miniData = Join-Path $artifactsRoot 'miniinflux-data'
$influxRoot = Join-Path $artifactsRoot 'influxdb179'
$influxMeta = Join-Path $influxRoot 'meta'
$influxData = Join-Path $influxRoot 'data'
$influxWal = Join-Path $influxRoot 'wal'
$influxConfig = Join-Path $influxRoot 'influxdb.conf'

Remove-Item -LiteralPath $miniData -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $influxRoot -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $artifactsRoot, $miniData, $influxRoot, $influxMeta, $influxData, $influxWal | Out-Null

New-BasicConfig -Path $influxConfig -BindAddress '127.0.0.1:18087' -MetaDir $influxMeta -DataDir $influxData -WalDir $influxWal

$miniProc = $null
$influxProc = $null

try {
    $env:Auth__Enabled = 'false'
    $env:Urls = 'http://127.0.0.1:18086'
    $env:Data__Dir = $miniData
    $env:Http__BindAddress = '127.0.0.1:18086'
    $env:Data__QueryLogEnabled = 'false'
    $env:Http__LogEnabled = 'false'
    $env:Http__SuppressWriteLog = 'true'
    $env:Logging__ConsoleEnabled = 'false'
    $env:Logging__FileEnabled = 'false'
    $env:MiniInflux__FlushThreshold = [Math]::Max($Points * 2, 50000).ToString([System.Globalization.CultureInfo]::InvariantCulture)
    $env:DOTNET_CLI_HOME = (Join-Path $root '.dotnet_home')
    $env:DOTNET_SKIP_FIRST_TIME_EXPERIENCE = '1'

    $miniProc = Start-Process -FilePath 'dotnet' -ArgumentList @('run', '-c', 'Release', '--project', 'MiniInflux.Net10.csproj', '--no-launch-profile', '--no-restore') -WorkingDirectory $root -WindowStyle Hidden -PassThru
    $influxProc = Start-Process -FilePath 'D:\workingfold\Influxdb\influxdb-1.7.9\influxd.exe' -ArgumentList @('run', '-config', $influxConfig) -WorkingDirectory $root -WindowStyle Hidden -PassThru

    $miniResult = Measure-Server -Name 'MiniInflux' -BaseUrl 'http://127.0.0.1:18086' -Database 'benchmini' -Points $Points -BatchSize $BatchSize -Concurrency $Concurrency
    $influxResult = Measure-Server -Name 'InfluxDB 1.7.9' -BaseUrl 'http://127.0.0.1:18087' -Database 'benchinflux' -Points $Points -BatchSize $BatchSize -Concurrency $Concurrency

    [pscustomobject]@{
        TimestampUtc = [DateTime]::UtcNow.ToString('o')
        Host = $env:COMPUTERNAME
        Points = $Points
        BatchSize = $BatchSize
        Concurrency = $Concurrency
        Results = @($miniResult, $influxResult)
    } | ConvertTo-Json -Depth 6
}
finally {
    if ($miniProc -and -not $miniProc.HasExited) {
        Stop-Process -Id $miniProc.Id -Force
    }

    if ($influxProc -and -not $influxProc.HasExited) {
        Stop-Process -Id $influxProc.Id -Force
    }
}
