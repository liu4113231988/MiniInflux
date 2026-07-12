param(
    [string]$Url = 'http://localhost:8086',
    [int]$DurationMinutes = 1440,
    [int]$BatchSize = 1000,
    [string]$Database = 'soak',
    [string]$Username = '',
    [string]$Password = ''
)

$ErrorActionPreference = 'Stop'
$headers = @{}
if ($Username) {
    $token = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("${Username}:${Password}"))
    $headers.Authorization = "Basic $token"
}

$deadline = (Get-Date).AddMinutes($DurationMinutes)
$written = 0L
$queries = 0L
$failures = 0L
while ((Get-Date) -lt $deadline) {
    $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000000
    $body = (0..($BatchSize - 1) | ForEach-Object { "cpu,host=h$($_ % 16) value=$_,seq=${written}i $($now + $_)" }) -join "`n"
    try {
        Invoke-WebRequest "$Url/write?db=$Database&precision=ns" -Method Post -Headers $headers -ContentType 'text/plain' -Body $body | Out-Null
        $written += $BatchSize
        Invoke-WebRequest "$Url/query?db=$Database&q=SELECT%20count(value)%20FROM%20cpu" -Headers $headers | Out-Null
        $queries++
    }
    catch {
        $failures++
        Write-Warning $_.Exception.Message
        Start-Sleep -Seconds 1
    }
}

[pscustomobject]@{
    EndedAtUtc = [DateTimeOffset]::UtcNow
    DurationMinutes = $DurationMinutes
    PointsWritten = $written
    Queries = $queries
    Failures = $failures
} | ConvertTo-Json
