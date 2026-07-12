using System.Text.Json.Serialization;
using MiniInflux.Net10.Storage;

public sealed class AdminSessionResponse
{
    [JsonPropertyName("requiresAuthentication")]
    public bool RequiresAuthentication { get; set; }
    [JsonPropertyName("authenticated")]
    public bool Authenticated { get; set; }
    [JsonPropertyName("userName")]
    public string? UserName { get; set; }
    [JsonPropertyName("rateLimited")]
    public bool RateLimited { get; set; }
    [JsonPropertyName("retryAfterSeconds")]
    public int? RetryAfterSeconds { get; set; }
}

public sealed class AdminOverviewResponse
{
    [JsonPropertyName("dataPath")]
    public string DataPath { get; set; } = "";
    [JsonPropertyName("httpBindAddress")]
    public string HttpBindAddress { get; set; } = "";
    [JsonPropertyName("authEnabled")]
    public bool AuthEnabled { get; set; }
    [JsonPropertyName("tlsEnabled")]
    public bool TlsEnabled { get; set; }
    [JsonPropertyName("restorePending")]
    public bool RestorePending { get; set; }
    [JsonPropertyName("restorePreviousExists")]
    public bool RestorePreviousExists { get; set; }
    [JsonPropertyName("databaseCount")]
    public int DatabaseCount { get; set; }
    [JsonPropertyName("continuousQueryCount")]
    public int ContinuousQueryCount { get; set; }
    [JsonPropertyName("stats")]
    public DebugStats Stats { get; set; } = new();
}

public sealed class AdminDatabaseSummary
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";
    [JsonPropertyName("defaultRetentionPolicy")]
    public string DefaultRetentionPolicy { get; set; } = "";
    [JsonPropertyName("measurementCount")]
    public int MeasurementCount { get; set; }
    [JsonPropertyName("seriesCardinality")]
    public int SeriesCardinality { get; set; }
    [JsonPropertyName("shardCount")]
    public int ShardCount { get; set; }
    [JsonPropertyName("segmentCount")]
    public int SegmentCount { get; set; }
    [JsonPropertyName("retentionPolicies")]
    public List<AdminRetentionPolicySummary> RetentionPolicies { get; set; } = [];
}

public sealed class AdminRetentionPolicySummary
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";
    [JsonPropertyName("durationNs")]
    public long DurationNs { get; set; }
    [JsonPropertyName("isDefault")]
    public bool IsDefault { get; set; }
    [JsonPropertyName("shardCount")]
    public int ShardCount { get; set; }
    [JsonPropertyName("segmentCount")]
    public int SegmentCount { get; set; }
}

public sealed class AdminContinuousQuerySummary
{
    [JsonPropertyName("database")]
    public string Database { get; set; } = "";
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";
    [JsonPropertyName("queryText")]
    public string QueryText { get; set; } = "";
    [JsonPropertyName("everyNs")]
    public long EveryNs { get; set; }
    [JsonPropertyName("forNs")]
    public long ForNs { get; set; }
    [JsonPropertyName("recomputeRecentBuckets")]
    public int RecomputeRecentBuckets { get; set; }
    [JsonPropertyName("lastCompletedBucketStartNs")]
    public long? LastCompletedBucketStartNs { get; set; }
}

public sealed class MaintenanceResult
{
    [JsonPropertyName("message")]
    public string Message { get; set; } = "";
    [JsonPropertyName("compactionTasksMerged")]
    public int CompactionTasksMerged { get; set; }
    [JsonPropertyName("continuousQueriesExecuted")]
    public int ContinuousQueriesExecuted { get; set; }
    [JsonPropertyName("compaction")]
    public CompactionStatsSnapshot? Compaction { get; set; }
}

public sealed class BackupPathRequest
{
    [JsonPropertyName("path")]
    public string Path { get; set; } = "";
}
