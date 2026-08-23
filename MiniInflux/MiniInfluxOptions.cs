using Microsoft.Extensions.Configuration;

public sealed class MiniInfluxOptions
{
    public string DataPath { get; init; } = "./data";
    public int FlushThreshold { get; init; } = 50_000;
    public string Urls { get; init; } = "http://0.0.0.0:8086";
    public DataOptions Data { get; init; } = new();
    public HttpOptions Http { get; init; } = new();
    public LoggingOptions Logging { get; init; } = new();
    public ContinuousQueryOptions ContinuousQuery { get; init; } = new();
    public WalOptions Wal { get; init; } = new();
    public WriteOptions Write { get; init; } = new();
    public StorageOptions Storage { get; init; } = new();
    public AuthOptions Auth { get; init; } = new();
    public TlsOptions Tls { get; init; } = new();

    public static MiniInfluxOptions Load(IConfiguration config)
    {
        var dataDir = ReadString(config, "Data:Dir")
            ?? config["MiniInflux:DataPath"]
            ?? Environment.GetEnvironmentVariable("MINI_INFLUX_DATA")
            ?? "./data";

        var httpEnabled = ReadBool(config, true, "Http:Enabled");
        var authEnabled = ReadBool(config, false, "Auth:Enabled", "Http:AuthEnabled");
        var bindAddress = ReadString(config, "Http:BindAddress");
        var urls = !string.IsNullOrWhiteSpace(bindAddress)
            ? BuildUrlFromBindAddress(bindAddress!)
            : config["Urls"] ?? "http://0.0.0.0:8086";

        return new MiniInfluxOptions
        {
            DataPath = dataDir,
            FlushThreshold = ReadInt(config, 50_000, "MiniInflux:FlushThreshold"),
            Urls = urls,
            Data = new DataOptions
            {
                Dir = dataDir,
                BackupDir = ReadString(config, "Data:BackupDir", "")!,
                QueryLogEnabled = ReadBool(config, true, "Data:QueryLogEnabled")
            },
            Http = new HttpOptions
            {
                Enabled = httpEnabled,
                BindAddress = bindAddress ?? "0.0.0.0:8086",
                AuthEnabled = authEnabled,
                LogEnabled = ReadBool(config, true, "Http:LogEnabled"),
                SuppressWriteLog = ReadBool(config, false, "Http:SuppressWriteLog"),
                AccessLogPath = ReadString(config, "Http:AccessLogPath"),
                AccessLogStatusFilters = ReadStringList(config, "Http:AccessLogStatusFilters"),
                WriteTracing = ReadBool(config, false, "Http:WriteTracing")
            },
            Logging = new LoggingOptions
            {
                Level = ReadString(config, "Logging:Level", "Information")!,
                SystemLevel = ReadString(config, "Logging:SystemLevel", "Warning")!,
                ConsoleEnabled = ReadBool(config, true, "Logging:ConsoleEnabled"),
                FileEnabled = ReadBool(config, false, "Logging:FileEnabled"),
                FilePath = ReadString(config, "Logging:FilePath", "./logs/miniinflux.log")!,
                FileMaxBytes = Math.Max(0, ReadLong(config, 10 * 1024 * 1024, "Logging:FileMaxBytes")),
                FileRetainedFileCount = Math.Max(1, ReadInt(config, 5, "Logging:FileRetainedFileCount"))
            },
            ContinuousQuery = new ContinuousQueryOptions
            {
                Enabled = ReadBool(config, true, "ContinuousQuery:Enabled"),
                CheckIntervalMs = ReadInt(config, 5000, "ContinuousQuery:CheckIntervalMs"),
                MaxCatchUpRunsPerCycle = ReadInt(config, 8, "ContinuousQuery:MaxCatchUpRunsPerCycle"),
                RecomputeRecentBuckets = ReadInt(config, 0, "ContinuousQuery:RecomputeRecentBuckets"),
                InitialBackfillDuration = ReadString(config, "ContinuousQuery:InitialBackfillDuration", "0s")!,
                InitialBackfillDurationNs = ParseDurationOrZero(ReadString(config, "ContinuousQuery:InitialBackfillDuration", "0s"))
            },
            Wal = new WalOptions
            {
                Fsync = ReadBool(config, true, "Wal:Fsync"),
                FsyncIntervalMs = ReadInt(config, 1000, "Wal:FsyncIntervalMs"),
                MaxWalFileBytes = ReadLong(config, 16 * 1024 * 1024, "Wal:MaxWalFileBytes")
            },
            Write = new WriteOptions
            {
                QueueCapacity = ReadInt(config, 100_000, "Write:QueueCapacity"),
                BatchSize = ReadInt(config, 10_000, "Write:BatchSize"),
                MaxRequestBodyBytes = ReadLong(config, 26_214_400, "Write:MaxRequestBodyBytes")
            },
            Storage = new StorageOptions
            {
                RpCheckIntervalMs = ReadInt(config, 60_000, "Storage:RpCheckIntervalMs"),
                MaxSeriesPerDatabase = ReadLong(config, 10_000_000, "Storage:MaxSeriesPerDatabase"),
                MaxFieldsPerMeasurement = ReadInt(config, 1024, "Storage:MaxFieldsPerMeasurement"),
                MaxResponseRows = ReadInt(config, 100_000, "Storage:MaxResponseRows"),
                MaxQueryPoints = ReadInt(config, 1_000_000, "Storage:MaxQueryPoints"),
                MaxBufferPoints = ReadLong(config, 1_000_000, "Storage:MaxBufferPoints"),
                MaxQueryDurationMs = ReadInt(config, 10_000, "Storage:MaxQueryDurationMs"),
                MaxConcurrentQueries = ReadInt(config, 0, "Storage:MaxConcurrentQueries"),
                MaxBufferBytes = ReadLong(config, 0, "Storage:MaxBufferBytes"),
                MaxQueryMemoryBytes = ReadLong(config, 512L * 1024 * 1024, "Storage:MaxQueryMemoryBytes"),
                MinFreeDiskBytes = ReadLong(config, 1L * 1024 * 1024 * 1024, "Storage:MinFreeDiskBytes"),
                FlushColdDurationMs = ReadInt(config, 600_000, "Storage:FlushColdDurationMs"),
                CompactionTargetBytes = ReadLong(config, 512L * 1024 * 1024, "Storage:CompactionTargetBytes"),
                MaxSegmentFileBytes = ReadLong(config, 512L * 1024 * 1024, "Storage:MaxSegmentFileBytes"),
            MinSegmentFileBytes = ReadLong(config, 0L, "Storage:MinSegmentFileBytes"),
            MinSegmentFillRatio = ReadDouble(config, 0.5, "Storage:MinSegmentFillRatio"),
            CompactionMaxWriteBytesPerSecond = ReadLong(config, 0L, "Storage:CompactionMaxWriteBytesPerSecond")
            },
            Auth = new AuthOptions
            {
                Enabled = authEnabled,
                Username = ReadString(config, "Auth:Username", "admin")!,
                Password = ReadString(config, "Auth:Password", "")!,
                AuditFailures = ReadBool(config, true, "Auth:AuditFailures"),
                AllowQueryCredentials = ReadBool(config, false, "Auth:AllowQueryCredentials"),
                TrustedProxyAddresses = ReadStringList(config, "Auth:TrustedProxyAddresses"),
                MaxFailedAttempts = Math.Max(0, ReadInt(config, 5, "Auth:MaxFailedAttempts")),
                FailureWindowMs = Math.Max(1000, ReadInt(config, 60_000, "Auth:FailureWindowMs")),
                LockoutMs = Math.Max(0, ReadInt(config, 300_000, "Auth:LockoutMs"))
            },
            Tls = new TlsOptions
            {
                Enabled = ReadBool(config, false, "Tls:Enabled"),
                Port = ReadInt(config, 8087, "Tls:Port"),
                CertPath = config["Tls:CertPath"],
                Password = config["Tls:Password"]
            }
        };
    }

    private static string? ReadString(IConfiguration config, params string[] keys)
    {
        foreach (var key in keys)
            if (!string.IsNullOrWhiteSpace(key) && !string.IsNullOrWhiteSpace(config[key]))
                return config[key];
        return null;
    }

    private static string? ReadString(IConfiguration config, string key, string? fallback)
    {
        var value = config[key];
        return string.IsNullOrWhiteSpace(value) ? fallback : value;
    }

    private static int ReadInt(IConfiguration config, int fallback, params string[] keys)
    {
        foreach (var key in keys)
            if (int.TryParse(config[key], out var value))
                return value;
        return fallback;
    }

    private static long ReadLong(IConfiguration config, long fallback, params string[] keys)
    {
        foreach (var key in keys)
            if (long.TryParse(config[key], out var value))
                return value;
        return fallback;
    }

    private static double ReadDouble(IConfiguration config, double fallback, params string[] keys)
    {
        foreach (var key in keys)
            if (double.TryParse(config[key], out var value) && value is > 0 and <= 1)
                return value;
        return fallback;
    }

    private static bool ReadBool(IConfiguration config, bool fallback, params string[] keys)
    {
        foreach (var key in keys)
            if (bool.TryParse(config[key], out var value))
                return value;
        return fallback;
    }

    private static List<string> ReadStringList(IConfiguration config, string key)
    {
        var section = config.GetSection(key);
        var values = section.GetChildren()
            .Select(child => child.Value)
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Cast<string>()
            .ToList();

        if (values.Count > 0)
            return values;

        var scalar = config[key];
        if (string.IsNullOrWhiteSpace(scalar))
            return [];

        return scalar
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .ToList();
    }

    private static string BuildUrlFromBindAddress(string bindAddress)
    {
        var trimmed = bindAddress.Trim();
        if (trimmed.StartsWith("http://", StringComparison.OrdinalIgnoreCase) || trimmed.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            return trimmed;

        if (trimmed.StartsWith(':'))
            return $"http://0.0.0.0{trimmed}";

        if (!trimmed.Contains(':'))
            return $"http://{trimmed}:8086";

        return $"http://{trimmed}";
    }

    private static long ParseDurationOrZero(string? value)
    {
        if (string.IsNullOrWhiteSpace(value) || value == "0" || value == "0s")
            return 0;

        return MiniInflux.Net10.Protocol.InfluxQlParser.DurationToNs(value);
    }
}

public sealed class DataOptions
{
    public string Dir { get; init; } = "./data";
    public string BackupDir { get; init; } = "";
    public bool QueryLogEnabled { get; init; } = true;
}

public sealed class HttpOptions
{
    public bool Enabled { get; init; } = true;
    public string BindAddress { get; init; } = "0.0.0.0:8086";
    public bool AuthEnabled { get; init; }
    public bool LogEnabled { get; init; } = true;
    public bool SuppressWriteLog { get; init; }
    public string? AccessLogPath { get; init; }
    public List<string> AccessLogStatusFilters { get; init; } = [];
    public bool WriteTracing { get; init; }
}

public sealed class LoggingOptions
{
    /// <summary>
    /// Application log level (for MiniInflux.* loggers).
    /// </summary>
    public string Level { get; init; } = "Information";

    /// <summary>
    /// System log level (for Microsoft.*, System.*, etc. loggers).
    /// Defaults to Warning to reduce noise from framework components.
    /// </summary>
    public string SystemLevel { get; init; } = "Warning";

    public bool ConsoleEnabled { get; init; } = true;
    public bool FileEnabled { get; init; }
    public string FilePath { get; init; } = "./logs/miniinflux.log";
    public long FileMaxBytes { get; init; } = 10 * 1024 * 1024;
    public int FileRetainedFileCount { get; init; } = 5;
}

public sealed class ContinuousQueryOptions
{
    public bool Enabled { get; init; } = true;
    public int CheckIntervalMs { get; init; } = 5000;
    public int MaxCatchUpRunsPerCycle { get; init; } = 8;
    public int RecomputeRecentBuckets { get; init; }
    public string InitialBackfillDuration { get; init; } = "0s";
    public long InitialBackfillDurationNs { get; init; }
}

public sealed class WalOptions
{
    public bool Fsync { get; init; } = true;
    public int FsyncIntervalMs { get; init; } = 1000;
    public long MaxWalFileBytes { get; init; } = 16 * 1024 * 1024;
}

public sealed class WriteOptions
{
    public int QueueCapacity { get; init; } = 100_000;
    public int BatchSize { get; init; } = 10_000;
    public long MaxRequestBodyBytes { get; init; } = 26_214_400;
}

public sealed class StorageOptions
{
    public int RpCheckIntervalMs { get; init; } = 60_000;
    public long MaxSeriesPerDatabase { get; init; } = 10_000_000;
    public int MaxFieldsPerMeasurement { get; init; } = 1024;
    public int MaxResponseRows { get; init; } = 100_000;
    public int MaxQueryPoints { get; init; } = 1_000_000;
    public long MaxBufferPoints { get; init; } = 1_000_000;
    public int MaxQueryDurationMs { get; init; } = 10_000;
    public int MaxConcurrentQueries { get; init; } = 0;
    public long MaxBufferBytes { get; init; }
    public long MaxQueryMemoryBytes { get; init; } = 512L * 1024 * 1024;
    public long MinFreeDiskBytes { get; init; } = 1L * 1024 * 1024 * 1024;
    public int FlushColdDurationMs { get; init; } = 600_000;
    public long CompactionTargetBytes { get; init; } = 512L * 1024 * 1024;
    /// <summary>Hard cap on a single flushed <c>.seg</c> file's estimated size. FlushLocked splits a
    /// shard's pending points into chunks of at most this many bytes so segment files never exceed
    /// the configured limit; smaller flushes land in one file (compacted later).</summary>
    public long MaxSegmentFileBytes { get; init; } = 512L * 1024 * 1024;
    /// <summary>Below this estimated buffer size a flush is deferred so cold/low-volume shards don't
    /// scatter many tiny <c>.seg</c> files. Data is merged into larger files as it accumulates. 0
    /// disables the floor (flush on every trigger). A shard that stays cold past twice
    /// <see cref="FlushColdDurationMs"/> is still force-flushed regardless of this floor.</summary>
    public long MinSegmentFileBytes { get; init; } = 0L;
    /// <summary>Tail-merge ratio in (0,1]. When the remaining unwritten data drops below
    /// <c>MaxSegmentFileBytes * MinSegmentFillRatio</c>, it is packed into the current file instead
    /// of being split off into a tiny trailing <c>.seg</c>. This caps the file count on HDD/network
    /// storage where "file count x random IO" dominates. Higher = fewer, larger files (more memory
    /// per flush); lower = closer to the strict hard cap. 1 reverts to the old strict split-at-cap
    /// behavior. Files are intentionally <em>not</em> exact-size aligned.</summary>
    public double MinSegmentFillRatio { get; init; } = 0.5;
    /// <summary>Optional I/O budget for background compaction, in bytes per second. When non-zero,
    /// compaction output writes are throttled to roughly this rate so a large merge cannot saturate
    /// disk and degrade foreground read/write latency. 0 (default) disables throttling.</summary>
    public long CompactionMaxWriteBytesPerSecond { get; init; } = 0L;
}

public sealed class AuthOptions
{
    public bool Enabled { get; set; }
    public string Username { get; init; } = "admin";
    public string Password { get; init; } = "";
    public bool AuditFailures { get; init; } = true;
    public bool AllowQueryCredentials { get; init; }
    public List<string> TrustedProxyAddresses { get; init; } = [];
    public int MaxFailedAttempts { get; init; } = 5;
    public int FailureWindowMs { get; init; } = 60_000;
    public int LockoutMs { get; init; } = 300_000;
}

public sealed class TlsOptions
{
    public bool Enabled { get; set; }
    public int Port { get; init; } = 8087;
    public string? CertPath { get; init; }
    public string? Password { get; init; }
}
