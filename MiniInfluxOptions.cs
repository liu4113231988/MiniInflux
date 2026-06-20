public sealed class MiniInfluxOptions
{
    public string DataPath { get; init; } = "./data";
    public int FlushThreshold { get; init; } = 50_000;
    public string Urls { get; init; } = "http://0.0.0.0:8086";
    public WalOptions Wal { get; init; } = new();
    public WriteOptions Write { get; init; } = new();
    public StorageOptions Storage { get; init; } = new();
    public AuthOptions Auth { get; init; } = new();
    public TlsOptions Tls { get; init; } = new();

    public static MiniInfluxOptions Load(IConfiguration config)
    {
        var authUsers = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var child in config.GetSection("Auth:Users").GetChildren())
            if (!string.IsNullOrEmpty(child.Key) && child.Value != null) authUsers[child.Key] = child.Value;

        return new MiniInfluxOptions
        {
            DataPath = config["MiniInflux:DataPath"] ?? Environment.GetEnvironmentVariable("MINI_INFLUX_DATA") ?? "./data",
            FlushThreshold = ReadInt(config, "MiniInflux:FlushThreshold", 50_000),
            Urls = config["Urls"] ?? "http://0.0.0.0:8086",
            Wal = new WalOptions
            {
                Fsync = ReadBool(config, "Wal:Fsync", true),
                FsyncIntervalMs = ReadInt(config, "Wal:FsyncIntervalMs", 1000),
                MaxWalFileBytes = ReadLong(config, "Wal:MaxWalFileBytes", 16 * 1024 * 1024)
            },
            Write = new WriteOptions
            {
                QueueCapacity = ReadInt(config, "Write:QueueCapacity", 100_000),
                BatchSize = ReadInt(config, "Write:BatchSize", 10_000),
                MaxRequestBodyBytes = ReadLong(config, "Write:MaxRequestBodyBytes", 26_214_400)
            },
            Storage = new StorageOptions
            {
                RpCheckIntervalMs = ReadInt(config, "Storage:RpCheckIntervalMs", 60_000),
                MaxSeriesPerDatabase = ReadLong(config, "Storage:MaxSeriesPerDatabase", 10_000_000),
                MaxFieldsPerMeasurement = ReadInt(config, "Storage:MaxFieldsPerMeasurement", 1024),
                MaxResponseRows = ReadInt(config, "Storage:MaxResponseRows", 100_000),
                MaxQueryPoints = ReadInt(config, "Storage:MaxQueryPoints", 1_000_000),
                MaxBufferPoints = ReadLong(config, "Storage:MaxBufferPoints", 1_000_000),
                MaxQueryDurationMs = ReadInt(config, "Storage:MaxQueryDurationMs", 0),
                MaxBufferBytes = ReadLong(config, "Storage:MaxBufferBytes", 0),
                MaxQueryMemoryBytes = ReadLong(config, "Storage:MaxQueryMemoryBytes", 0)
            },
            Auth = new AuthOptions
            {
                Enabled = ReadBool(config, "Auth:Enabled", false),
                Users = authUsers
            },
            Tls = new TlsOptions
            {
                Enabled = ReadBool(config, "Tls:Enabled", false),
                Port = ReadInt(config, "Tls:Port", 8087),
                CertPath = config["Tls:CertPath"],
                Password = config["Tls:Password"]
            }
        };
    }

    private static int ReadInt(IConfiguration config, string key, int fallback) =>
        int.TryParse(config[key], out var value) ? value : fallback;

    private static long ReadLong(IConfiguration config, string key, long fallback) =>
        long.TryParse(config[key], out var value) ? value : fallback;

    private static bool ReadBool(IConfiguration config, string key, bool fallback) =>
        bool.TryParse(config[key], out var value) ? value : fallback;
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
    public int MaxQueryDurationMs { get; init; }
    public long MaxBufferBytes { get; init; }
    public long MaxQueryMemoryBytes { get; init; }
}

public sealed class AuthOptions
{
    public bool Enabled { get; init; }
    public Dictionary<string, string> Users { get; init; } = new(StringComparer.Ordinal);
}

public sealed class TlsOptions
{
    public bool Enabled { get; init; }
    public int Port { get; init; } = 8087;
    public string? CertPath { get; init; }
    public string? Password { get; init; }
}
