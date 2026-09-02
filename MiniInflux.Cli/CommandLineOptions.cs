using System.Globalization;

public static class CommandLineOptions
{
    public static MiniInfluxOptions Parse(string[] args)
    {
        var dataPath = ReadString(args, "--data") ?? "./data";
        return new MiniInfluxOptions
        {
            DataPath = dataPath,
            Data = new DataOptions { Dir = dataPath },
            FlushThreshold = ReadInt(args, "--flush-threshold", 50_000),
            Wal = new WalOptions
            {
                Fsync = ReadBool(args, "--wal-fsync", true),
                FsyncIntervalMs = ReadInt(args, "--wal-fsync-interval-ms", 1_000),
                MaxWalFileBytes = ReadLong(args, "--wal-max-file-bytes", 16 * 1024 * 1024)
            },
            Storage = new StorageOptions
            {
                MaxSeriesPerDatabase = ReadLong(args, "--storage-max-series-per-database", 10_000_000),
                MaxFieldsPerMeasurement = ReadInt(args, "--storage-max-fields-per-measurement", 1_024),
                MaxBufferPoints = ReadLong(args, "--storage-max-buffer-points", 1_000_000),
                MaxBufferBytes = ReadLong(args, "--storage-max-buffer-bytes", 0)
            }
        };
    }

    private static string? ReadString(string[] args, string name)
    {
        for (var i = 0; i < args.Length; i++)
        {
            if (!string.Equals(args[i], name, StringComparison.OrdinalIgnoreCase))
                continue;

            if (i + 1 >= args.Length || string.IsNullOrWhiteSpace(args[i + 1]) || args[i + 1].StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"missing value for {name}");

            return args[i + 1];
        }

        return null;
    }

    private static int ReadInt(string[] args, string name, int fallback)
    {
        var value = ReadString(args, name);
        return value is null ? fallback : int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result)
            ? result
            : throw new ArgumentException($"invalid integer for {name}: {value}");
    }

    private static long ReadLong(string[] args, string name, long fallback)
    {
        var value = ReadString(args, name);
        return value is null ? fallback : long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result)
            ? result
            : throw new ArgumentException($"invalid integer for {name}: {value}");
    }

    private static bool ReadBool(string[] args, string name, bool fallback)
    {
        var value = ReadString(args, name);
        return value is null ? fallback : bool.TryParse(value, out var result)
            ? result
            : throw new ArgumentException($"invalid boolean for {name}: {value}; expected true or false");
    }
}
