using System.Text;
using MiniInflux.Net10.Storage;

public static class ManagementCli
{
    public static int? TryRun(string[] args, MiniInfluxOptions options, TextWriter output, TextWriter error)
    {
        if (args.Length == 0)
            return null;

        var command = args[0].Trim().ToLowerInvariant();
        return command switch
        {
            "benchmark" => RunBenchmark(args.Skip(1).ToArray(), options, output),
            "inspect" => RunInspect(args.Skip(1).ToArray(), options, output, error),
            "repair" => RunRepair(args.Skip(1).ToArray(), options, output),
            "compact" => RunCompact(args.Skip(1).ToArray(), options, output),
            "backup" => RunBackup(args.Skip(1).ToArray(), options, output, error),
            "restore" => RunRestore(args.Skip(1).ToArray(), options, output, error),
            "help" or "--help" or "-h" => ShowHelp(output),
            _ => null
        };
    }

    private static int RunBenchmark(string[] args, MiniInfluxOptions options, TextWriter output)
    {
        var dataPath = ResolveDataPath(args, options);
        var benchmarkRoot = Path.Combine(dataPath, "benchmarks", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString());
        var benchmarkOptions = ParseBenchmarkOptions(args);
        var benchmarkFormat = ParseBenchmarkFormat(args);
        var benchmark = BenchmarkRunner.Run(benchmarkRoot, benchmarkOptions);
        output.WriteLine(benchmarkFormat switch
        {
            "json" => BenchmarkRunner.FormatJson(benchmark),
            "prometheus" => BenchmarkRunner.FormatPrometheus(benchmark),
            _ => BenchmarkRunner.FormatText(benchmark)
        });
        return 0;
    }

    private static int RunInspect(string[] args, MiniInfluxOptions options, TextWriter output, TextWriter error)
    {
        if (args.Length == 0)
        {
            error.WriteLine("missing inspect target: expected 'segment' or 'wal'");
            return 1;
        }

        return args[0].Trim().ToLowerInvariant() switch
        {
            "segment" => RunInspectSegment(args.Skip(1).ToArray(), output, error),
            "wal" => RunInspectWal(args.Skip(1).ToArray(), options, output, error),
            _ => WriteError(error, $"unsupported inspect target: {args[0]}")
        };
    }

    private static int RunInspectSegment(string[] args, TextWriter output, TextWriter error)
    {
        if (!TryGetOption(args, "--path", out var segmentPath))
        {
            error.WriteLine("missing required option --path for inspect segment");
            return 1;
        }

        if (!File.Exists(segmentPath))
        {
            error.WriteLine($"segment file does not exist: {segmentPath}");
            return 1;
        }

        var metadata = SegmentReader.ReadMetadata(segmentPath);
        var fileInfo = new FileInfo(segmentPath);
        var measurements = metadata.Select(m => m.Measurement).Distinct(StringComparer.Ordinal).Order().ToArray();
        var minTime = metadata.Count == 0 ? 0 : metadata.Min(m => m.MinTime);
        var maxTime = metadata.Count == 0 ? 0 : metadata.Max(m => m.MaxTime);
        var totalPoints = metadata.Sum(m => m.PointCount);

        output.WriteLine($"path={Path.GetFullPath(segmentPath)}");
        output.WriteLine($"file_bytes={fileInfo.Length}");
        output.WriteLine($"columns={metadata.Count}");
        output.WriteLine($"measurements={string.Join(",", measurements)}");
        output.WriteLine($"min_time_ns={minTime}");
        output.WriteLine($"max_time_ns={maxTime}");
        output.WriteLine($"total_points={totalPoints}");

        foreach (var entry in metadata
            .OrderBy(m => m.Measurement, StringComparer.Ordinal)
            .ThenBy(m => m.TagsCanonical, StringComparer.Ordinal)
            .ThenBy(m => m.Field, StringComparer.Ordinal))
        {
            var stats = entry.Stats == null
                ? "stats=none"
                : $"stats=min:{entry.Stats.Min},max:{entry.Stats.Max},sum:{entry.Stats.Sum},count:{entry.Stats.Count}";
            output.WriteLine($"column measurement={entry.Measurement} tags={entry.TagsCanonical} field={entry.Field} kind={entry.Kind} points={entry.PointCount} min_time_ns={entry.MinTime} max_time_ns={entry.MaxTime} {stats}");
        }

        return 0;
    }

    private static int RunInspectWal(string[] args, MiniInfluxOptions options, TextWriter output, TextWriter error)
    {
        var walPath = TryGetOption(args, "--path", out var explicitPath)
            ? explicitPath
            : Path.Combine(ResolveDataPath(args, options), "wal");

        if (!Directory.Exists(walPath))
        {
            error.WriteLine($"wal directory does not exist: {walPath}");
            return 1;
        }

        using var wal = new WalManager(walPath, fsync: false, fsyncIntervalMs: 0);
        var records = wal.ReplayWithPositions();
        var files = Directory.GetFiles(walPath, "*.wal")
            .Select(path => new FileInfo(path))
            .OrderBy(info => info.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        output.WriteLine($"path={Path.GetFullPath(walPath)}");
        output.WriteLine($"checkpoint={wal.CheckpointPosition.FileId}:{wal.CheckpointPosition.Offset}");
        output.WriteLine($"current_position={wal.CurrentPosition.FileId}:{wal.CurrentPosition.Offset}");
        output.WriteLine($"wal_files={files.Count}");
        output.WriteLine($"replay_records={records.Count}");

        foreach (var file in files)
            output.WriteLine($"file name={file.Name} bytes={file.Length}");

        return 0;
    }

    private static int RunRepair(string[] args, MiniInfluxOptions options, TextWriter output)
    {
        var dataPath = ResolveDataPath(args, options);
        using var engine = OpenOfflineEngine(dataPath, options);
        var result = engine.Recover();
        engine.FlushAll();
        output.WriteLine($"data_path={Path.GetFullPath(dataPath)}");
        output.WriteLine($"wal_records_replayed={result.WalRecordsReplayed}");
        output.WriteLine($"segments_scanned={result.SegmentsScanned}");
        output.WriteLine($"segments_corrupted={result.SegmentsCorrupted}");
        output.WriteLine($"schema_conflicts_skipped={result.SchemaConflictsSkipped}");
        return 0;
    }

    private static int RunCompact(string[] args, MiniInfluxOptions options, TextWriter output)
    {
        var dataPath = ResolveDataPath(args, options);
        using var engine = OpenOfflineEngine(dataPath, options);
        engine.Recover();
        engine.FlushAll();
        var compactor = new Compactor(engine.Meta, new ShardManager(engine.RootPath, engine.Meta), engine.Tombstones, engine.Schema, maxL0Segments: 2, maxL1Segments: 1);

        var totalMerged = 0;
        int merged;
        do
        {
            merged = compactor.CompactAll();
            totalMerged += merged;
        } while (merged > 0);

        var stats = compactor.GetStats();
        output.WriteLine($"data_path={Path.GetFullPath(dataPath)}");
        output.WriteLine($"compaction_tasks_merged={totalMerged}");
        output.WriteLine($"compaction_runs_total={stats.TotalRuns}");
        output.WriteLine($"segments_merged_total={stats.TotalSegmentsMerged}");
        return 0;
    }

    private static int RunBackup(string[] args, MiniInfluxOptions options, TextWriter output, TextWriter error)
    {
        if (!TryGetOption(args, "--path", out var backupPath))
        {
            error.WriteLine("missing required option --path for backup");
            return 1;
        }

        var dataPath = ResolveDataPath(args, options);
        using (var engine = OpenOfflineEngine(dataPath, options))
        {
            engine.Recover();
            engine.FlushAll();
        }

        BackupManager.CreateBackup(dataPath, backupPath);
        output.WriteLine($"backup_created={Path.GetFullPath(backupPath)}");
        return 0;
    }

    private static int RunRestore(string[] args, MiniInfluxOptions options, TextWriter output, TextWriter error)
    {
        if (!TryGetOption(args, "--path", out var backupPath))
        {
            error.WriteLine("missing required option --path for restore");
            return 1;
        }

        var dataPath = ResolveDataPath(args, options);
        BackupManager.PrepareRestore(backupPath, dataPath);
        output.WriteLine($"restore_prepared={Path.GetFullPath(dataPath)}.restore-pending");
        output.WriteLine("restart_required=true");
        return 0;
    }

    private static int ShowHelp(TextWriter output)
    {
        output.WriteLine("mini-influx benchmark [--points N] [--concurrency N] [--format text|json|prometheus] [--data PATH]");
        output.WriteLine("mini-influx inspect segment --path FILE");
        output.WriteLine("mini-influx inspect wal [--path DIR] [--data PATH]");
        output.WriteLine("mini-influx repair [--data PATH]");
        output.WriteLine("mini-influx compact [--data PATH]");
        output.WriteLine("mini-influx backup --path DIR [--data PATH]");
        output.WriteLine("mini-influx restore --path DIR [--data PATH]");
        return 0;
    }

    private static TsdbEngine OpenOfflineEngine(string dataPath, MiniInfluxOptions options) =>
        new(
            dataPath,
            options.FlushThreshold,
            options.Wal.MaxWalFileBytes,
            options.Wal.Fsync,
            options.Wal.FsyncIntervalMs,
            rpCheckIntervalMs: 0,
            maxSeriesPerDb: options.Storage.MaxSeriesPerDatabase,
            maxFieldsPerMeasurement: options.Storage.MaxFieldsPerMeasurement,
            flushIntervalMs: 0,
            maxBufferPoints: options.Storage.MaxBufferPoints,
            maxBufferBytes: options.Storage.MaxBufferBytes,
            compactionIntervalMs: 0);

    private static string ResolveDataPath(string[] args, MiniInfluxOptions options) =>
        TryGetOption(args, "--data", out var dataPath) ? dataPath : options.DataPath;

    private static BenchmarkRunOptions ParseBenchmarkOptions(string[] args)
    {
        var points = 10_000;
        var concurrency = 1;
        for (int i = 0; i < args.Length; i++)
        {
            if (string.Equals(args[i], "--points", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length && int.TryParse(args[i + 1], out var parsedPoints))
            {
                points = parsedPoints;
                i++;
            }
            else if (string.Equals(args[i], "--concurrency", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length && int.TryParse(args[i + 1], out var parsedConcurrency))
            {
                concurrency = parsedConcurrency;
                i++;
            }
        }

        return new BenchmarkRunOptions(points, concurrency);
    }

    private static string ParseBenchmarkFormat(string[] args)
    {
        for (int i = 0; i < args.Length; i++)
        {
            if (string.Equals(args[i], "--format", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
                return args[i + 1].Trim().ToLowerInvariant();
        }

        return "text";
    }

    private static bool TryGetOption(string[] args, string optionName, out string value)
    {
        for (int i = 0; i < args.Length; i++)
        {
            if (!string.Equals(args[i], optionName, StringComparison.OrdinalIgnoreCase))
                continue;

            if (i + 1 < args.Length && !string.IsNullOrWhiteSpace(args[i + 1]))
            {
                value = args[i + 1];
                return true;
            }

            break;
        }

        value = string.Empty;
        return false;
    }

    private static int WriteError(TextWriter error, string message)
    {
        error.WriteLine(message);
        return 1;
    }
}
