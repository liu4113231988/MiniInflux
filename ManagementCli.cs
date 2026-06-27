using System.Text;
using System.Text.Json;
using MiniInflux.Net10.Model;
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
            "validate" => RunValidate(args.Skip(1).ToArray(), options, output, error),
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
            error.WriteLine("missing inspect target: expected 'segment', 'wal', 'manifest', 'schema', or 'tombstone'");
            return 1;
        }

        return args[0].Trim().ToLowerInvariant() switch
        {
            "segment" => RunInspectSegment(args.Skip(1).ToArray(), output, error),
            "wal" => RunInspectWal(args.Skip(1).ToArray(), options, output, error),
            "manifest" => RunInspectManifest(args.Skip(1).ToArray(), options, output, error),
            "schema" => RunInspectSchema(args.Skip(1).ToArray(), options, output, error),
            "tombstone" => RunInspectTombstone(args.Skip(1).ToArray(), options, output, error),
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
        var format = ParseTextOrJsonFormat(args);

        if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        {
            var result = new CliInspectSegmentResult
            {
                Path = Path.GetFullPath(segmentPath),
                FileBytes = fileInfo.Length,
                Columns = metadata.Count,
                Measurements = measurements.ToList(),
                MinTimeNs = minTime,
                MaxTimeNs = maxTime,
                TotalPoints = totalPoints,
                ColumnEntries = metadata
                    .OrderBy(m => m.Measurement, StringComparer.Ordinal)
                    .ThenBy(m => m.TagsCanonical, StringComparer.Ordinal)
                    .ThenBy(m => m.Field, StringComparer.Ordinal)
                    .Select(entry => new CliSegmentColumnSummary
                    {
                        Measurement = entry.Measurement,
                        Tags = entry.TagsCanonical,
                        Field = entry.Field,
                        Kind = entry.Kind.ToString(),
                        Points = entry.PointCount,
                        MinTimeNs = entry.MinTime,
                        MaxTimeNs = entry.MaxTime,
                        Stats = entry.Stats == null
                            ? null
                            : new CliNumericStatsSummary
                            {
                                Min = entry.Stats.Min,
                                Max = entry.Stats.Max,
                                Sum = entry.Stats.Sum,
                                Count = entry.Stats.Count
                            }
                    })
                    .ToList()
            };
            output.WriteLine(JsonSerializer.Serialize(result, AppJsonContext.Default.CliInspectSegmentResult));
            return 0;
        }

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
        var format = ParseTextOrJsonFormat(args);

        if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        {
            var result = new CliInspectWalResult
            {
                Path = Path.GetFullPath(walPath),
                Checkpoint = $"{wal.CheckpointPosition.FileId}:{wal.CheckpointPosition.Offset}",
                CurrentPosition = $"{wal.CurrentPosition.FileId}:{wal.CurrentPosition.Offset}",
                WalFiles = files.Count,
                ReplayRecords = records.Count,
                Files = files.Select(file => new CliWalFileSummary
                {
                    Name = file.Name,
                    Bytes = file.Length
                }).ToList()
            };
            output.WriteLine(JsonSerializer.Serialize(result, AppJsonContext.Default.CliInspectWalResult));
            return 0;
        }

        output.WriteLine($"path={Path.GetFullPath(walPath)}");
        output.WriteLine($"checkpoint={wal.CheckpointPosition.FileId}:{wal.CheckpointPosition.Offset}");
        output.WriteLine($"current_position={wal.CurrentPosition.FileId}:{wal.CurrentPosition.Offset}");
        output.WriteLine($"wal_files={files.Count}");
        output.WriteLine($"replay_records={records.Count}");

        foreach (var file in files)
            output.WriteLine($"file name={file.Name} bytes={file.Length}");

        return 0;
    }

    private static int RunInspectManifest(string[] args, MiniInfluxOptions options, TextWriter output, TextWriter error)
    {
        var dataPath = ResolveDataPath(args, options);
        var manifestPath = Path.Combine(dataPath, "meta", "manifest.json");
        if (!File.Exists(manifestPath))
            return WriteError(error, $"manifest file does not exist: {manifestPath}");

        if (!TryReadManifestData(manifestPath, out var manifestData, out var manifestError))
            return WriteError(error, $"manifest is invalid: {manifestError}");

        var databases = manifestData!.Databases.OrderBy(kv => kv.Key, StringComparer.Ordinal).ToList();
        var rpCount = databases.Sum(kv => kv.Value.RetentionPolicies.Count);
        var shardCount = databases.Sum(kv => kv.Value.RetentionPolicies.Values.Sum(rp => rp.ShardGroups.Count));
        var segmentCount = databases.Sum(kv => kv.Value.RetentionPolicies.Values.Sum(rp => rp.ShardGroups.Sum(shard => shard.SegmentFiles.Count)));
        var measurementCount = databases.Sum(kv => kv.Value.SeriesIndex.Count);
        var seriesCount = databases.Sum(kv => kv.Value.SeriesIndex.Values.Sum(series => series.Count));
        var cqCount = databases.Sum(kv => kv.Value.ContinuousQueries.Count);
        var format = ParseTextOrJsonFormat(args);

        if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        {
            var result = new CliInspectManifestResult
            {
                Path = Path.GetFullPath(manifestPath),
                Databases = databases.Count,
                RetentionPolicies = rpCount,
                ShardGroups = shardCount,
                SegmentFiles = segmentCount,
                Measurements = measurementCount,
                Series = seriesCount,
                ContinuousQueries = cqCount,
                DatabaseEntries = databases.Select(kv =>
                {
                    var dbName = kv.Key;
                    var dbInfo = kv.Value;
                    return new CliManifestDatabaseSummary
                    {
                        Name = dbName,
                        RetentionPolicies = dbInfo.RetentionPolicies.Count,
                        ShardGroups = dbInfo.RetentionPolicies.Values.Sum(rp => rp.ShardGroups.Count),
                        SegmentFiles = dbInfo.RetentionPolicies.Values.Sum(rp => rp.ShardGroups.Sum(shard => shard.SegmentFiles.Count)),
                        Measurements = dbInfo.SeriesIndex.Count,
                        Series = dbInfo.SeriesIndex.Values.Sum(series => series.Count),
                        ContinuousQueries = dbInfo.ContinuousQueries.Count,
                        RetentionPolicyEntries = dbInfo.RetentionPolicies.Values
                            .OrderBy(rp => rp.Name, StringComparer.Ordinal)
                            .Select(rp => new CliManifestRetentionPolicySummary
                            {
                                Database = dbName,
                                Name = rp.Name,
                                DurationNs = rp.DurationNs,
                                Replication = rp.Replication,
                                IsDefault = rp.IsDefault,
                                ShardGroups = rp.ShardGroups.Count,
                                ShardEntries = rp.ShardGroups
                                    .OrderBy(shard => shard.Id)
                                    .Select(shard => new CliManifestShardSummary
                                    {
                                        Database = dbName,
                                        RetentionPolicy = rp.Name,
                                        Id = shard.Id,
                                        StartTimeNs = shard.StartTimeNs,
                                        EndTimeNs = shard.EndTimeNs,
                                        Segments = shard.SegmentFiles.Count,
                                        Files = shard.SegmentFiles.OrderBy(name => name, StringComparer.OrdinalIgnoreCase).ToList()
                                    })
                                    .ToList()
                            })
                            .ToList(),
                        MeasurementEntries = dbInfo.SeriesIndex
                            .OrderBy(measurement => measurement.Key, StringComparer.Ordinal)
                            .Select(measurement => new CliManifestMeasurementSummary
                            {
                                Database = dbName,
                                Name = measurement.Key,
                                Series = measurement.Value.Count,
                                TagKeys = dbInfo.TagIndex.TryGetValue(measurement.Key, out var tagMap)
                                    ? tagMap.Keys.OrderBy(key => key, StringComparer.Ordinal).ToList()
                                    : []
                            })
                            .ToList(),
                        ContinuousQueryEntries = dbInfo.ContinuousQueries.Values
                            .OrderBy(query => query.Name, StringComparer.Ordinal)
                            .Select(cq => new CliManifestContinuousQuerySummary
                            {
                                Database = dbName,
                                Name = cq.Name,
                                EveryNs = cq.EveryNs,
                                ForNs = cq.ForNs,
                                RecomputeRecentBuckets = cq.RecomputeRecentBuckets,
                                LastCompletedBucketStartNs = cq.LastCompletedBucketStartNs
                            })
                            .ToList()
                    };
                }).ToList()
            };
            output.WriteLine(JsonSerializer.Serialize(result, AppJsonContext.Default.CliInspectManifestResult));
            return 0;
        }

        output.WriteLine($"path={Path.GetFullPath(manifestPath)}");
        output.WriteLine($"databases={databases.Count}");
        output.WriteLine($"retention_policies={rpCount}");
        output.WriteLine($"shard_groups={shardCount}");
        output.WriteLine($"segment_files={segmentCount}");
        output.WriteLine($"measurements={measurementCount}");
        output.WriteLine($"series={seriesCount}");
        output.WriteLine($"continuous_queries={cqCount}");

        foreach (var (dbName, dbInfo) in databases)
        {
            var dbShardCount = dbInfo.RetentionPolicies.Values.Sum(rp => rp.ShardGroups.Count);
            var dbSegmentCount = dbInfo.RetentionPolicies.Values.Sum(rp => rp.ShardGroups.Sum(shard => shard.SegmentFiles.Count));
            var dbSeriesCount = dbInfo.SeriesIndex.Values.Sum(series => series.Count);
            output.WriteLine(
                $"database name={dbName} retention_policies={dbInfo.RetentionPolicies.Count} shard_groups={dbShardCount} segment_files={dbSegmentCount} measurements={dbInfo.SeriesIndex.Count} series={dbSeriesCount} continuous_queries={dbInfo.ContinuousQueries.Count}");

            foreach (var rp in dbInfo.RetentionPolicies.Values.OrderBy(rp => rp.Name, StringComparer.Ordinal))
            {
                output.WriteLine(
                    $"retention_policy db={dbName} name={rp.Name} duration_ns={rp.DurationNs} replication={rp.Replication} default={rp.IsDefault.ToString().ToLowerInvariant()} shard_groups={rp.ShardGroups.Count}");

                foreach (var shard in rp.ShardGroups.OrderBy(shard => shard.Id))
                {
                    output.WriteLine(
                        $"shard db={dbName} rp={rp.Name} id={shard.Id} start_time_ns={shard.StartTimeNs} end_time_ns={shard.EndTimeNs} segments={shard.SegmentFiles.Count} files={string.Join(",", shard.SegmentFiles.OrderBy(name => name, StringComparer.OrdinalIgnoreCase))}");
                }
            }

            foreach (var measurement in dbInfo.SeriesIndex.OrderBy(kv => kv.Key, StringComparer.Ordinal))
            {
                var tagKeys = dbInfo.TagIndex.TryGetValue(measurement.Key, out var tagMap)
                    ? string.Join(",", tagMap.Keys.OrderBy(key => key, StringComparer.Ordinal))
                    : string.Empty;
                output.WriteLine(
                    $"measurement db={dbName} name={measurement.Key} series={measurement.Value.Count} tag_keys={tagKeys}");
            }

            foreach (var cq in dbInfo.ContinuousQueries.Values.OrderBy(q => q.Name, StringComparer.Ordinal))
            {
                output.WriteLine(
                    $"continuous_query db={dbName} name={cq.Name} every_ns={cq.EveryNs} for_ns={cq.ForNs} recompute_recent_buckets={cq.RecomputeRecentBuckets} last_completed_bucket_start_ns={cq.LastCompletedBucketStartNs}");
            }
        }

        return 0;
    }

    private static int RunInspectSchema(string[] args, MiniInfluxOptions options, TextWriter output, TextWriter error)
    {
        var dataPath = ResolveDataPath(args, options);
        var schemaPath = Path.Combine(dataPath, "meta", "schema.json");
        if (!File.Exists(schemaPath))
            return WriteError(error, $"schema file does not exist: {schemaPath}");

        if (!TryReadSchemaEntries(schemaPath, out var entries, out var schemaError))
            return WriteError(error, $"schema is invalid: {schemaError}");

        var dbFilter = TryGetOption(args, "--db", out var dbValue) ? dbValue.Trim() : null;
        var measurementFilter = TryGetOption(args, "--measurement", out var measurementValue) ? measurementValue.Trim() : null;
        var filteredEntries = entries!
            .Where(entry => string.IsNullOrWhiteSpace(dbFilter) || string.Equals(entry.Db, dbFilter, StringComparison.Ordinal))
            .Where(entry => string.IsNullOrWhiteSpace(measurementFilter) || string.Equals(entry.Measurement, measurementFilter, StringComparison.Ordinal))
            .OrderBy(entry => entry.Db, StringComparer.Ordinal)
            .ThenBy(entry => entry.Measurement, StringComparer.Ordinal)
            .ThenBy(entry => entry.Field, StringComparer.Ordinal)
            .ToList();
        var format = ParseTextOrJsonFormat(args);

        if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        {
            var result = new CliInspectSchemaResult
            {
                Path = Path.GetFullPath(schemaPath),
                Entries = filteredEntries.Count,
                Databases = filteredEntries.Select(entry => entry.Db).Distinct(StringComparer.Ordinal).Count(),
                Measurements = filteredEntries.Select(entry => $"{entry.Db}|{entry.Measurement}").Distinct(StringComparer.Ordinal).Count(),
                MeasurementEntries = filteredEntries
                    .GroupBy(entry => new { entry.Db, entry.Measurement })
                    .Select(group => new CliSchemaMeasurementSummary
                    {
                        Database = group.Key.Db,
                        Name = group.Key.Measurement,
                        Fields = group.Count()
                    })
                    .OrderBy(entry => entry.Database, StringComparer.Ordinal)
                    .ThenBy(entry => entry.Name, StringComparer.Ordinal)
                    .ToList(),
                FieldEntries = filteredEntries.Select(entry => new CliSchemaFieldSummary
                {
                    Database = entry.Db,
                    Measurement = entry.Measurement,
                    Name = entry.Field,
                    Kind = ((FieldKind)entry.Kind).ToString()
                }).ToList()
            };
            output.WriteLine(JsonSerializer.Serialize(result, AppJsonContext.Default.CliInspectSchemaResult));
            return 0;
        }

        output.WriteLine($"path={Path.GetFullPath(schemaPath)}");
        output.WriteLine($"entries={filteredEntries.Count}");
        output.WriteLine($"databases={filteredEntries.Select(entry => entry.Db).Distinct(StringComparer.Ordinal).Count()}");
        output.WriteLine($"measurements={filteredEntries.Select(entry => $"{entry.Db}|{entry.Measurement}").Distinct(StringComparer.Ordinal).Count()}");

        foreach (var group in filteredEntries.GroupBy(entry => new { entry.Db, entry.Measurement }))
            output.WriteLine($"measurement db={group.Key.Db} name={group.Key.Measurement} fields={group.Count()}");

        foreach (var entry in filteredEntries)
            output.WriteLine($"field db={entry.Db} measurement={entry.Measurement} name={entry.Field} kind={(FieldKind)entry.Kind}");

        return 0;
    }

    private static int RunInspectTombstone(string[] args, MiniInfluxOptions options, TextWriter output, TextWriter error)
    {
        var dataPath = ResolveDataPath(args, options);
        var tombstoneDir = Path.Combine(dataPath, "tombstones");
        if (!Directory.Exists(tombstoneDir))
            return WriteError(error, $"tombstone directory does not exist: {tombstoneDir}");

        var dbFilter = TryGetOption(args, "--db", out var dbValue) ? dbValue.Trim() : null;
        var tombstoneFiles = Directory.GetFiles(tombstoneDir, "*.json")
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var summaries = new List<(string Db, Tombstone Tombstone)>();

        foreach (var file in tombstoneFiles)
        {
            var dbName = Path.GetFileNameWithoutExtension(file);
            if (!string.IsNullOrWhiteSpace(dbFilter) && !string.Equals(dbName, dbFilter, StringComparison.Ordinal))
                continue;

            if (!TryReadTombstones(file, out var tombstones, out var tombstoneError))
                return WriteError(error, $"tombstone file is invalid: {Path.GetFileName(file)}: {tombstoneError}");

            summaries.AddRange(tombstones!.Select(tombstone => (dbName, tombstone)));
        }
        var format = ParseTextOrJsonFormat(args);

        if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        {
            var result = new CliInspectTombstoneResult
            {
                Path = Path.GetFullPath(tombstoneDir),
                TombstoneFiles = tombstoneFiles.Length,
                Tombstones = summaries.Count,
                Databases = summaries.Select(summary => summary.Db).Distinct(StringComparer.Ordinal).Count(),
                DatabaseEntries = summaries
                    .GroupBy(summary => summary.Db, StringComparer.Ordinal)
                    .OrderBy(group => group.Key, StringComparer.Ordinal)
                    .Select(group => new CliTombstoneDatabaseSummary
                    {
                        Name = group.Key,
                        Tombstones = group.Count()
                    })
                    .ToList(),
                TombstoneEntries = summaries
                    .OrderBy(summary => summary.Db, StringComparer.Ordinal)
                    .ThenBy(summary => summary.Tombstone.Measurement, StringComparer.Ordinal)
                    .ThenBy(summary => summary.Tombstone.TagsCanonical ?? string.Empty, StringComparer.Ordinal)
                    .ThenBy(summary => summary.Tombstone.MinTimeNs ?? long.MinValue)
                    .ThenBy(summary => summary.Tombstone.MaxTimeNs ?? long.MaxValue)
                    .Select(summary => new CliTombstoneEntry
                    {
                        Database = summary.Db,
                        Measurement = summary.Tombstone.Measurement,
                        Tags = summary.Tombstone.TagsCanonical ?? "*",
                        MinTimeNs = summary.Tombstone.MinTimeNs,
                        MaxTimeNs = summary.Tombstone.MaxTimeNs,
                        CreatedAtNs = summary.Tombstone.CreatedAtNs
                    })
                    .ToList()
            };
            output.WriteLine(JsonSerializer.Serialize(result, AppJsonContext.Default.CliInspectTombstoneResult));
            return 0;
        }

        output.WriteLine($"path={Path.GetFullPath(tombstoneDir)}");
        output.WriteLine($"tombstone_files={tombstoneFiles.Length}");
        output.WriteLine($"tombstones={summaries.Count}");
        output.WriteLine($"databases={summaries.Select(summary => summary.Db).Distinct(StringComparer.Ordinal).Count()}");

        foreach (var group in summaries
            .GroupBy(summary => summary.Db, StringComparer.Ordinal)
            .OrderBy(group => group.Key, StringComparer.Ordinal))
        {
            output.WriteLine($"database name={group.Key} tombstones={group.Count()}");
        }

        foreach (var summary in summaries
            .OrderBy(summary => summary.Db, StringComparer.Ordinal)
            .ThenBy(summary => summary.Tombstone.Measurement, StringComparer.Ordinal)
            .ThenBy(summary => summary.Tombstone.TagsCanonical ?? string.Empty, StringComparer.Ordinal)
            .ThenBy(summary => summary.Tombstone.MinTimeNs ?? long.MinValue)
            .ThenBy(summary => summary.Tombstone.MaxTimeNs ?? long.MaxValue))
        {
            output.WriteLine(
                $"tombstone db={summary.Db} measurement={summary.Tombstone.Measurement} tags={summary.Tombstone.TagsCanonical ?? "*"} min_time_ns={(summary.Tombstone.MinTimeNs?.ToString() ?? "*")} max_time_ns={(summary.Tombstone.MaxTimeNs?.ToString() ?? "*")} created_at_ns={summary.Tombstone.CreatedAtNs}");
        }

        return 0;
    }

    private static int RunValidate(string[] args, MiniInfluxOptions options, TextWriter output, TextWriter error)
    {
        if (args.Length == 0)
        {
            error.WriteLine("missing validate target: expected 'data-dir'");
            return 1;
        }

        return args[0].Trim().ToLowerInvariant() switch
        {
            "data-dir" => RunValidateDataDir(args.Skip(1).ToArray(), options, output, error),
            _ => WriteError(error, $"unsupported validate target: {args[0]}")
        };
    }

    private static int RunValidateDataDir(string[] args, MiniInfluxOptions options, TextWriter output, TextWriter error)
    {
        var dataPath = ResolveDataPath(args, options);
        if (!Directory.Exists(dataPath))
            return WriteError(error, $"data directory does not exist: {dataPath}");

        var issues = new List<(string Code, string Message)>();
        var warnings = new List<(string Code, string Message)>();
        var manifestPath = Path.Combine(dataPath, "meta", "manifest.json");
        var schemaPath = Path.Combine(dataPath, "meta", "schema.json");
        var tombstoneDir = Path.Combine(dataPath, "tombstones");
        var dbRoot = Path.Combine(dataPath, "db");
        var walPath = Path.Combine(dataPath, "wal");
        var restorePending = dataPath + ".restore-pending";
        var restorePrevious = dataPath + ".restore-previous";

        ManifestData? manifestData = null;
        if (File.Exists(manifestPath))
        {
            if (!TryReadManifestData(manifestPath, out manifestData, out var manifestError))
                issues.Add(("manifest_invalid", $"manifest json is invalid: {manifestError}"));
        }
        else
        {
            var hasSegmentsOnDisk = Directory.Exists(dbRoot) && Directory.EnumerateFiles(dbRoot, "*.seg", SearchOption.AllDirectories).Any();
            if (hasSegmentsOnDisk)
                issues.Add(("manifest_missing", "manifest.json is missing while segment files exist on disk"));
        }

        if (File.Exists(schemaPath) && !TryReadSchemaEntries(schemaPath, out _, out var schemaError))
            issues.Add(("schema_invalid", $"schema json is invalid: {schemaError}"));

        var tombstoneFiles = Directory.Exists(tombstoneDir)
            ? Directory.GetFiles(tombstoneDir, "*.json").OrderBy(path => path, StringComparer.OrdinalIgnoreCase).ToArray()
            : Array.Empty<string>();
        foreach (var tombstoneFile in tombstoneFiles)
        {
            if (!TryReadTombstones(tombstoneFile, out _, out var tombstoneError))
                issues.Add(("tombstone_invalid", $"{Path.GetFileName(tombstoneFile)}: {tombstoneError}"));
        }

        if (Directory.Exists(restorePending))
            warnings.Add(("restore_pending_present", $"pending restore directory exists: {restorePending}"));
        if (Directory.Exists(restorePrevious))
            warnings.Add(("restore_previous_present", $"previous restore backup directory exists: {restorePrevious}"));

        var segmentFilesOnDisk = Directory.Exists(dbRoot)
            ? Directory.EnumerateFiles(dbRoot, "*.seg", SearchOption.AllDirectories).Count()
            : 0;
        var walFilesOnDisk = Directory.Exists(walPath)
            ? Directory.EnumerateFiles(walPath, "*.wal", SearchOption.TopDirectoryOnly).Count()
            : 0;

        var manifestDbCount = 0;
        var manifestRpCount = 0;
        var manifestShardCount = 0;
        var manifestSegmentCount = 0;

        if (manifestData != null)
        {
            manifestDbCount = manifestData.Databases.Count;
            manifestRpCount = manifestData.Databases.Sum(kv => kv.Value.RetentionPolicies.Count);
            manifestShardCount = manifestData.Databases.Sum(kv => kv.Value.RetentionPolicies.Values.Sum(rp => rp.ShardGroups.Count));
            manifestSegmentCount = manifestData.Databases.Sum(kv => kv.Value.RetentionPolicies.Values.Sum(rp => rp.ShardGroups.Sum(shard => shard.SegmentFiles.Count)));
            ValidateManifestLayout(dataPath, manifestData, issues, warnings);
        }
        var format = ParseTextOrJsonFormat(args);

        if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        {
            var result = new CliValidateDataDirResult
            {
                DataPath = Path.GetFullPath(dataPath),
                ManifestPresent = File.Exists(manifestPath),
                SchemaPresent = File.Exists(schemaPath),
                TombstoneFiles = tombstoneFiles.Length,
                WalFiles = walFilesOnDisk,
                SegmentsOnDisk = segmentFilesOnDisk,
                ManifestDatabases = manifestDbCount,
                ManifestRetentionPolicies = manifestRpCount,
                ManifestShardGroups = manifestShardCount,
                ManifestSegmentFiles = manifestSegmentCount,
                Issues = issues.Count,
                Warnings = warnings.Count,
                IssueEntries = issues.Select(issue => new CliValidationMessage { Code = issue.Code, Message = issue.Message }).ToList(),
                WarningEntries = warnings.Select(warning => new CliValidationMessage { Code = warning.Code, Message = warning.Message }).ToList()
            };
            output.WriteLine(JsonSerializer.Serialize(result, AppJsonContext.Default.CliValidateDataDirResult));
            return issues.Count == 0 ? 0 : 1;
        }

        output.WriteLine($"data_path={Path.GetFullPath(dataPath)}");
        output.WriteLine($"manifest_present={File.Exists(manifestPath).ToString().ToLowerInvariant()}");
        output.WriteLine($"schema_present={File.Exists(schemaPath).ToString().ToLowerInvariant()}");
        output.WriteLine($"tombstone_files={tombstoneFiles.Length}");
        output.WriteLine($"wal_files={walFilesOnDisk}");
        output.WriteLine($"segments_on_disk={segmentFilesOnDisk}");
        output.WriteLine($"manifest_databases={manifestDbCount}");
        output.WriteLine($"manifest_retention_policies={manifestRpCount}");
        output.WriteLine($"manifest_shard_groups={manifestShardCount}");
        output.WriteLine($"manifest_segment_files={manifestSegmentCount}");
        output.WriteLine($"issues={issues.Count}");
        output.WriteLine($"warnings={warnings.Count}");

        foreach (var issue in issues)
            output.WriteLine($"issue code={issue.Code} message={issue.Message}");
        foreach (var warning in warnings)
            output.WriteLine($"warning code={warning.Code} message={warning.Message}");

        return issues.Count == 0 ? 0 : 1;
    }

    private static int RunRepair(string[] args, MiniInfluxOptions options, TextWriter output)
    {
        var dataPath = ResolveDataPath(args, options);
        var dryRun = HasFlag(args, "--dry-run");
        var format = ParseTextOrJsonFormat(args);
        var executionDataPath = dryRun ? CreateDryRunDataClone(dataPath) : dataPath;
        try
        {
            using var engine = OpenOfflineEngine(executionDataPath, options);
            var result = engine.Recover();
            engine.FlushAll();

            if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
            {
                var jsonResult = new CliRepairResult
                {
                    DataPath = Path.GetFullPath(dataPath),
                    DryRun = dryRun,
                    ChangesApplied = !dryRun,
                    WalRecordsReplayed = result.WalRecordsReplayed,
                    SegmentsScanned = result.SegmentsScanned,
                    SegmentsCorrupted = result.SegmentsCorrupted,
                    SchemaConflictsSkipped = result.SchemaConflictsSkipped
                };
                output.WriteLine(JsonSerializer.Serialize(jsonResult, AppJsonContext.Default.CliRepairResult));
                return 0;
            }

            output.WriteLine($"data_path={Path.GetFullPath(dataPath)}");
            output.WriteLine($"dry_run={dryRun.ToString().ToLowerInvariant()}");
            output.WriteLine($"wal_records_replayed={result.WalRecordsReplayed}");
            output.WriteLine($"segments_scanned={result.SegmentsScanned}");
            output.WriteLine($"segments_corrupted={result.SegmentsCorrupted}");
            output.WriteLine($"schema_conflicts_skipped={result.SchemaConflictsSkipped}");
            if (dryRun)
                output.WriteLine("changes_applied=false");
            return 0;
        }
        finally
        {
            if (dryRun)
                DeleteDirectoryBestEffort(executionDataPath);
        }
    }

    private static int RunCompact(string[] args, MiniInfluxOptions options, TextWriter output)
    {
        var dataPath = ResolveDataPath(args, options);
        var dryRun = HasFlag(args, "--dry-run");
        var format = ParseTextOrJsonFormat(args);
        var executionDataPath = dryRun ? CreateDryRunDataClone(dataPath) : dataPath;
        try
        {
            using var engine = OpenOfflineEngine(executionDataPath, options);
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

            if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
            {
                var jsonResult = new CliCompactResult
                {
                    DataPath = Path.GetFullPath(dataPath),
                    DryRun = dryRun,
                    ChangesApplied = !dryRun,
                    CompactionTasksMerged = totalMerged,
                    CompactionRunsTotal = stats.TotalRuns,
                    SegmentsMergedTotal = stats.TotalSegmentsMerged
                };
                output.WriteLine(JsonSerializer.Serialize(jsonResult, AppJsonContext.Default.CliCompactResult));
                return 0;
            }

            output.WriteLine($"data_path={Path.GetFullPath(dataPath)}");
            output.WriteLine($"dry_run={dryRun.ToString().ToLowerInvariant()}");
            output.WriteLine($"compaction_tasks_merged={totalMerged}");
            output.WriteLine($"compaction_runs_total={stats.TotalRuns}");
            output.WriteLine($"segments_merged_total={stats.TotalSegmentsMerged}");
            if (dryRun)
                output.WriteLine("changes_applied=false");
            return 0;
        }
        finally
        {
            if (dryRun)
                DeleteDirectoryBestEffort(executionDataPath);
        }
    }

    private static int RunBackup(string[] args, MiniInfluxOptions options, TextWriter output, TextWriter error)
    {
        if (args.Length > 0 && string.Equals(args[0], "verify", StringComparison.OrdinalIgnoreCase))
            return RunBackupVerify(args.Skip(1).ToArray(), output, error);

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
        if (string.Equals(ParseTextOrJsonFormat(args), "json", StringComparison.OrdinalIgnoreCase))
        {
            var result = new CliBackupCreateResult { BackupCreated = Path.GetFullPath(backupPath) };
            output.WriteLine(JsonSerializer.Serialize(result, AppJsonContext.Default.CliBackupCreateResult));
            return 0;
        }

        output.WriteLine($"backup_created={Path.GetFullPath(backupPath)}");
        return 0;
    }

    private static int RunBackupVerify(string[] args, TextWriter output, TextWriter error)
    {
        if (!TryGetOption(args, "--path", out var backupPath))
        {
            error.WriteLine("missing required option --path for backup verify");
            return 1;
        }

        if (!Directory.Exists(backupPath))
            return WriteError(error, $"backup path does not exist: {backupPath}");

        var metadataPath = Path.Combine(backupPath, "backup.metadata.json");
        if (!File.Exists(metadataPath))
            return WriteError(error, $"backup metadata file does not exist: {metadataPath}");

        if (!TryReadBackupMetadata(metadataPath, out var metadata, out var metadataError))
            return WriteError(error, $"backup metadata is invalid: {metadataError}");

        try
        {
            BackupManager.ValidateBackup(backupPath);
        }
        catch (Exception ex) when (ex is InvalidDataException or IOException)
        {
            return WriteError(error, $"backup verification failed: {ex.Message}");
        }

        if (string.Equals(ParseTextOrJsonFormat(args), "json", StringComparison.OrdinalIgnoreCase))
        {
            var result = new CliBackupVerifyResult
            {
                BackupPath = Path.GetFullPath(backupPath),
                FormatVersion = metadata!.FormatVersion,
                CreatedAtUtc = metadata.CreatedAtUtc,
                SourceRoot = metadata.SourceRoot,
                Files = metadata.Files.Count,
                Verified = true
            };
            output.WriteLine(JsonSerializer.Serialize(result, AppJsonContext.Default.CliBackupVerifyResult));
            return 0;
        }

        output.WriteLine($"backup_path={Path.GetFullPath(backupPath)}");
        output.WriteLine($"format_version={metadata!.FormatVersion}");
        output.WriteLine($"created_at_utc={metadata.CreatedAtUtc:O}");
        output.WriteLine($"source_root={metadata.SourceRoot}");
        output.WriteLine($"files={metadata.Files.Count}");
        output.WriteLine("verified=true");
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
        var format = ParseTextOrJsonFormat(args);
        if (HasFlag(args, "--dry-run"))
        {
            try
            {
                BackupManager.ValidateBackup(backupPath);
            }
            catch (Exception ex) when (ex is InvalidDataException or IOException or DirectoryNotFoundException)
            {
                return WriteError(error, $"restore dry-run failed: {ex.Message}");
            }

            if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
            {
                var result = new CliRestoreResult
                {
                    BackupPath = Path.GetFullPath(backupPath),
                    RestoreTarget = Path.GetFullPath(dataPath),
                    PendingRestorePath = Path.GetFullPath(dataPath) + ".restore-pending",
                    DryRun = true,
                    ValidatedBackup = true,
                    ChangesApplied = false,
                    RestartRequired = false
                };
                output.WriteLine(JsonSerializer.Serialize(result, AppJsonContext.Default.CliRestoreResult));
                return 0;
            }

            output.WriteLine($"backup_path={Path.GetFullPath(backupPath)}");
            output.WriteLine($"restore_target={Path.GetFullPath(dataPath)}");
            output.WriteLine($"pending_restore_path={Path.GetFullPath(dataPath)}.restore-pending");
            output.WriteLine("dry_run=true");
            output.WriteLine("validated_backup=true");
            output.WriteLine("changes_applied=false");
            output.WriteLine("restart_required=false");
            return 0;
        }

        BackupManager.PrepareRestore(backupPath, dataPath);
        if (string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        {
            var result = new CliRestoreResult
            {
                BackupPath = Path.GetFullPath(backupPath),
                RestoreTarget = Path.GetFullPath(dataPath),
                PendingRestorePath = Path.GetFullPath(dataPath) + ".restore-pending",
                DryRun = false,
                ValidatedBackup = true,
                ChangesApplied = true,
                RestartRequired = true
            };
            output.WriteLine(JsonSerializer.Serialize(result, AppJsonContext.Default.CliRestoreResult));
            return 0;
        }

        output.WriteLine($"restore_prepared={Path.GetFullPath(dataPath)}.restore-pending");
        output.WriteLine("restart_required=true");
        return 0;
    }

    private static int ShowHelp(TextWriter output)
    {
        output.WriteLine("mini-influx benchmark [--points N] [--concurrency N] [--format text|json|prometheus] [--data PATH]");
        output.WriteLine("mini-influx inspect segment --path FILE [--format text|json]");
        output.WriteLine("mini-influx inspect wal [--path DIR] [--data PATH] [--format text|json]");
        output.WriteLine("mini-influx inspect manifest [--data PATH] [--format text|json]");
        output.WriteLine("mini-influx inspect schema [--data PATH] [--db NAME] [--measurement NAME] [--format text|json]");
        output.WriteLine("mini-influx inspect tombstone [--data PATH] [--db NAME] [--format text|json]");
        output.WriteLine("mini-influx validate data-dir [--data PATH] [--format text|json]");
        output.WriteLine("mini-influx repair [--data PATH] [--dry-run] [--format text|json]");
        output.WriteLine("mini-influx compact [--data PATH] [--dry-run] [--format text|json]");
        output.WriteLine("mini-influx backup --path DIR [--data PATH] [--format text|json]");
        output.WriteLine("mini-influx backup verify --path DIR [--format text|json]");
        output.WriteLine("mini-influx restore --path DIR [--data PATH] [--dry-run] [--format text|json]");
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

    private static string ParseTextOrJsonFormat(string[] args)
    {
        var format = ParseBenchmarkFormat(args);
        return string.Equals(format, "json", StringComparison.OrdinalIgnoreCase) ? "json" : "text";
    }

    private static bool HasFlag(string[] args, string flagName) =>
        args.Any(arg => string.Equals(arg, flagName, StringComparison.OrdinalIgnoreCase));

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

    private static bool TryReadManifestData(string manifestPath, out ManifestData? data, out string error)
    {
        try
        {
            var json = File.ReadAllText(manifestPath);
            data = JsonSerializer.Deserialize(json, ManifestJsonContext.Default.ManifestData);
            if (data == null)
            {
                error = "manifest content is empty";
                return false;
            }

            error = string.Empty;
            return true;
        }
        catch (Exception ex)
        {
            data = null;
            error = ex.Message;
            return false;
        }
    }

    private static bool TryReadSchemaEntries(string schemaPath, out List<SchemaEntry>? entries, out string error)
    {
        try
        {
            var json = File.ReadAllText(schemaPath);
            entries = JsonSerializer.Deserialize(json, SchemaJsonContext.Default.ListSchemaEntry);
            if (entries == null)
            {
                error = "schema content is empty";
                return false;
            }

            error = string.Empty;
            return true;
        }
        catch (Exception ex)
        {
            entries = null;
            error = ex.Message;
            return false;
        }
    }

    private static bool TryReadTombstones(string tombstonePath, out List<Tombstone>? tombstones, out string error)
    {
        try
        {
            var json = File.ReadAllText(tombstonePath);
            tombstones = JsonSerializer.Deserialize(json, TombstoneJsonContext.Default.ListTombstone);
            if (tombstones == null)
            {
                error = "tombstone content is empty";
                return false;
            }

            error = string.Empty;
            return true;
        }
        catch (Exception ex)
        {
            tombstones = null;
            error = ex.Message;
            return false;
        }
    }

    private static bool TryReadBackupMetadata(string metadataPath, out BackupMetadata? metadata, out string error)
    {
        try
        {
            metadata = JsonSerializer.Deserialize(File.ReadAllText(metadataPath), AppJsonContext.Default.BackupMetadata) as BackupMetadata;
            if (metadata == null)
            {
                error = "backup metadata content is empty";
                return false;
            }

            error = string.Empty;
            return true;
        }
        catch (Exception ex)
        {
            metadata = null;
            error = ex.Message;
            return false;
        }
    }

    private static void ValidateManifestLayout(
        string dataPath,
        ManifestData manifestData,
        List<(string Code, string Message)> issues,
        List<(string Code, string Message)> warnings)
    {
        foreach (var (dbName, dbInfo) in manifestData.Databases)
        {
            foreach (var (rpName, rpInfo) in dbInfo.RetentionPolicies)
            {
                foreach (var shard in rpInfo.ShardGroups)
                {
                    if (shard.EndTimeNs < shard.StartTimeNs)
                    {
                        issues.Add(("shard_range_invalid", $"{dbName}/{rpName}/shard {shard.Id} has end_time_ns < start_time_ns"));
                        continue;
                    }

                    var shardDir = Path.Combine(dataPath, "db", dbName, rpName, "shards", shard.Id.ToString("D6"));
                    if (!Directory.Exists(shardDir))
                    {
                        issues.Add(("shard_dir_missing", $"{dbName}/{rpName}/shard {shard.Id} directory is missing"));
                        continue;
                    }

                    var manifestNames = shard.SegmentFiles
                        .Where(name => !string.IsNullOrWhiteSpace(name))
                        .Select(Path.GetFileName)
                        .Where(name => !string.IsNullOrWhiteSpace(name))
                        .Select(name => name!)
                        .ToList();
                    var duplicateNames = manifestNames
                        .GroupBy(name => name, StringComparer.OrdinalIgnoreCase)
                        .Where(group => group.Count() > 1)
                        .Select(group => group.Key)
                        .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
                        .ToArray();
                    foreach (var duplicateName in duplicateNames)
                        issues.Add(("manifest_segment_duplicate", $"{dbName}/{rpName}/shard {shard.Id} duplicates segment entry {duplicateName}"));

                    var manifestSet = manifestNames.ToHashSet(StringComparer.OrdinalIgnoreCase);
                    var actualSegments = Directory.GetFiles(shardDir, "*.seg", SearchOption.TopDirectoryOnly)
                        .Select(Path.GetFileName)
                        .Where(name => !string.IsNullOrWhiteSpace(name))
                        .Select(name => name!)
                        .ToHashSet(StringComparer.OrdinalIgnoreCase);

                    foreach (var segmentName in manifestSet.OrderBy(name => name, StringComparer.OrdinalIgnoreCase))
                    {
                        if (!actualSegments.Contains(segmentName))
                            issues.Add(("segment_missing_on_disk", $"{dbName}/{rpName}/shard {shard.Id} manifest references missing file {segmentName}"));
                    }

                    foreach (var segmentName in actualSegments.OrderBy(name => name, StringComparer.OrdinalIgnoreCase))
                    {
                        if (!manifestSet.Contains(segmentName))
                            issues.Add(("segment_orphan_on_disk", $"{dbName}/{rpName}/shard {shard.Id} contains untracked segment file {segmentName}"));
                    }

                    foreach (var tmpFile in Directory.GetFiles(shardDir, "*.tmp", SearchOption.TopDirectoryOnly)
                        .OrderBy(path => path, StringComparer.OrdinalIgnoreCase))
                    {
                        warnings.Add(("temporary_file_present", $"{dbName}/{rpName}/shard {shard.Id} contains temporary file {Path.GetFileName(tmpFile)}"));
                    }
                }
            }
        }
    }

    private static int WriteError(TextWriter error, string message)
    {
        error.WriteLine(message);
        return 1;
    }

    private static string CreateDryRunDataClone(string dataPath)
    {
        var source = Path.GetFullPath(dataPath);
        if (!Directory.Exists(source))
            throw new DirectoryNotFoundException($"data directory does not exist: {source}");

        var cloneRoot = Path.Combine(Path.GetTempPath(), "miniinflux-dryrun", Guid.NewGuid().ToString("N"));
        CopyDirectoryRecursive(source, cloneRoot);
        return cloneRoot;
    }

    private static void CopyDirectoryRecursive(string source, string destination)
    {
        Directory.CreateDirectory(destination);

        foreach (var file in Directory.GetFiles(source))
            File.Copy(file, Path.Combine(destination, Path.GetFileName(file)), overwrite: true);

        foreach (var directory in Directory.GetDirectories(source))
            CopyDirectoryRecursive(directory, Path.Combine(destination, Path.GetFileName(directory)));
    }

    private static void DeleteDirectoryBestEffort(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best effort cleanup for dry-run temp directories.
        }
    }
}
