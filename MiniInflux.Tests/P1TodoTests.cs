using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;
using System.Collections.Concurrent;

namespace MiniInflux.Tests;

public class P1TodoTests : IDisposable
{
    private readonly string _testDir;

    public P1TodoTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p1_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public async Task RegexTagPredicate_UsesIndexPushdown()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", 1, "server01"),
            Point("cpu", 2, "server02"),
            Point("cpu", 3, "edge01")
        ]);

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT value FROM cpu WHERE host =~ /server.*/");

        Assert.True(outcome.Report.UsedRegexPushdown);
        Assert.True(outcome.Report.UsedSeriesIndexPushdown);
        Assert.Equal(2, outcome.Response.Results[0].Series![0].Values.Count);
    }

    [Fact]
    public async Task AggregateQuery_UsesBlockStatsPushdown()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", 1, "server01"),
            Point("cpu", 2, "server01"),
            Point("cpu", 3, "server01")
        ]);
        engine.FlushAll();

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT sum(value),count(value),max(value),mean(value) FROM cpu");

        Assert.True(outcome.Report.UsedAggregatePushdown);
        var row = Assert.Single(outcome.Response.Results[0].Series![0].Values);
        Assert.Equal(6.0, row[1]);
        Assert.Equal(3, row[2]);
        Assert.Equal(3.0, row[3]);
        Assert.Equal(2.0, row[4]);
    }

    [Fact]
    public async Task AggregateQuery_UsesBufferOnlyPushdown()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", 1, "server01"),
            Point("cpu", 2, "server01"),
            Point("cpu", 3, "server01")
        ]);

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT sum(value),count(value),max(value),mean(value) FROM cpu");

        Assert.True(outcome.Report.UsedAggregatePushdown);
        var row = Assert.Single(outcome.Response.Results[0].Series![0].Values);
        Assert.Equal(6.0, row[1]);
        Assert.Equal(3, row[2]);
        Assert.Equal(3.0, row[3]);
        Assert.Equal(2.0, row[4]);
    }

    [Fact]
    public async Task AggregateQuery_WithTagFilter_UsesBufferStatsPushdown()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", 1, "server01"),
            Point("cpu", 2, "server01"),
            Point("cpu", 10, "server02")
        ]);

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT mean(value),count(value) FROM cpu WHERE host='server01'");

        Assert.True(outcome.Report.UsedAggregatePushdown);
        Assert.Equal(2, outcome.Report.ScannedPoints);
        var row = Assert.Single(outcome.Response.Results[0].Series![0].Values);
        Assert.Equal(1.5, row[1]);
        Assert.Equal(2, row[2]);
    }

    [Fact]
    public async Task AggregateQuery_GroupByTag_UsesBlockStatsPushdown()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", 1, "server01"),
            Point("cpu", 2, "server01"),
            Point("cpu", 10, "server02"),
            Point("cpu", 20, "server02")
        ]);
        engine.FlushAll();

        var outcome = new QueryExecutor().ExecuteWithReport(
            engine,
            "testdb",
            "SELECT sum(value),count(value),max(value),mean(value) FROM cpu GROUP BY host");

        Assert.True(outcome.Report.UsedAggregatePushdown);
        var series = outcome.Response.Results[0].Series;
        Assert.NotNull(series);
        Assert.Equal(2, series.Count);

        var server01 = Assert.Single(series, s => s.Tags?["host"] == "server01");
        var server02 = Assert.Single(series, s => s.Tags?["host"] == "server02");

        var row01 = Assert.Single(server01.Values);
        Assert.Equal(3.0, row01[1]);
        Assert.Equal(2, row01[2]);
        Assert.Equal(2.0, row01[3]);
        Assert.Equal(1.5, row01[4]);

        var row02 = Assert.Single(server02.Values);
        Assert.Equal(30.0, row02[1]);
        Assert.Equal(2, row02[2]);
        Assert.Equal(20.0, row02[3]);
        Assert.Equal(15.0, row02[4]);
    }

    [Fact]
    public async Task AggregateQuery_GroupByTag_UsesBufferOnlyPushdown()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", 1, "server01"),
            Point("cpu", 2, "server01"),
            Point("cpu", 10, "server02"),
            Point("cpu", 20, "server02")
        ]);

        var outcome = new QueryExecutor().ExecuteWithReport(
            engine,
            "testdb",
            "SELECT sum(value),count(value),max(value),mean(value) FROM cpu GROUP BY host");

        Assert.True(outcome.Report.UsedAggregatePushdown);
        var series = outcome.Response.Results[0].Series;
        Assert.NotNull(series);
        Assert.Equal(2, series.Count);
    }

    [Fact]
    public async Task SampleFunction_ReturnsDeterministicSamples()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
            Enumerable.Range(1, 5).Select(i => Point("cpu", i, "server01")).ToList());

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT sample(value, 3) FROM cpu");

        var rows = response.Results[0].Series![0].Values;
        Assert.Equal(3, rows.Count);
        Assert.Equal(1.0, rows[0][1]);
        Assert.Equal(3.0, rows[1][1]);
        Assert.Equal(5.0, rows[2][1]);
    }

    [Fact]
    public async Task MetricsCollector_TracksQueryOutcomes()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", 1, "server01")]);

        var metrics = new MetricsCollector(engine);
        metrics.RecordQuery(new QueryExecutionReport { DurationMs = 3, RowsReturned = 2, ScannedPoints = 5 });
        metrics.RecordQuery(new QueryExecutionReport { DurationMs = 20, TimedOut = true, Error = "query timed out after 1 ms", ScannedPoints = 8 });
        var stats = metrics.CollectStats();

        Assert.Equal(2, stats.QueryTotal);
        Assert.Equal(1, stats.QueryTimeoutTotal);
        Assert.Equal(2, stats.QueryRowsReturnedTotal);
        Assert.Equal(13, stats.QueryScannedPointsTotal);
    }

    [Fact]
    public async Task Compactor_PromotesSegmentsAcrossLevels_AndTracksStats()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", 1, "server01")]);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", 2, "server01")]);
        engine.FlushAll();

        var shardManager = new ShardManager(engine.RootPath, engine.Meta);
        var compactor = new Compactor(engine.Meta, shardManager, engine.Tombstones, engine.Schema, maxL0Segments: 2, maxL1Segments: 1);

        Assert.Equal(1, compactor.CompactAll());
        Assert.Equal(1, compactor.CompactAll());

        var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
        Assert.Contains(shard.SegmentFiles, file => file.StartsWith("l2-", StringComparison.OrdinalIgnoreCase));

        var stats = compactor.GetStats();
        Assert.True(stats.TotalRuns >= 2);
        Assert.True(stats.TotalSegmentsMerged >= 3);
    }

    [Fact]
    public async Task SelectInto_WritesQueryResultToTargetMeasurement()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", 11, "server01"),
            Point("cpu", 22, "server02")
        ]);

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT value INTO cpu_copy FROM cpu");

        Assert.Null(outcome.Response.Results[0].Error);
        var copied = engine.ReadAllPoints("testdb", "autogen", "cpu_copy", null, null);
        Assert.Equal(2, copied.Count);
        Assert.Contains(copied, p => p.Tags["host"] == "server01" && p.Fields["value"].AsDouble() == 11);
        Assert.Contains(copied, p => p.Tags["host"] == "server02" && p.Fields["value"].AsDouble() == 22);
    }

    [Fact]
    public void Parser_ParsesSelectInto_AndRejectsUserManagementDdl()
    {
        var select = InfluxQlParser.Parse("SELECT value INTO archive.cpu FROM cpu");
        Assert.Equal(QueryKind.Select, select.Kind);
        Assert.Equal("archive.cpu", select.IntoTarget);
        Assert.Equal("cpu", select.Measurement);

        Assert.Throws<NotSupportedException>(() => InfluxQlParser.Parse("CREATE USER readonly WITH PASSWORD '123'"));
        Assert.Throws<NotSupportedException>(() => InfluxQlParser.Parse("ALTER USER readonly WITH PASSWORD '456'"));
        Assert.Throws<NotSupportedException>(() => InfluxQlParser.Parse("SET PASSWORD FOR readonly = '789'"));
        Assert.Throws<NotSupportedException>(() => InfluxQlParser.Parse("GRANT READ ON metrics TO readonly"));
        Assert.Throws<NotSupportedException>(() => InfluxQlParser.Parse("REVOKE READ ON metrics FROM readonly"));
        Assert.Throws<NotSupportedException>(() => InfluxQlParser.Parse("SHOW USERS"));
        Assert.Throws<NotSupportedException>(() => InfluxQlParser.Parse("SHOW GRANTS FOR readonly"));
        Assert.Throws<NotSupportedException>(() => InfluxQlParser.Parse("DROP USER readonly"));

        var qualified = InfluxQlParser.Parse("SELECT value INTO cpu_copy FROM metrics.archive.cpu");
        Assert.Equal("metrics", qualified.SourceDatabase);
        Assert.Equal("archive", qualified.SourceRpName);
        Assert.Equal("cpu", qualified.Measurement);
    }

    [Fact]
    public async Task BufferByteLimit_RejectsWrites()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000, maxBufferBytes: 1);
        await Assert.ThrowsAsync<MemoryLimitExceededException>(() => engine.WriteAsync("testdb", "autogen", [Point("cpu", 1, "server01")]));
    }

    [Fact]
    public async Task QueryMemoryLimit_RejectsLargeResult()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen", Enumerable.Range(1, 4).Select(i => Point("cpu", i, "server01")).ToList());

        var outcome = new QueryExecutor(maxQueryMemoryBytes: 32).ExecuteWithReport(engine, "testdb", "SELECT value FROM cpu");

        Assert.Contains("query memory limit exceeded", outcome.Response.Results[0].Error);
    }

    [Fact]
    public void BenchmarkRunner_ProducesSnapshot()
    {
        var result = BenchmarkRunner.Run(Path.Combine(_testDir, "bench"), new BenchmarkRunOptions(128, 2));

        Assert.Equal(128, result.PointsWritten);
        Assert.Equal(2, result.Concurrency);
        Assert.True(result.WriteThroughputPointsPerSec > 0);
        Assert.True(result.QueryLatencyMs >= 0);
        Assert.True(result.RecoveryMs >= 0);
        Assert.True(result.CompactionMs >= 0);
        Assert.True(result.CodecComparison.Legacy.TotalBytes > 0);
        Assert.True(result.CodecComparison.Gorilla.TotalBytes > 0);
        Assert.True(result.CodecComparison.Gorilla.TimestampBytes <= result.CodecComparison.Legacy.TimestampBytes);
        Assert.True(result.CodecComparison.Gorilla.EncodeMs >= 0);
        Assert.True(result.CodecComparison.Gorilla.DecodeMs >= 0);
        Assert.Equal("Gorilla", result.CodecComparison.Gorilla.TimestampCodec);
        Assert.Equal("Gorilla", result.CodecComparison.Gorilla.ValueCodec);
        Assert.Equal(3, result.FloatStrategyBenchmarks.Count);
        Assert.Contains(result.FloatStrategyBenchmarks, x => x.Workload == "smooth_linear");
        Assert.All(result.FloatStrategyBenchmarks, workload =>
        {
            Assert.Equal(5, workload.Strategies.Count);
            Assert.Contains(workload.Strategies, x => x.Strategy == "legacy_raw");
            Assert.Contains(workload.Strategies, x => x.Strategy == "legacy_brotli");
            Assert.Contains(workload.Strategies, x => x.Strategy == "gorilla_raw");
            Assert.Contains(workload.Strategies, x => x.Strategy == "gorilla_brotli");
            Assert.Contains(workload.Strategies, x => x.Strategy == "adaptive");
        });
        Assert.Contains(result.FloatStrategyBenchmarks, x => x.Workload == "repeating_plateau" && x.Strategies.Any(s => s.Strategy == "adaptive" && s.ValueCodec == "Gorilla" && s.ValueCompression == "None"));
        Assert.Contains(result.FloatStrategyBenchmarks, x => x.Workload == "smooth_linear" && x.Strategies.Any(s => s.Strategy == "adaptive" && s.ValueCodec == "Legacy" && s.ValueCompression == "Brotli"));
        Assert.Contains(result.FloatStrategyBenchmarks, x => x.Workload == "noisy_sine" && x.Strategies.Any(s => s.Strategy == "adaptive" && s.ValueCodec == "Legacy" && s.ValueCompression == "None"));
        Assert.Contains("mini_influx_benchmark_points_written", BenchmarkRunner.FormatPrometheus(result));
        Assert.Contains("mini_influx_benchmark_codec_gorilla_total_bytes", BenchmarkRunner.FormatPrometheus(result));
        Assert.Contains("mini_influx_benchmark_float_workload_best_size", BenchmarkRunner.FormatPrometheus(result));
        Assert.Contains("\"PointsWritten\":128", BenchmarkRunner.FormatJson(result));
        Assert.Contains("\"CodecComparison\"", BenchmarkRunner.FormatJson(result));
        Assert.Contains("\"FloatStrategyBenchmarks\"", BenchmarkRunner.FormatJson(result));
        Assert.Contains("codec_gorilla_total_bytes=", BenchmarkRunner.FormatText(result));
        Assert.Contains("float_workload_smooth_linear_best_size=", BenchmarkRunner.FormatText(result));
    }

    [Fact]
    public async Task FieldFilterQuery_ReadsPredicateFieldAndUsesMetadataSkip()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1);
        await engine.WriteAsync("testdb", "autogen",
        [
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue>
                {
                    ["value"] = FieldValue.FromDouble(5),
                    ["temp"] = FieldValue.FromDouble(50)
                },
                TimestampNs = 1_000_000_000
            }
        ]);
        await engine.WriteAsync("testdb", "autogen",
        [
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server02" },
                Fields = new Dictionary<string, FieldValue>
                {
                    ["value"] = FieldValue.FromDouble(20),
                    ["temp"] = FieldValue.FromDouble(80)
                },
                TimestampNs = 2_000_000_000
            }
        ]);

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT temp FROM cpu WHERE value > 10");

        var rows = outcome.Response.Results[0].Series![0].Values;
        Assert.Single(rows);
        Assert.Equal(80.0, rows[0][2]);
        Assert.Equal(1, outcome.Report.ScannedPoints);
    }

    [Fact]
    public async Task Compactor_CanTriggerByFileSizeEvenWhenCountThresholdNotReached()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", 1, "server01")]);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", 2, "server01")]);

        var shardManager = new ShardManager(engine.RootPath, engine.Meta);
        var compactor = new Compactor(
            engine.Meta,
            shardManager,
            engine.Tombstones,
            engine.Schema,
            maxL0Segments: 10,
            maxL1Segments: 10,
            maxL0Bytes: 1,
            maxL1Bytes: 1);

        Assert.Equal(1, compactor.CompactAll());

        var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
        Assert.Single(shard.SegmentFiles, file => file.StartsWith("l1-", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Compactor_OverlapAwarePolicy_MergesOverlappingLowerLevelSegments()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, compactionIntervalMs: 0);
        const long ts = 10_000_000_000L;

        await engine.WriteAsync("testdb", "autogen",
        [
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(10) },
                TimestampNs = ts
            }
        ]);
        await engine.WriteAsync("testdb", "autogen",
        [
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(20) },
                TimestampNs = ts + 1_000_000_000
            }
        ]);

        var shardManager = new ShardManager(engine.RootPath, engine.Meta);
        var compactor = new Compactor(engine.Meta, shardManager, engine.Tombstones, engine.Schema, maxL0Segments: 2, maxL1Segments: 99);
        Assert.Equal(1, compactor.CompactAll());

        await engine.WriteAsync("testdb", "autogen",
        [
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(99) },
                TimestampNs = ts
            }
        ]);
        await engine.WriteAsync("testdb", "autogen",
        [
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(30) },
                TimestampNs = ts + 2_000_000_000
            }
        ]);

        var overlapAware = new Compactor(engine.Meta, shardManager, engine.Tombstones, engine.Schema, maxL0Segments: 2, maxL1Segments: 99);
        Assert.Equal(1, overlapAware.CompactAll());

        var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
        Assert.Single(shard.SegmentFiles);
        Assert.DoesNotContain(shard.SegmentFiles, file => file.StartsWith("l0-", StringComparison.OrdinalIgnoreCase));
        Assert.Single(shard.SegmentFiles, file => file.StartsWith("l1-", StringComparison.OrdinalIgnoreCase));

        var points = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null)
            .OrderBy(p => p.TimestampNs)
            .ToList();
        Assert.Equal(3, points.Count);
        Assert.Equal(99.0, points[0].Fields["value"].AsDouble());
        Assert.Equal(20.0, points[1].Fields["value"].AsDouble());
        Assert.Equal(30.0, points[2].Fields["value"].AsDouble());
    }

    [Fact]
    public async Task Compactor_CompactAll_DrainsMultiLevelBacklogInSingleRun()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, compactionIntervalMs: 0);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", 1, "server01")]);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", 2, "server01")]);

        var compactor = new Compactor(
            engine.Meta,
            new ShardManager(engine.RootPath, engine.Meta),
            engine.Tombstones,
            engine.Schema,
            maxL0Segments: 2,
            maxL1Segments: 1);

        Assert.Equal(2, compactor.CompactAll());

        var shard = Assert.Single(engine.Meta.GetShards("testdb", "autogen"));
        Assert.Single(shard.SegmentFiles);
        Assert.Single(shard.SegmentFiles, file => file.StartsWith("l2-", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Query_ReadOrder_PrefersNewerL0PointsOverOlderCompactedLevels()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, compactionIntervalMs: 0);
        const long ts = 20_000_000_000L;

        await engine.WriteAsync("testdb", "autogen",
        [
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(10) },
                TimestampNs = ts
            }
        ]);
        await engine.WriteAsync("testdb", "autogen",
        [
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(11) },
                TimestampNs = ts + 1_000_000_000
            }
        ]);

        var compactor = new Compactor(
            engine.Meta,
            new ShardManager(engine.RootPath, engine.Meta),
            engine.Tombstones,
            engine.Schema,
            maxL0Segments: 2,
            maxL1Segments: 1);
        Assert.Equal(2, compactor.CompactAll());

        await engine.WriteAsync("testdb", "autogen",
        [
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(99) },
                TimestampNs = ts
            }
        ]);

        var points = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null)
            .OrderBy(p => p.TimestampNs)
            .ToList();
        Assert.Equal(2, points.Count);
        Assert.Equal(99.0, points[0].Fields["value"].AsDouble());
    }

    [Fact]
    public async Task SelectInto_PreservesNanosecondPrecision_AndSupportsQualifiedSourceRp()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        engine.CreateDatabase("testdb");
        engine.Meta.CreateRetentionPolicy("testdb", "archive", 0, false);
        const long timestampNs = 1_717_171_717_123_456_789L;
        await engine.WriteAsync("testdb", "archive",
        [
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "server01" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(42) },
                TimestampNs = timestampNs
            }
        ]);

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT value INTO cpu_copy FROM archive.cpu");

        Assert.Null(outcome.Response.Results[0].Error);
        var copied = Assert.Single(engine.ReadAllPoints("testdb", "autogen", "cpu_copy", null, null));
        Assert.Equal(timestampNs, copied.TimestampNs);
    }

    [Fact]
    public async Task MetricsCollector_TracksEstimatedQueryMemory()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", 1, "server01"), Point("cpu", 2, "server01")]);

        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", "SELECT value FROM cpu");
        var metrics = new MetricsCollector(engine);
        metrics.RecordQuery(outcome.Report);
        var stats = metrics.CollectStats();

        Assert.True(stats.QueryEstimatedInputBytesTotal > 0);
        Assert.True(stats.QueryEstimatedResultBytesTotal > 0);
        Assert.True(stats.LastQueryEstimatedPeakBytes >= stats.QueryEstimatedInputBytesTotal);
        Assert.True(stats.MemoryBufferBytes >= 0);
    }

    [Fact]
    public async Task ComplexGroupByQuery_TracksIntermediateMemory_AndRejectsTightLimit()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        var points = new List<Point>();
        for (var hostIndex = 0; hostIndex < 4; hostIndex++)
        {
            for (var bucketIndex = 0; bucketIndex < 6; bucketIndex++)
            {
                points.Add(new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { ["host"] = $"server{hostIndex:00}" },
                    Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(bucketIndex + hostIndex) },
                    TimestampNs = bucketIndex * 10_000_000_000L + hostIndex
                });
            }
        }

        await engine.WriteAsync("testdb", "autogen", points);

        const string query = "SELECT mean(value),max(value),count(value) FROM cpu GROUP BY time(10s),host";
        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", query);

        Assert.Null(outcome.Response.Results[0].Error);
        Assert.True(outcome.Report.EstimatedInputBytes > 0);
        Assert.True(outcome.Report.EstimatedResultBytes > 0);
        Assert.True(outcome.Report.PeakEstimatedMemoryBytes > outcome.Report.EstimatedInputBytes);

        var limited = new QueryExecutor(maxQueryMemoryBytes: outcome.Report.PeakEstimatedMemoryBytes - 1)
            .ExecuteWithReport(engine, "testdb", query);

        Assert.Contains("query memory limit exceeded", limited.Response.Results[0].Error);
    }

    [Fact]
    public async Task SubqueryAggregation_TracksMaterializationPeak_AndRejectsTightLimit()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        var points = new List<Point>();
        for (var hostIndex = 0; hostIndex < 3; hostIndex++)
        {
            for (var bucketIndex = 0; bucketIndex < 6; bucketIndex++)
            {
                points.Add(new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { ["host"] = $"server{hostIndex:00}" },
                    Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble((hostIndex + 1) * (bucketIndex + 1)) },
                    TimestampNs = bucketIndex * 10_000_000_000L + hostIndex
                });
            }
        }

        await engine.WriteAsync("testdb", "autogen", points);

        const string query = "SELECT mean(max_value) FROM (SELECT max(value) FROM cpu GROUP BY time(10s),host) GROUP BY host";
        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", query);

        Assert.Null(outcome.Response.Results[0].Error);
        Assert.True(outcome.Report.EstimatedInputBytes > 0);
        Assert.True(outcome.Report.PeakEstimatedMemoryBytes > outcome.Report.EstimatedInputBytes);

        var limited = new QueryExecutor(maxQueryMemoryBytes: outcome.Report.PeakEstimatedMemoryBytes - 1)
            .ExecuteWithReport(engine, "testdb", query);

        Assert.Contains("query memory limit exceeded", limited.Response.Results[0].Error);
    }

    [Fact]
    public async Task SelectInto_GroupByQuery_AccountsForWriteBackMaterialization()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        var points = new List<Point>();
        for (var hostIndex = 0; hostIndex < 3; hostIndex++)
        {
            for (var bucketIndex = 0; bucketIndex < 5; bucketIndex++)
            {
                points.Add(new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { ["host"] = $"server{hostIndex:00}" },
                    Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(hostIndex + bucketIndex + 1) },
                    TimestampNs = bucketIndex * 10_000_000_000L + hostIndex
                });
            }
        }

        await engine.WriteAsync("testdb", "autogen", points);

        const string query = "SELECT mean(value) INTO cpu_rollup FROM cpu GROUP BY time(10s),host";
        var outcome = new QueryExecutor().ExecuteWithReport(engine, "testdb", query);

        Assert.Null(outcome.Response.Results[0].Error);
        Assert.True(outcome.Report.PeakEstimatedMemoryBytes > outcome.Report.EstimatedInputBytes + outcome.Report.EstimatedResultBytes);

        var limited = new QueryExecutor(maxQueryMemoryBytes: outcome.Report.PeakEstimatedMemoryBytes - 1)
            .ExecuteWithReport(engine, "testdb", query);

        Assert.Contains("query memory limit exceeded", limited.Response.Results[0].Error);
    }

    [Fact]
    public async Task Compactor_CanRunConcurrentlyWithQueries_WithoutErrors()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, compactionIntervalMs: 0);
        var executor = new QueryExecutor();
        var compactor = new Compactor(
            engine.Meta,
            new ShardManager(engine.RootPath, engine.Meta),
            engine.Tombstones,
            engine.Schema,
            maxL0Segments: 2,
            maxL1Segments: 2);

        for (var i = 0; i < 12; i++)
            await engine.WriteAsync("testdb", "autogen", [PointWithTimestamp("cpu", i, "server01", i)]);

        var queryErrors = new ConcurrentQueue<string>();

        var writer = Task.Run(async () =>
        {
            for (var i = 12; i < 40; i++)
            {
                await engine.WriteAsync("testdb", "autogen", [PointWithTimestamp("cpu", i, "server01", i)]);
                if (i % 4 == 0)
                    await Task.Yield();
            }
        });

        var reader = Task.Run(() =>
        {
            for (var i = 0; i < 40; i++)
            {
                var outcome = executor.ExecuteWithReport(engine, "testdb", "SELECT max(value),count(value) FROM cpu");
                if (!string.IsNullOrWhiteSpace(outcome.Response.Results[0].Error))
                    queryErrors.Enqueue(outcome.Response.Results[0].Error!);
            }
        });

        var compacting = Task.Run(() =>
        {
            for (var i = 0; i < 20; i++)
            {
                compactor.CompactAll();
                Thread.Sleep(2);
            }
        });

        await Task.WhenAll(writer, reader, compacting);
        compactor.CompactAll();

        Assert.Empty(queryErrors);
        var points = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null)
            .OrderBy(p => p.TimestampNs)
            .ToList();
        Assert.Equal(40, points.Count);
        Assert.Equal(39.0, points[^1].Fields["value"].AsDouble());
    }

    [Fact]
    public async Task Compactor_Soak_WriteDeleteCompact_PreservesExpectedVisibleDataset()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, compactionIntervalMs: 0);
        var compactor = new Compactor(
            engine.Meta,
            new ShardManager(engine.RootPath, engine.Meta),
            engine.Tombstones,
            engine.Schema,
            maxL0Segments: 2,
            maxL1Segments: 2);
        var expected = new SortedDictionary<long, double>();

        for (var i = 0; i < 36; i++)
        {
            await engine.WriteAsync("testdb", "autogen", [PointWithTimestamp("cpu", i, "server01", i)]);
            expected[i] = i;

            if (i > 0 && i % 4 == 0)
            {
                var overwriteTs = i - 1;
                var overwriteValue = 1000 + i;
                await engine.WriteAsync("testdb", "autogen", [PointWithTimestamp("cpu", overwriteValue, "server01", overwriteTs)]);
                expected[overwriteTs] = overwriteValue;
            }

            if (i > 2 && i % 5 == 0)
            {
                var deleteTs = i - 2;
                engine.DeleteFromMeasurement("testdb", "cpu", deleteTs, deleteTs);
                expected.Remove(deleteTs);
            }

            if (i % 3 == 0)
                compactor.CompactAll();
        }

        compactor.CompactAll();
        compactor.CompactAll();

        var actual = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null)
            .OrderBy(p => p.TimestampNs)
            .ToList();

        Assert.Equal(expected.Count, actual.Count);
        Assert.Equal(expected.Keys, actual.Select(p => p.TimestampNs));
        foreach (var point in actual)
            Assert.Equal(expected[point.TimestampNs], point.Fields["value"].AsDouble());
    }

    [Fact]
    public async Task Compactor_CanRunConcurrentlyWithDeletesAndQueries_WithoutLeakingDeletedPoints()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1, compactionIntervalMs: 0);
        var executor = new QueryExecutor();
        var compactor = new Compactor(
            engine.Meta,
            new ShardManager(engine.RootPath, engine.Meta),
            engine.Tombstones,
            engine.Schema,
            maxL0Segments: 2,
            maxL1Segments: 2);

        for (var i = 0; i < 50; i++)
            await engine.WriteAsync("testdb", "autogen", [PointWithTimestamp("cpu", i, "server01", i)]);

        var deleted = Enumerable.Range(0, 50).Where(i => i % 2 == 0).Select(i => (long)i).ToHashSet();
        var queryErrors = new ConcurrentQueue<string>();

        var deleter = Task.Run(() =>
        {
            foreach (var timestamp in deleted)
            {
                engine.DeleteFromMeasurement("testdb", "cpu", timestamp, timestamp);
                Thread.Sleep(1);
            }
        });

        var reader = Task.Run(() =>
        {
            for (var i = 0; i < 50; i++)
            {
                var outcome = executor.ExecuteWithReport(engine, "testdb", "SELECT value FROM cpu");
                if (!string.IsNullOrWhiteSpace(outcome.Response.Results[0].Error))
                    queryErrors.Enqueue(outcome.Response.Results[0].Error!);
            }
        });

        var compacting = Task.Run(() =>
        {
            for (var i = 0; i < 25; i++)
            {
                compactor.CompactAll();
                Thread.Sleep(1);
            }
        });

        await Task.WhenAll(deleter, reader, compacting);
        compactor.CompactAll();
        compactor.CompactAll();

        Assert.Empty(queryErrors);
        var remaining = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null)
            .OrderBy(p => p.TimestampNs)
            .ToList();

        Assert.Equal(25, remaining.Count);
        Assert.DoesNotContain(remaining, point => deleted.Contains(point.TimestampNs));
        Assert.Equal(Enumerable.Range(0, 50).Where(i => i % 2 == 1).Select(i => (long)i), remaining.Select(p => p.TimestampNs));
    }

    private static Point Point(string measurement, double value, string host) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000 + (long)(value * 1_000_000)
    };

    private static Point PointWithTimestamp(string measurement, double value, string host, long timestampNs) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = timestampNs
    };
}
