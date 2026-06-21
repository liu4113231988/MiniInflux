using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

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
    public void Parser_ParsesSelectIntoAndAuthDdl()
    {
        var select = InfluxQlParser.Parse("SELECT value INTO archive.cpu FROM cpu");
        Assert.Equal(QueryKind.Select, select.Kind);
        Assert.Equal("archive.cpu", select.IntoTarget);
        Assert.Equal("cpu", select.Measurement);

        var create = InfluxQlParser.Parse("CREATE USER readonly WITH PASSWORD '123'");
        Assert.Equal(QueryKind.CreateUser, create.Kind);
        Assert.Equal("readonly", create.UserName);
        Assert.Equal("123", create.Password);

        var grant = InfluxQlParser.Parse("GRANT READ ON metrics TO readonly");
        Assert.Equal(QueryKind.GrantPrivilege, grant.Kind);
        Assert.Equal("metrics", grant.Grant!.Database);

        var revoke = InfluxQlParser.Parse("REVOKE READ ON metrics FROM readonly");
        Assert.Equal(QueryKind.RevokePrivilege, revoke.Kind);

        var showGrants = InfluxQlParser.Parse("SHOW GRANTS FOR readonly");
        Assert.Equal(QueryKind.ShowGrants, showGrants.Kind);
        Assert.Equal("readonly", showGrants.UserName);

        var qualified = InfluxQlParser.Parse("SELECT value INTO cpu_copy FROM metrics.archive.cpu");
        Assert.Equal("metrics", qualified.SourceDatabase);
        Assert.Equal("archive", qualified.SourceRpName);
        Assert.Equal("cpu", qualified.Measurement);
    }

    [Fact]
    public void AuthStore_SupportsCrudAndGrant()
    {
        var authRoot = Path.Combine(_testDir, "auth");
        var store = new AuthStore(authRoot);

        store.CreateUser("readonly", "123", false);
        store.Grant("readonly", "metrics", "READ");

        Assert.True(store.Validate("readonly", "123", out var identity));
        Assert.NotNull(identity);
        Assert.True(store.IsAuthorized(identity, "metrics", AuthPermission.Read));
        Assert.False(store.IsAuthorized(identity, "metrics", AuthPermission.Write));

        var users = store.ListUsers();
        Assert.Single(users);
        Assert.Equal("readonly", users[0].UserName);
        Assert.StartsWith("sha256:", users[0].Password, StringComparison.Ordinal);

        store.Revoke("readonly", "metrics", "READ");
        Assert.False(store.IsAuthorized(store.Find("readonly"), "metrics", AuthPermission.Read));

        store.DropUser("readonly");
        Assert.Empty(store.ListUsers());
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

    private static Point Point(string measurement, double value, string host) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000 + (long)(value * 1_000_000)
    };
}
