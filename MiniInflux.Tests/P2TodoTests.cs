using MiniInflux.Net10.Model;
using Microsoft.Extensions.Logging;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class P2TodoTests : IDisposable
{
    private readonly string _testDir;

    public P2TodoTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p2_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public async Task Subquery_AllowsOuterAggregationOverInnerTimeBuckets()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "server01", 10, 1_000_000_000L),
            Point("cpu", "server01", 20, 5_000_000_000L),
            Point("cpu", "server01", 30, 11_000_000_000L),
            Point("cpu", "server01", 50, 15_000_000_000L)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(
            engine,
            "testdb",
            "SELECT mean(max_value) FROM (SELECT max(value) FROM cpu GROUP BY time(10s)) GROUP BY time(20s)");

        var row = Assert.Single(Assert.Single(response.Results[0].Series!).Values);
        Assert.Equal(35.0, Convert.ToDouble(row[1]));
    }

    [Fact]
    public async Task Subquery_PreservesGroupedTagsForOuterAggregation()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "server01", 10, 1_000_000_000L),
            Point("cpu", "server01", 40, 12_000_000_000L),
            Point("cpu", "server02", 7, 2_000_000_000L),
            Point("cpu", "server02", 9, 13_000_000_000L)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(
            engine,
            "testdb",
            "SELECT max(max_value) FROM (SELECT max(value) FROM cpu GROUP BY time(10s),host) GROUP BY host");

        var series = response.Results[0].Series!;
        Assert.Equal(2, series.Count);
        Assert.Contains(series, s => s.Tags!["host"] == "server01" && Convert.ToDouble(Assert.Single(s.Values)[1]) == 40.0);
        Assert.Contains(series, s => s.Tags!["host"] == "server02" && Convert.ToDouble(Assert.Single(s.Values)[1]) == 9.0);
    }

    [Fact]
    public async Task ContinuousQuery_Ddl_CanCreateShowAndDrop()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        var executor = new QueryExecutor();

        var create = await executor.ExecuteAsync(
            engine,
            "testdb",
            "CREATE CONTINUOUS QUERY cq_cpu ON testdb BEGIN SELECT mean(value) INTO cpu_rollup FROM cpu GROUP BY time(1s),host END");

        Assert.Null(create.Results[0].Error);

        var show = await executor.ExecuteAsync(engine, "testdb", "SHOW CONTINUOUS QUERIES");
        var row = Assert.Single(Assert.Single(show.Results[0].Series!).Values);
        Assert.Equal("testdb", row[0]);
        Assert.Equal("cq_cpu", row[1]);
        Assert.Null(row[4]);
        Assert.Null(row[5]);

        var drop = await executor.ExecuteAsync(engine, "testdb", "DROP CONTINUOUS QUERY cq_cpu ON testdb");
        Assert.Null(drop.Results[0].Error);

        var showAfterDrop = await executor.ExecuteAsync(engine, "testdb", "SHOW CONTINUOUS QUERIES");
        Assert.Empty(Assert.Single(showAfterDrop.Results[0].Series!).Values);
    }

    [Fact]
    public async Task ContinuousQueryRunner_ExecutesDueBucket_AndPersistsProgress()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        const long everyNs = 1_000_000_000L;
        var nowNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var bucketStart = (nowNs / everyNs) * everyNs - everyNs;
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "server01", 10, bucketStart + 100_000_000L),
            Point("cpu", "server01", 20, bucketStart + 600_000_000L)
        ]);

        var executor = new QueryExecutor();
        await executor.ExecuteAsync(
            engine,
            "testdb",
            "CREATE CONTINUOUS QUERY cq_cpu ON testdb BEGIN SELECT mean(value) INTO cpu_rollup FROM cpu GROUP BY time(1s),host END");

        using var loggerFactory = LoggerFactory.Create(builder => { });
        var metrics = new MetricsCollector(engine);
        var options = new MiniInfluxOptions
        {
            ContinuousQuery = new ContinuousQueryOptions
            {
                Enabled = true,
                CheckIntervalMs = 1000,
                MaxCatchUpRunsPerCycle = 4
            }
        };
        var runner = new ContinuousQueryRunner(engine, executor, options, metrics, loggerFactory.CreateLogger<ContinuousQueryRunner>());

        var executed = await runner.ExecuteDueQueriesAsync();

        Assert.Equal(1, executed);
        var rollup = Assert.Single(engine.ReadAllPoints("testdb", "autogen", "cpu_rollup", null, null));
        Assert.Equal(bucketStart, rollup.TimestampNs);
        Assert.Equal("server01", rollup.Tags["host"]);
        Assert.Equal(15.0, rollup.Fields["mean_value"].AsDouble());
        Assert.Equal(bucketStart, engine.Meta.GetContinuousQuery("testdb", "cq_cpu")!.LastCompletedBucketStartNs);

        var secondRun = await runner.ExecuteDueQueriesAsync();
        Assert.Equal(0, secondRun);
        Assert.Single(engine.ReadAllPoints("testdb", "autogen", "cpu_rollup", null, null));

        var reloaded = new Manifest(_testDir);
        Assert.Equal(bucketStart, reloaded.GetContinuousQuery("testdb", "cq_cpu")!.LastCompletedBucketStartNs);
    }

    [Fact]
    public async Task ContinuousQueryRunner_UsesResampleForWindow_ForInitialBackfill()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        const long everyNs = 10_000_000_000L;
        var nowNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var latestClosedBucketStart = (nowNs / everyNs) * everyNs - everyNs;
        var oldestBucketStart = latestClosedBucketStart - 2 * everyNs;
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "server01", 10, oldestBucketStart + 1_000_000_000L),
            Point("cpu", "server01", 20, oldestBucketStart + 6_000_000_000L),
            Point("cpu", "server01", 30, oldestBucketStart + everyNs + 1_000_000_000L),
            Point("cpu", "server01", 40, latestClosedBucketStart + 1_000_000_000L)
        ]);

        var executor = new QueryExecutor();
        await executor.ExecuteAsync(
            engine,
            "testdb",
            "CREATE CONTINUOUS QUERY cq_cpu ON testdb RESAMPLE EVERY 10s FOR 30s BEGIN SELECT mean(value) INTO cpu_rollup FROM cpu GROUP BY time(10s),host END");

        using var loggerFactory = LoggerFactory.Create(builder => { });
        var metrics = new MetricsCollector(engine);
        var runner = new ContinuousQueryRunner(
            engine,
            executor,
            new MiniInfluxOptions
            {
                ContinuousQuery = new ContinuousQueryOptions
                {
                    Enabled = true,
                    MaxCatchUpRunsPerCycle = 8
                }
            },
            metrics,
            loggerFactory.CreateLogger<ContinuousQueryRunner>());

        var executed = await runner.ExecuteDueQueriesAsync();

        Assert.Equal(3, executed);
        var rollups = engine.ReadAllPoints("testdb", "autogen", "cpu_rollup", null, null)
            .OrderBy(p => p.TimestampNs)
            .ToList();
        Assert.Equal(3, rollups.Count);
        Assert.Equal(oldestBucketStart, rollups[0].TimestampNs);
        Assert.Equal(15.0, rollups[0].Fields["mean_value"].AsDouble());
        Assert.Equal(latestClosedBucketStart, rollups[^1].TimestampNs);
        Assert.Equal(40.0, rollups[^1].Fields["mean_value"].AsDouble());
    }

    [Fact]
    public async Task ContinuousQueryRunner_UsesConfiguredInitialBackfillWindow_WhenForIsAbsent()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        const long everyNs = 10_000_000_000L;
        var nowNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var latestClosedBucketStart = (nowNs / everyNs) * everyNs - everyNs;
        var previousBucketStart = latestClosedBucketStart - everyNs;
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "server01", 11, previousBucketStart + 1_000_000_000L),
            Point("cpu", "server01", 22, latestClosedBucketStart + 1_000_000_000L)
        ]);

        var executor = new QueryExecutor();
        await executor.ExecuteAsync(
            engine,
            "testdb",
            "CREATE CONTINUOUS QUERY cq_cpu ON testdb BEGIN SELECT mean(value) INTO cpu_rollup FROM cpu GROUP BY time(10s),host END");

        using var loggerFactory = LoggerFactory.Create(builder => { });
        var metrics = new MetricsCollector(engine);
        var runner = new ContinuousQueryRunner(
            engine,
            executor,
            new MiniInfluxOptions
            {
                ContinuousQuery = new ContinuousQueryOptions
                {
                    Enabled = true,
                    MaxCatchUpRunsPerCycle = 8,
                    InitialBackfillDuration = "20s",
                    InitialBackfillDurationNs = 20_000_000_000L
                }
            },
            metrics,
            loggerFactory.CreateLogger<ContinuousQueryRunner>());

        var executed = await runner.ExecuteDueQueriesAsync();

        Assert.Equal(2, executed);
        var rollups = engine.ReadAllPoints("testdb", "autogen", "cpu_rollup", null, null)
            .OrderBy(p => p.TimestampNs)
            .ToList();
        Assert.Equal(2, rollups.Count);
        Assert.Equal(previousBucketStart, rollups[0].TimestampNs);
        Assert.Equal(latestClosedBucketStart, rollups[1].TimestampNs);
    }

    [Fact]
    public async Task ContinuousQueryRunner_RecomputesRecentBuckets_WhenConfigured()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        const long everyNs = 10_000_000_000L;
        var nowNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var latestClosedBucketStart = (nowNs / everyNs) * everyNs - everyNs;
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "server01", 10, latestClosedBucketStart + 1_000_000_000L)
        ]);

        var executor = new QueryExecutor();
        await executor.ExecuteAsync(
            engine,
            "testdb",
            "CREATE CONTINUOUS QUERY cq_cpu ON testdb BEGIN SELECT mean(value) INTO cpu_rollup FROM cpu GROUP BY time(10s),host END");

        using var loggerFactory = LoggerFactory.Create(builder => { });
        var metrics = new MetricsCollector(engine);
        var runner = new ContinuousQueryRunner(
            engine,
            executor,
            new MiniInfluxOptions
            {
                ContinuousQuery = new ContinuousQueryOptions
                {
                    Enabled = true,
                    MaxCatchUpRunsPerCycle = 8
                }
            },
            metrics,
            loggerFactory.CreateLogger<ContinuousQueryRunner>());

        Assert.Equal(1, await runner.ExecuteDueQueriesAsync());
        var first = Assert.Single(engine.ReadAllPoints("testdb", "autogen", "cpu_rollup", null, null));
        Assert.Equal(10.0, first.Fields["mean_value"].AsDouble());

        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "server01", 30, latestClosedBucketStart + 5_000_000_000L)
        ]);

        var recomputeRunner = new ContinuousQueryRunner(
            engine,
            executor,
            new MiniInfluxOptions
            {
                ContinuousQuery = new ContinuousQueryOptions
                {
                    Enabled = true,
                    MaxCatchUpRunsPerCycle = 8,
                    RecomputeRecentBuckets = 1
                }
            },
            metrics,
            loggerFactory.CreateLogger<ContinuousQueryRunner>());

        Assert.Equal(1, await recomputeRunner.ExecuteDueQueriesAsync());
        var recomputed = Assert.Single(engine.ReadAllPoints("testdb", "autogen", "cpu_rollup", null, null));
        Assert.Equal(20.0, recomputed.Fields["mean_value"].AsDouble());
    }

    [Fact]
    public async Task MetricsCollector_TracksContinuousQueryRuns_AndRecomputes()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        const long everyNs = 10_000_000_000L;
        var nowNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var latestClosedBucketStart = (nowNs / everyNs) * everyNs - everyNs;
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "server01", 5, latestClosedBucketStart + 1_000_000_000L)
        ]);

        var executor = new QueryExecutor();
        await executor.ExecuteAsync(
            engine,
            "testdb",
            "CREATE CONTINUOUS QUERY cq_cpu ON testdb BEGIN SELECT mean(value) INTO cpu_rollup FROM cpu GROUP BY time(10s),host END");

        using var loggerFactory = LoggerFactory.Create(builder => { });
        var metrics = new MetricsCollector(engine);
        var runner = new ContinuousQueryRunner(
            engine,
            executor,
            new MiniInfluxOptions
            {
                ContinuousQuery = new ContinuousQueryOptions
                {
                    Enabled = true,
                    MaxCatchUpRunsPerCycle = 8,
                    RecomputeRecentBuckets = 1
                }
            },
            metrics,
            loggerFactory.CreateLogger<ContinuousQueryRunner>());

        await runner.ExecuteDueQueriesAsync();
        await runner.ExecuteDueQueriesAsync();

        var stats = metrics.CollectStats();
        Assert.True(stats.ContinuousQueryRunsTotal >= 2);
        Assert.True(stats.ContinuousQueryBucketsTotal >= 2);
        Assert.True(stats.ContinuousQueryRecomputeBucketsTotal >= 1);
        Assert.True(stats.LastContinuousQueryDurationMs >= 0);
        var metric = Assert.Single(stats.ContinuousQueryMetrics);
        Assert.Equal("testdb", metric.Database);
        Assert.Equal("cq_cpu", metric.Name);
        Assert.True(metric.RunsTotal >= 2);
        Assert.True(metric.RecomputeBucketsTotal >= 1);
        Assert.Contains("mini_influx_continuous_query_runs_total", metrics.FormatPrometheus());
        Assert.Contains("mini_influx_continuous_query_recompute_buckets_total", metrics.FormatPrometheus());
        Assert.Contains("mini_influx_continuous_query_run_total{db=\"testdb\",name=\"cq_cpu\"}", metrics.FormatPrometheus());
    }

    [Fact]
    public async Task ContinuousQuery_Show_IncludesPerQueryRecomputeStrategy()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        var executor = new QueryExecutor();

        await executor.ExecuteAsync(
            engine,
            "testdb",
            "CREATE CONTINUOUS QUERY cq_cpu ON testdb RESAMPLE EVERY 10s RECOMPUTE 2 BEGIN SELECT mean(value) INTO cpu_rollup FROM cpu GROUP BY time(10s),host END");

        var show = await executor.ExecuteAsync(engine, "testdb", "SHOW CONTINUOUS QUERIES");
        var row = Assert.Single(Assert.Single(show.Results[0].Series!).Values);
        Assert.Equal(2, row[5]);
    }

    private static Point Point(string measurement, string host, double value, long timestampNs) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = timestampNs
    };
}
