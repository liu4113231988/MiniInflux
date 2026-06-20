using System.Text;
using System.Text.Json;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Query;

namespace MiniInflux.Net10.Storage;

public sealed record BenchmarkRunOptions(int PointCount = 10_000, int Concurrency = 1);

public sealed record BenchmarkRunResult(
    int PointsWritten,
    int Concurrency,
    double WriteThroughputPointsPerSec,
    double QueryLatencyMs,
    double RecoveryMs,
    double CompactionMs,
    long BufferedPointsAfterWrite);

public static class BenchmarkRunner
{
    public static BenchmarkRunResult Run(string dataPath, BenchmarkRunOptions? options = null)
    {
        var effective = options ?? new BenchmarkRunOptions();
        var pointCount = Math.Max(1, effective.PointCount);
        var concurrency = Math.Max(1, effective.Concurrency);

        Directory.CreateDirectory(dataPath);
        var writeWatch = System.Diagnostics.Stopwatch.StartNew();
        long buffered;
        double queryLatencyMs;
        double compactionMs;
        using (var engine = new TsdbEngine(dataPath, flushThreshold: Math.Max(1, pointCount / 4)))
        {
            engine.CreateDatabase("bench");

            var partitions = Enumerable.Range(0, concurrency)
                .Select(worker => Task.Run(() =>
                {
                    var points = CreatePoints(worker, concurrency, pointCount);
                    engine.WriteAsync("bench", "autogen", points).GetAwaiter().GetResult();
                }))
                .ToArray();
            Task.WaitAll(partitions);
            writeWatch.Stop();

            var queryExecutor = new QueryExecutor();
            var queryWatch = System.Diagnostics.Stopwatch.StartNew();
            queryExecutor.ExecuteWithReport(engine, "bench", "SELECT mean(value),count(value) FROM cpu");
            queryWatch.Stop();
            queryLatencyMs = queryWatch.Elapsed.TotalMilliseconds;

            buffered = engine.GetBufferedPointCount();
            var compactionWatch = System.Diagnostics.Stopwatch.StartNew();
            engine.FlushAll();
            var compactor = new Compactor(engine.Meta, new ShardManager(engine.RootPath, engine.Meta), engine.Tombstones, engine.Schema, maxL0Segments: 2, maxL1Segments: 1);
            compactor.CompactAll();
            compactionWatch.Stop();
            compactionMs = compactionWatch.Elapsed.TotalMilliseconds;
        }

        var recoveryWatch = System.Diagnostics.Stopwatch.StartNew();
        using var recovered = new TsdbEngine(dataPath, flushThreshold: Math.Max(1, pointCount / 4));
        recovered.Recover();
        recoveryWatch.Stop();

        return new BenchmarkRunResult(
            pointCount,
            concurrency,
            pointCount / Math.Max(writeWatch.Elapsed.TotalSeconds, 0.001),
            queryLatencyMs,
            recoveryWatch.Elapsed.TotalMilliseconds,
            compactionMs,
            buffered);
    }

    public static string FormatText(BenchmarkRunResult result) =>
        string.Join(Environment.NewLine,
        [
            $"points_written={result.PointsWritten}",
            $"concurrency={result.Concurrency}",
            $"write_throughput_points_per_sec={result.WriteThroughputPointsPerSec:F2}",
            $"query_latency_ms={result.QueryLatencyMs:F2}",
            $"recovery_ms={result.RecoveryMs:F2}",
            $"compaction_ms={result.CompactionMs:F2}",
            $"buffered_points_after_write={result.BufferedPointsAfterWrite}"
        ]);

    public static string FormatPrometheus(BenchmarkRunResult result)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"mini_influx_benchmark_points_written {result.PointsWritten}");
        sb.AppendLine($"mini_influx_benchmark_concurrency {result.Concurrency}");
        sb.AppendLine($"mini_influx_benchmark_write_throughput_points_per_sec {result.WriteThroughputPointsPerSec:F2}");
        sb.AppendLine($"mini_influx_benchmark_query_latency_ms {result.QueryLatencyMs:F2}");
        sb.AppendLine($"mini_influx_benchmark_recovery_ms {result.RecoveryMs:F2}");
        sb.AppendLine($"mini_influx_benchmark_compaction_ms {result.CompactionMs:F2}");
        sb.AppendLine($"mini_influx_benchmark_buffered_points_after_write {result.BufferedPointsAfterWrite}");
        return sb.ToString();
    }

    public static string FormatJson(BenchmarkRunResult result) => JsonSerializer.Serialize(result, AppJsonContext.Default.BenchmarkRunResult);

    private static List<Point> CreatePoints(int worker, int concurrency, int totalPointCount)
    {
        var points = new List<Point>();
        for (int index = worker; index < totalPointCount; index += concurrency)
            points.Add(CreatePoint(index));
        return points;
    }

    private static Point CreatePoint(int index) => new()
    {
        Measurement = "cpu",
        Tags = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["host"] = $"server{index % 16:00}",
            ["region"] = index % 2 == 0 ? "cn" : "us"
        },
        Fields = new Dictionary<string, FieldValue>(StringComparer.Ordinal)
        {
            ["value"] = FieldValue.FromDouble(index),
            ["load"] = FieldValue.FromDouble(index % 100)
        },
        TimestampNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000 + index
    };
}
