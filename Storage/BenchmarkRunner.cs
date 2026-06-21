using System.Text;
using System.Text.Json;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Query;

namespace MiniInflux.Net10.Storage;

public sealed record BenchmarkRunOptions(int PointCount = 10_000, int Concurrency = 1);

public sealed record CodecBenchmarkResult(
    string Strategy,
    int TimestampBytes,
    int ValueBytes,
    int TotalBytes,
    double EncodeMs,
    double DecodeMs,
    string TimestampCodec,
    string TimestampCompression,
    string ValueCodec,
    string ValueCompression);

public sealed record CodecComparisonBenchmark(
    int PointCount,
    CodecBenchmarkResult Legacy,
    CodecBenchmarkResult Gorilla);

public sealed record FloatStrategyBenchmarkResult(
    string Workload,
    string Strategy,
    int ValueBytes,
    double EncodeMs,
    double DecodeMs,
    string ValueCodec,
    string ValueCompression);

public sealed record FloatWorkloadBenchmark(
    string Workload,
    int PointCount,
    FloatStrategyBenchmarkResult BestBySize,
    FloatStrategyBenchmarkResult BestByEncode,
    FloatStrategyBenchmarkResult BestByDecode,
    IReadOnlyList<FloatStrategyBenchmarkResult> Strategies);

public sealed record BenchmarkRunResult(
    int PointsWritten,
    int Concurrency,
    double WriteThroughputPointsPerSec,
    double QueryLatencyMs,
    double RecoveryMs,
    double CompactionMs,
    long BufferedPointsAfterWrite,
    CodecComparisonBenchmark CodecComparison,
    IReadOnlyList<FloatWorkloadBenchmark> FloatStrategyBenchmarks);

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

        var codecComparison = RunCodecComparisonBenchmark(pointCount);
        var floatStrategyBenchmarks = RunFloatStrategyBenchmarks(pointCount);

        return new BenchmarkRunResult(
            pointCount,
            concurrency,
            pointCount / Math.Max(writeWatch.Elapsed.TotalSeconds, 0.001),
            queryLatencyMs,
            recoveryWatch.Elapsed.TotalMilliseconds,
            compactionMs,
            buffered,
            codecComparison,
            floatStrategyBenchmarks);
    }

    public static string FormatText(BenchmarkRunResult result)
    {
        var lines = new List<string>
        {
            $"points_written={result.PointsWritten}",
            $"concurrency={result.Concurrency}",
            $"write_throughput_points_per_sec={result.WriteThroughputPointsPerSec:F2}",
            $"query_latency_ms={result.QueryLatencyMs:F2}",
            $"recovery_ms={result.RecoveryMs:F2}",
            $"compaction_ms={result.CompactionMs:F2}",
            $"buffered_points_after_write={result.BufferedPointsAfterWrite}",
            $"codec_compare_point_count={result.CodecComparison.PointCount}",
            $"codec_legacy_total_bytes={result.CodecComparison.Legacy.TotalBytes}",
            $"codec_legacy_encode_ms={result.CodecComparison.Legacy.EncodeMs:F2}",
            $"codec_legacy_decode_ms={result.CodecComparison.Legacy.DecodeMs:F2}",
            $"codec_gorilla_total_bytes={result.CodecComparison.Gorilla.TotalBytes}",
            $"codec_gorilla_encode_ms={result.CodecComparison.Gorilla.EncodeMs:F2}",
            $"codec_gorilla_decode_ms={result.CodecComparison.Gorilla.DecodeMs:F2}"
        };

        foreach (var workload in result.FloatStrategyBenchmarks)
        {
            lines.Add($"float_workload_{workload.Workload}_best_size={workload.BestBySize.Strategy}");
            lines.Add($"float_workload_{workload.Workload}_best_encode={workload.BestByEncode.Strategy}");
            lines.Add($"float_workload_{workload.Workload}_best_decode={workload.BestByDecode.Strategy}");
        }

        return string.Join(Environment.NewLine, lines);
    }

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
        sb.AppendLine($"mini_influx_benchmark_codec_compare_point_count {result.CodecComparison.PointCount}");
        sb.AppendLine($"mini_influx_benchmark_codec_legacy_total_bytes {result.CodecComparison.Legacy.TotalBytes}");
        sb.AppendLine($"mini_influx_benchmark_codec_legacy_encode_ms {result.CodecComparison.Legacy.EncodeMs:F2}");
        sb.AppendLine($"mini_influx_benchmark_codec_legacy_decode_ms {result.CodecComparison.Legacy.DecodeMs:F2}");
        sb.AppendLine($"mini_influx_benchmark_codec_gorilla_total_bytes {result.CodecComparison.Gorilla.TotalBytes}");
        sb.AppendLine($"mini_influx_benchmark_codec_gorilla_encode_ms {result.CodecComparison.Gorilla.EncodeMs:F2}");
        sb.AppendLine($"mini_influx_benchmark_codec_gorilla_decode_ms {result.CodecComparison.Gorilla.DecodeMs:F2}");

        foreach (var workload in result.FloatStrategyBenchmarks)
        {
            sb.AppendLine($"mini_influx_benchmark_float_workload_best_size{{workload=\"{workload.Workload}\",strategy=\"{workload.BestBySize.Strategy}\"}} 1");
            sb.AppendLine($"mini_influx_benchmark_float_workload_best_encode{{workload=\"{workload.Workload}\",strategy=\"{workload.BestByEncode.Strategy}\"}} 1");
            sb.AppendLine($"mini_influx_benchmark_float_workload_best_decode{{workload=\"{workload.Workload}\",strategy=\"{workload.BestByDecode.Strategy}\"}} 1");
        }
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

    private static CodecComparisonBenchmark RunCodecComparisonBenchmark(int requestedPointCount)
    {
        var pointCount = Math.Max(128, requestedPointCount);
        var timestamps = Enumerable.Range(0, pointCount)
            .Select(i => 1_717_171_717_000_000_000L + i * 1_000_000_000L)
            .ToArray();
        var values = Enumerable.Range(0, pointCount)
            .Select(i => FieldValue.FromDouble(1000.0 + i * 0.25))
            .ToArray();

        var legacy = BenchmarkCodecStrategy(
            "legacy",
            timestamps,
            values,
            TimestampCodecKind.DeltaOfDeltaVarint,
            ValueCodecKind.Legacy);

        var gorilla = BenchmarkCodecStrategy(
            "gorilla",
            timestamps,
            values,
            TimestampCodecKind.Gorilla,
            ValueCodecKind.Gorilla);

        return new CodecComparisonBenchmark(pointCount, legacy, gorilla);
    }

    private static CodecBenchmarkResult BenchmarkCodecStrategy(
        string strategy,
        IReadOnlyList<long> timestamps,
        IReadOnlyList<FieldValue> values,
        TimestampCodecKind timestampCodec,
        ValueCodecKind valueCodec)
    {
        TimestampEncodedBlock timestampBlock = default;
        ValueEncodedBlock valueBlock = default;

        var encodeWatch = System.Diagnostics.Stopwatch.StartNew();
        for (int i = 0; i < 5; i++)
        {
            timestampBlock = CompressionCodec.EncodeTimestampsBlock(timestampCodec, timestamps);
            valueBlock = CompressionCodec.EncodeValuesBlock(FieldKind.Float, valueCodec, values);
        }
        encodeWatch.Stop();

        var decodeWatch = System.Diagnostics.Stopwatch.StartNew();
        for (int i = 0; i < 5; i++)
        {
            _ = CompressionCodec.DecodeTimestamps(timestampBlock.Codec, timestampBlock.Compression, timestampBlock.Payload);
            _ = CompressionCodec.DecodeValues(FieldKind.Float, valueBlock.Codec, valueBlock.Compression, valueBlock.Payload);
        }
        decodeWatch.Stop();

        return new CodecBenchmarkResult(
            strategy,
            timestampBlock.Payload.Length,
            valueBlock.Payload.Length,
            timestampBlock.Payload.Length + valueBlock.Payload.Length,
            encodeWatch.Elapsed.TotalMilliseconds / 5.0,
            decodeWatch.Elapsed.TotalMilliseconds / 5.0,
            timestampBlock.Codec.ToString(),
            timestampBlock.Compression.ToString(),
            valueBlock.Codec.ToString(),
            valueBlock.Compression.ToString());
    }

    private static IReadOnlyList<FloatWorkloadBenchmark> RunFloatStrategyBenchmarks(int requestedPointCount)
    {
        var pointCount = Math.Max(256, requestedPointCount);
        var workloads = new[]
        {
            BuildFloatWorkload("smooth_linear", Enumerable.Range(0, pointCount).Select(i => 1000.0 + i * 0.25)),
            BuildFloatWorkload("repeating_plateau", Enumerable.Range(0, pointCount).Select(i => 1000.0 + (i / 16) * 0.5)),
            BuildFloatWorkload("noisy_sine", Enumerable.Range(0, pointCount).Select(i => 1000.0 + Math.Sin(i / 8.0) * 50.0 + ((i % 7) - 3) * 0.137))
        };

        return workloads;
    }

    private static FloatWorkloadBenchmark BuildFloatWorkload(string workload, IEnumerable<double> source)
    {
        var values = source.Select(FieldValue.FromDouble).ToArray();
        var strategies = new[]
        {
            BenchmarkFloatStrategy(workload, "legacy_raw", values, ValueCodecKind.Legacy, BlockCompressionKind.None),
            BenchmarkFloatStrategy(workload, "legacy_brotli", values, ValueCodecKind.Legacy, BlockCompressionKind.Brotli),
            BenchmarkFloatStrategy(workload, "gorilla_raw", values, ValueCodecKind.Gorilla, BlockCompressionKind.None),
            BenchmarkFloatStrategy(workload, "gorilla_brotli", values, ValueCodecKind.Gorilla, BlockCompressionKind.Brotli),
            BenchmarkAdaptiveFloatStrategy(workload, values)
        };

        return new FloatWorkloadBenchmark(
            workload,
            values.Length,
            strategies.OrderBy(x => x.ValueBytes).ThenBy(x => x.EncodeMs).First(),
            strategies.OrderBy(x => x.EncodeMs).ThenBy(x => x.ValueBytes).First(),
            strategies.OrderBy(x => x.DecodeMs).ThenBy(x => x.ValueBytes).First(),
            strategies);
    }

    private static FloatStrategyBenchmarkResult BenchmarkFloatStrategy(
        string workload,
        string strategy,
        IReadOnlyList<FieldValue> values,
        ValueCodecKind codec,
        BlockCompressionKind compression)
    {
        ValueEncodedBlock block = default;

        var encodeWatch = System.Diagnostics.Stopwatch.StartNew();
        for (int i = 0; i < 5; i++)
            block = CompressionCodec.EncodeValuesBlock(FieldKind.Float, codec, compression, values);
        encodeWatch.Stop();

        var decodeWatch = System.Diagnostics.Stopwatch.StartNew();
        for (int i = 0; i < 5; i++)
            _ = CompressionCodec.DecodeValues(FieldKind.Float, block.Codec, block.Compression, block.Payload);
        decodeWatch.Stop();

        return new FloatStrategyBenchmarkResult(
            workload,
            strategy,
            block.Payload.Length,
            encodeWatch.Elapsed.TotalMilliseconds / 5.0,
            decodeWatch.Elapsed.TotalMilliseconds / 5.0,
            block.Codec.ToString(),
            block.Compression.ToString());
    }

    private static FloatStrategyBenchmarkResult BenchmarkAdaptiveFloatStrategy(
        string workload,
        IReadOnlyList<FieldValue> values)
    {
        ValueEncodedBlock block = default;

        var encodeWatch = System.Diagnostics.Stopwatch.StartNew();
        for (int i = 0; i < 5; i++)
            block = CompressionCodec.EncodeValuesAdaptive(FieldKind.Float, values);
        encodeWatch.Stop();

        var decodeWatch = System.Diagnostics.Stopwatch.StartNew();
        for (int i = 0; i < 5; i++)
            _ = CompressionCodec.DecodeValues(FieldKind.Float, block.Codec, block.Compression, block.Payload);
        decodeWatch.Stop();

        return new FloatStrategyBenchmarkResult(
            workload,
            "adaptive",
            block.Payload.Length,
            encodeWatch.Elapsed.TotalMilliseconds / 5.0,
            decodeWatch.Elapsed.TotalMilliseconds / 5.0,
            block.Codec.ToString(),
            block.Compression.ToString());
    }
}
