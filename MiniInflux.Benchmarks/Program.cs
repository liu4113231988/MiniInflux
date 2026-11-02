using System.Diagnostics;
using System.Runtime;
using System.Text;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Benchmarks;

/// <summary>
/// Standalone evaluation harness for the P3 "compact data model" proposal.
/// It measures the *current* in-memory/wire cost on the real production types
/// (Point + Dictionary tags/fields + text WAL) and compares them against a
/// candidate compact representation implemented entirely in this project so
/// production storage semantics are never touched.
/// </summary>
public static class Program
{
    private const long BaseTs = 1_710_000_000_000_000_000L;

    public static void Main(string[] args)
    {
        var points = int.TryParse(args.Length > 0 ? args[0] : null, out var p) ? p : 100_000;
        var batch = int.TryParse(args.Length > 1 ? args[1] : null, out var b) ? b : 5_000;
        var series = int.TryParse(args.Length > 2 ? args[2] : null, out var s) ? s : 32;

        RunWorkload("2 tags / 2 fields (typical)", points, batch, series, 2, 2);
        Console.WriteLine();
        RunWorkload("4 tags / 4 fields (rich)", points, batch, series, 4, 4);

        Console.WriteLine();
        QueryBenchmarks.Run(points, series, 2, 2);
    }

    private static void RunWorkload(string label, int points, int batch, int series, int tags, int fields)
    {
        Console.WriteLine($"=== Workload: {label} ===");
        Console.WriteLine($"points={points}, batch={batch}, series={series}, tags={tags}, fields={fields}");
        Console.WriteLine($"clr={Environment.Version}, gc={(GCSettings.IsServerGC ? "server" : "workstation")}");
        Console.WriteLine();

        // Phase 1: current model allocation profile
        var current = MeasureCurrentModel(points, series, tags, fields);
        PrintModel("current (Point + Dictionary<string,string> + Dictionary<string,FieldValue>)", current);

        // Phase 2: compact row model
        var row = MeasureCompactRowModel(points, series, tags, fields);
        PrintModel("compact-row (struct CompactPoint + per-point FieldValue[] + shared SeriesDescriptor)", row);

        // Phase 3: compact columnar model
        var col = MeasureCompactColumnarModel(points, series, tags, fields);
        PrintModel("compact-columnar (per-series long[] + typed value arrays)", col);

        Console.WriteLine();

        // Phase 4: WAL text vs binary
        var walText = MeasureWalText(points, series, tags, fields);
        PrintWal("WAL text (line protocol, current)", walText);
        var walBin = MeasureWalBinary(points, series, tags, fields);
        PrintWal("WAL binary (candidate)", walBin);

        Console.WriteLine();

        // Phase 5: end-to-end write throughput + GC on real engine
        var e2e = MeasureEndToEndWrite(points, batch, series, tags, fields);
        PrintE2E(e2e);

        Console.WriteLine();
    }

    // ---------- workload generation ----------

    private static string BuildLineProtocolBatch(int count, int seriesCount, int tags, int fields)
    {
        var sb = new StringBuilder(count * (64 + tags * 16 + fields * 16));
        var inv = System.Globalization.CultureInfo.InvariantCulture;
        for (var i = 0; i < count; i++)
        {
            var host = i % seriesCount;
            sb.Append("cpu");
            for (var t = 0; t < tags; t++)
            {
                string tagVal;
                if (t == 0) tagVal = "server" + host.ToString("d2", inv);
                else if (t == 1) tagVal = host % 2 == 0 ? "cn" : "us";
                else tagVal = "v" + t.ToString(inv) + "-" + (host % 4).ToString(inv);
                sb.Append(',').Append('t').Append(t.ToString(inv)).Append('=').Append(tagVal);
            }
            sb.Append(' ');
            for (var f = 0; f < fields; f++)
            {
                if (f > 0) sb.Append(',');
                if (f == 0) sb.Append("value");
                else { sb.Append('f'); sb.Append(f.ToString(inv)); }
                sb.Append('=');
                if (f % 2 == 0)
                    sb.Append(((i % 1000) / 10.0).ToString("0.###", inv));
                else
                {
                    sb.Append((i % 100).ToString(inv));
                    sb.Append('i');
                }
            }
            var ts = BaseTs + i * 1_000_000_000L;
            sb.Append(' ').Append(ts.ToString(inv)).Append('\n');
        }
        return sb.ToString();
    }

    private static List<Point> ParsePoints(string lp)
    {
        try { return LineProtocolParser.ParseMany(lp, TimestampPrecision.Parse("ns")); }
        catch (FormatException ex)
        {
            var lines = lp.Split('\n');
            for (var i = 0; i < lines.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(lines[i])) continue;
                try { LineProtocolParser.ParseOne(lines[i], TimestampPrecision.Parse("ns")); }
                catch (FormatException)
                {
                    Console.Error.WriteLine($"Parse failure at line {i}: [{lines[i]}]");
                    break;
                }
            }
            throw new FormatException(ex.Message, ex);
        }
    }

    // ---------- allocation measurement helpers ----------

    private static (long bytes, double ms, int gen0, int gen1, int gen2) MeasureAlloc(Action body)
    {
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, true, true);

        var g0 = GC.CollectionCount(0);
        var g1 = GC.CollectionCount(1);
        var g2 = GC.CollectionCount(2);
        var before = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        body();
        sw.Stop();
        var after = GC.GetAllocatedBytesForCurrentThread();
        GC.KeepAlive(body);
        return (after - before, sw.Elapsed.TotalMilliseconds,
            GC.CollectionCount(0) - g0, GC.CollectionCount(1) - g1, GC.CollectionCount(2) - g2);
    }

    // Retained heap size of the object produced by factory: force GC, snapshot heap,
    // build and root the object, force GC again, return the heap delta.
    private static long MeasureHeld(Func<object> factory)
    {
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        var before = GC.GetGCMemoryInfo().HeapSizeBytes;
        var held = factory();
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        var after = GC.GetGCMemoryInfo().HeapSizeBytes;
        GC.KeepAlive(held);
        return after - before;
    }

    // ---------- Phase 1: current model ----------

    private static ModelResult MeasureCurrentModel(int count, int seriesCount, int tags, int fields)
    {
        var lp = BuildLineProtocolBatch(count, seriesCount, tags, fields);

        var (bytes, ms, g0, g1, g2) = MeasureAlloc(() =>
        {
            var pts = ParsePoints(lp);
            GC.KeepAlive(pts);
        });

        var held = MeasureHeld(() => ParsePoints(lp));

        return new ModelResult(count, bytes, ms, g0, g1, g2, held);
    }

    // ---------- Phase 2: compact row model ----------

    private static ModelResult MeasureCompactRowModel(int count, int seriesCount, int tags, int fields)
    {
        var lp = BuildLineProtocolBatch(count, seriesCount, tags, fields);
        var source = ParsePoints(lp);

        var (bytes, ms, g0, g1, g2) = MeasureAlloc(() =>
        {
            var batch = CompactRowBatch.From(source);
            GC.KeepAlive(batch);
        });

        var held = MeasureHeld(() => CompactRowBatch.From(source));
        return new ModelResult(count, bytes, ms, g0, g1, g2, held);
    }

    // ---------- Phase 3: compact columnar model ----------

    private static ModelResult MeasureCompactColumnarModel(int count, int seriesCount, int tags, int fields)
    {
        var lp = BuildLineProtocolBatch(count, seriesCount, tags, fields);
        var source = ParsePoints(lp);

        var (bytes, ms, g0, g1, g2) = MeasureAlloc(() =>
        {
            var batch = CompactColumnarBatch.From(source);
            GC.KeepAlive(batch);
        });

        var held = MeasureHeld(() => CompactColumnarBatch.From(source));
        return new ModelResult(count, bytes, ms, g0, g1, g2, held);
    }

    // ---------- Phase 4: WAL text vs binary ----------

    private static WalResult MeasureWalText(int count, int seriesCount, int tags, int fields)
    {
        var lp = BuildLineProtocolBatch(count, seriesCount, tags, fields);
        var points = ParsePoints(lp);

        var encodeMs = 0.0;
        var payloadLen = 0;
        for (var r = 0; r < 3; r++)
        {
            var sw = Stopwatch.StartNew();
            var buf = new PooledWriter(count * (64 + tags * 16 + fields * 16));
            WalTextEncoder.WritePayload(buf, "bench", "autogen", points);
            sw.Stop();
            encodeMs = sw.Elapsed.TotalMilliseconds;
            payloadLen = buf.Written;
            buf.Dispose();
        }

        var payload = new byte[payloadLen];
        {
            var buf = new PooledWriter(count * (64 + tags * 16 + fields * 16));
            WalTextEncoder.WritePayload(buf, "bench", "autogen", points);
            buf.WrittenSpan.CopyTo(payload);
            buf.Dispose();
        }

        var decodeMs = 0.0;
        for (var r = 0; r < 3; r++)
        {
            var sw = Stopwatch.StartNew();
            var line = Encoding.UTF8.GetString(payload).TrimEnd('\n');
            var parts = line.Split('\t', 3);
            if (parts.Length < 3)
                throw new InvalidOperationException($"WAL text split failed: parts={parts.Length}");
            var pts = LineProtocolParser.ParseMany(parts[2], TimestampPrecision.Parse("ns"));
            sw.Stop();
            decodeMs = sw.Elapsed.TotalMilliseconds;
            GC.KeepAlive(pts);
        }

        return new WalResult(count, payloadLen, encodeMs, decodeMs);
    }

    private static WalResult MeasureWalBinary(int count, int seriesCount, int tags, int fields)
    {
        var lp = BuildLineProtocolBatch(count, seriesCount, tags, fields);
        var points = ParsePoints(lp);

        var encodeMs = 0.0;
        var payloadLen = 0;
        for (var r = 0; r < 3; r++)
        {
            var sw = Stopwatch.StartNew();
            var buf = new PooledWriter(count * (32 + tags * 12 + fields * 12));
            WalBinaryEncoder.WritePayload(buf, "bench", "autogen", points);
            sw.Stop();
            encodeMs = sw.Elapsed.TotalMilliseconds;
            payloadLen = buf.Written;
            buf.Dispose();
        }

        var payload = new byte[payloadLen];
        {
            var buf = new PooledWriter(count * (32 + tags * 12 + fields * 12));
            WalBinaryEncoder.WritePayload(buf, "bench", "autogen", points);
            buf.WrittenSpan.CopyTo(payload);
            buf.Dispose();
        }

        var decodeMs = 0.0;
        for (var r = 0; r < 3; r++)
        {
            var sw = Stopwatch.StartNew();
            var pts = WalBinaryEncoder.ReadPayload(payload);
            sw.Stop();
            decodeMs = sw.Elapsed.TotalMilliseconds;
            GC.KeepAlive(pts);
        }

        return new WalResult(count, payloadLen, encodeMs, decodeMs);
    }

    // ---------- Phase 5: end-to-end engine write with GC ----------

    private static E2EResult MeasureEndToEndWrite(int count, int batchSize, int seriesCount, int tags, int fields)
    {
        var lp = BuildLineProtocolBatch(count, seriesCount, tags, fields);
        var allPoints = ParsePoints(lp);

        var dataDir = Path.Combine(Path.GetTempPath(), "miniinflux-eval-" + Guid.NewGuid().ToString("N"));
        try
        {
            GC.Collect(2, GCCollectionMode.Forced, true, true);
            var g0 = GC.CollectionCount(0);
            var g1 = GC.CollectionCount(1);
            var g2 = GC.CollectionCount(2);
            var memBefore = GC.GetGCMemoryInfo();

            double writeMs, pauseMs;
            int g0After, g1After, g2After;
            long heapAfter, walPeakBytes;
            {
                // flushThreshold above count keeps all points in the WAL until dispose,
                // so walPeakBytes reflects the full un-checkpointed WAL volume.
                using var engine = new TsdbEngine(dataDir, flushThreshold: count + 1);
                engine.CreateDatabase("bench");

                var sw = Stopwatch.StartNew();
                var offset = 0;
                while (offset < allPoints.Count)
                {
                    var take = Math.Min(batchSize, allPoints.Count - offset);
                    var slice = allPoints.GetRange(offset, take);
                    engine.Write("bench", "autogen", slice);
                    offset += take;
                }
                sw.Stop();
                writeMs = sw.Elapsed.TotalMilliseconds;

                // Peak WAL size before flush/checkpoint on dispose.
                walPeakBytes = 0;
                foreach (var f in Directory.EnumerateFiles(dataDir, "*.wal", SearchOption.AllDirectories))
                    walPeakBytes += new FileInfo(f).Length;

                g0After = GC.CollectionCount(0) - g0;
                g1After = GC.CollectionCount(1) - g1;
                g2After = GC.CollectionCount(2) - g2;
                var memAfter = GC.GetGCMemoryInfo();
                heapAfter = memAfter.HeapSizeBytes;
                pauseMs = 0;
                foreach (var t in memAfter.PauseDurations) pauseMs += t.TotalMilliseconds;
            } // dispose triggers flush + WAL checkpoint

            // Recovery throughput
            double recoveryMs;
            {
                using var recovered = new TsdbEngine(dataDir, flushThreshold: count + 1);
                var recSw = Stopwatch.StartNew();
                recovered.Recover();
                recSw.Stop();
                recoveryMs = recSw.Elapsed.TotalMilliseconds;
            }

            long segBytes = 0;
            foreach (var f in Directory.EnumerateFiles(dataDir, "*.seg", SearchOption.AllDirectories))
                segBytes += new FileInfo(f).Length;

            return new E2EResult(count, writeMs, recoveryMs,
                g0After, g1After, g2After, pauseMs,
                memBefore.HeapSizeBytes, heapAfter, walPeakBytes, segBytes);
        }
        finally
        {
            try { Directory.Delete(dataDir, true); } catch { }
        }
    }

    // ---------- printers ----------

    private static void PrintModel(string name, ModelResult r)
    {
        Console.WriteLine($"[{name}]");
        Console.WriteLine($"  alloc_bytes_total     = {r.AllocatedBytes,12:N0}  ({r.BytesPerPoint,8:N1} B/point)");
        Console.WriteLine($"  live_held_bytes       = {r.HeldBytes,12:N0}  ({r.HeldBytesPerPoint,8:N1} B/point)");
        Console.WriteLine($"  build_time_ms         = {r.BuildMs,12:F2}  ({r.PointsPerSec,12:N0} pts/s)");
        Console.WriteLine($"  gc gen0/gen1/gen2     = {r.Gen0}/{r.Gen1}/{r.Gen2}");
    }

    private static void PrintWal(string name, WalResult r)
    {
        Console.WriteLine($"[{name}]");
        Console.WriteLine($"  payload_bytes         = {r.PayloadBytes,12:N0}  ({r.BytesPerPoint,8:N2} B/point)");
        Console.WriteLine($"  encode_ms (1 pass)    = {r.EncodeMs,12:F2}  ({r.EncodePointsPerSec,12:N0} pts/s)");
        Console.WriteLine($"  decode_ms (1 pass)    = {r.DecodeMs,12:F2}  ({r.DecodePointsPerSec,12:N0} pts/s)");
    }

    private static void PrintE2E(E2EResult r)
    {
        Console.WriteLine("[end-to-end write (real TsdbEngine)]");
        Console.WriteLine($"  write_ms              = {r.WriteMs,12:F2}  ({r.WritePointsPerSec,12:N0} pts/s)");
        Console.WriteLine($"  recovery_ms           = {r.RecoveryMs,12:F2}  ({r.RecoveryPointsPerSec,12:N0} pts/s)");
        Console.WriteLine($"  gc gen0/gen1/gen2     = {r.Gen0}/{r.Gen1}/{r.Gen2}  pause_total_ms={r.PauseMs:F2}");
        Console.WriteLine($"  heap before/after     = {r.HeapBefore:N0} -> {r.HeapAfter:N0}");
        Console.WriteLine($"  wal_peak_bytes        = {r.WalBytes,12:N0}  ({(double)r.WalBytes / r.Count,8:N2} B/point)");
        Console.WriteLine($"  segment_bytes         = {r.SegBytes,12:N0}  ({(double)r.SegBytes / r.Count,8:N2} B/point)");
    }
}

// ---------- result records ----------

internal sealed record ModelResult(int Count, long AllocatedBytes, double BuildMs, int Gen0, int Gen1, int Gen2, long HeldBytes)
{
    public double BytesPerPoint => (double)AllocatedBytes / Count;
    public double HeldBytesPerPoint => (double)HeldBytes / Count;
    public double PointsPerSec => Count / Math.Max(BuildMs / 1000.0, 0.0001);
}

internal sealed record WalResult(int Count, int PayloadBytes, double EncodeMs, double DecodeMs)
{
    public double BytesPerPoint => (double)PayloadBytes / Count;
    public double EncodePointsPerSec => Count / Math.Max(EncodeMs / 1000.0, 0.0001);
    public double DecodePointsPerSec => Count / Math.Max(DecodeMs / 1000.0, 0.0001);
}

internal sealed record E2EResult(int Count, double WriteMs, double RecoveryMs, int Gen0, int Gen1, int Gen2, double PauseMs,
    long HeapBefore, long HeapAfter, long WalBytes, long SegBytes)
{
    public double WritePointsPerSec => Count / Math.Max(WriteMs / 1000.0, 0.0001);
    public double RecoveryPointsPerSec => Count / Math.Max(RecoveryMs / 1000.0, 0.0001);
}
