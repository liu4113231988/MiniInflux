using System.Diagnostics;
using System.Text;
using System.Text.Json;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Benchmarks;

/// <summary>
/// Measures query-path memory for the current Point-based model and a compact-row
/// simulation that mirrors the real executor's data flow (materialize input →
/// group/sort → build boxed rows → JSON). The output layer (List&lt;List&lt;object?&gt;&gt;
/// boxing, QuerySeries, JSON serialization) is kept identical in both paths so the
/// measured delta isolates what compact-row actually changes: the input materialization
/// and grouping-state footprint.
/// </summary>
internal static class QueryBenchmarks
{
    private const long BaseTs = 1_710_000_000_000_000_000L;

    public static void Run(int points, int seriesCount, int tags, int fields)
    {
        Console.WriteLine($"--- query benchmarks: {points} pts, {seriesCount} series, {tags}t/{fields}f ---");

        // Build a real engine with flushed segments so queries hit the same code paths
        // (segment read + buffered merge) as production.
        var dataDir = Path.Combine(Path.GetTempPath(), "miniinflux-qeval-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var engine = new TsdbEngine(dataDir, flushThreshold: points + 1);
            engine.CreateDatabase("bench");
            var lp = BuildLineProtocol(points, seriesCount, tags, fields);
            var allPoints = LineProtocolParser.ParseMany(lp, TimestampPrecision.Parse("ns"));
            // Write in batches and force a flush so segments exist.
            var batchSize = 5000;
            for (var off = 0; off < allPoints.Count; off += batchSize)
                engine.Write("bench", "autogen", allPoints.GetRange(off, Math.Min(batchSize, allPoints.Count - off)));
            engine.FlushAll();

            var executor = new QueryExecutor();

            // Query set: raw select, group by time, group by tag, order by, fill, chunked.
            var queries = new (string Label, string Sql)[]
            {
                ("raw SELECT LIMIT 1000", "SELECT value FROM cpu LIMIT 1000"),
                ("GROUP BY time(1s) mean", "SELECT mean(value) FROM cpu GROUP BY time(1s)"),
                ("GROUP BY tag(t0) mean", "SELECT mean(value) FROM cpu GROUP BY t0"),
                ("ORDER BY time DESC LIMIT 1000", "SELECT value FROM cpu ORDER BY time DESC LIMIT 1000"),
                ("GROUP BY time(1s) fill(0)", "SELECT mean(value) FROM cpu GROUP BY time(1s) fill(0)"),
            };

            // --- current model (real QueryExecutor) ---
            Console.WriteLine();
            Console.WriteLine("[current model: real QueryExecutor]");
            foreach (var (label, sql) in queries)
            {
                var r = MeasureQuery(() =>
                {
                    var outcome = executor.ExecuteWithReport(engine, "bench", sql);
                    GC.KeepAlive(outcome);
                    return outcome.Response;
                });
                PrintQuery(label, r);
            }

            // Chunked streaming path (raw select only — chunking is a raw-select feature).
            {
                var r = MeasureQuery(() =>
                {
                    var outcome = executor.ExecuteChunkedWithReport(engine, "bench", "SELECT value FROM cpu LIMIT 10000", chunkSize: 4096);
                    var consumed = new List<QueryResponse>(outcome.Responses);
                    GC.KeepAlive(consumed);
                    return consumed;
                });
                PrintQuery("chunked raw SELECT LIMIT 10000", r);
            }

            // JSON direct-write path (buffered raw descending JSON).
            {
                var r = MeasureQuery(() =>
                {
                    var outcome = executor.TryExecuteBufferedRawDescendingJson(engine, "bench", "SELECT value FROM cpu WHERE t0 = 'server00' ORDER BY time DESC LIMIT 1000");
                    GC.KeepAlive(outcome);
                    return outcome?.Json ?? Array.Empty<byte>();
                });
                PrintQuery("JSON direct write (single series)", r);
            }

            // --- compact-row simulation ---
            // Reuse the parsed points to build a compact batch, then simulate each query's
            // data flow. The output (boxed rows + QuerySeries + JSON) is built identically
            // to the real executor; only the input materialization and grouping state use
            // compact structures.
            Console.WriteLine();
            Console.WriteLine("[compact-row simulation: same output layer, compact input]");
            var compact = CompactRowBatch.From(allPoints);
            var tagKeys = new[] { "t0", "t1" };
            var fieldKeys = new[] { "value", "f1" };

            // Raw select with limit: iterate compact points, build boxed rows.
            PrintQuery("compact raw SELECT LIMIT 1000", MeasureQuery(() =>
            {
                var series = new QuerySeries { Name = "cpu", Columns = ["time", "value"], Values = [] };
                var limit = 1000;
                var emitted = 0;
                foreach (var cp in compact.Points)
                {
                    if (emitted >= limit) break;
                    var desc = compact.Series[cp.SeriesIndex];
                    var fi = Array.IndexOf(desc.FieldNames, "value");
                    series.Values.Add(new List<object?> { cp.TimestampNs, cp.Values[fi].ToObject() });
                    emitted++;
                }
                return new QueryResponse { Results = [new QueryResult { Series = [series] }] };
            }));

            // GROUP BY time(1s) mean: bucket compact points by 1s, compute mean, boxed rows.
            PrintQuery("compact GROUP BY time(1s) mean", MeasureQuery(() =>
            {
                const long step = 1_000_000_000L;
                var buckets = new Dictionary<long, (double Sum, long Count)>();
                foreach (var cp in compact.Points)
                {
                    var bucket = (cp.TimestampNs / step) * step;
                    var desc = compact.Series[cp.SeriesIndex];
                    var fi = Array.IndexOf(desc.FieldNames, "value");
                    var v = cp.Values[fi].AsDouble() ?? 0;
                    if (buckets.TryGetValue(bucket, out var agg)) buckets[bucket] = (agg.Sum + v, agg.Count + 1);
                    else buckets[bucket] = (v, 1);
                }
                var series = new QuerySeries { Name = "cpu", Columns = ["time", "mean"], Values = [] };
                foreach (var (t, agg) in buckets.OrderBy(x => x.Key))
                    series.Values.Add(new List<object?> { t, agg.Count == 0 ? null : agg.Sum / agg.Count });
                return new QueryResponse { Results = [new QueryResult { Series = [series] }] };
            }));

            // GROUP BY tag(t0) mean: group by tag value, compute mean, boxed rows.
            PrintQuery("compact GROUP BY tag(t0) mean", MeasureQuery(() =>
            {
                var groups = new Dictionary<string, (double Sum, long Count)>(StringComparer.Ordinal);
                foreach (var cp in compact.Points)
                {
                    var desc = compact.Series[cp.SeriesIndex];
                    var t0Idx = Array.IndexOf(desc.TagKeys, "t0");
                    var t0 = t0Idx >= 0 ? desc.TagValues[t0Idx] : "";
                    var fi = Array.IndexOf(desc.FieldNames, "value");
                    var v = cp.Values[fi].AsDouble() ?? 0;
                    if (groups.TryGetValue(t0, out var agg)) groups[t0] = (agg.Sum + v, agg.Count + 1);
                    else groups[t0] = (v, 1);
                }
                var seriesMap = new Dictionary<string, QuerySeries>(StringComparer.Ordinal);
                foreach (var (t0, agg) in groups)
                {
                    var s = new QuerySeries { Name = "cpu", Tags = new Dictionary<string, string> { ["t0"] = t0 }, Columns = ["time", "mean"], Values = [] };
                    s.Values.Add(new List<object?> { null, agg.Count == 0 ? null : agg.Sum / agg.Count });
                    seriesMap[t0] = s;
                }
                return new QueryResponse { Results = [new QueryResult { Series = [.. seriesMap.Values] }] };
            }));

            // ORDER BY time DESC LIMIT 1000: sort compact points, build boxed rows.
            PrintQuery("compact ORDER BY time DESC LIMIT 1000", MeasureQuery(() =>
            {
                var sorted = compact.Points.OrderByDescending(p => p.TimestampNs).Take(1000).ToList();
                var series = new QuerySeries { Name = "cpu", Columns = ["time", "value"], Values = [] };
                foreach (var cp in sorted)
                {
                    var desc = compact.Series[cp.SeriesIndex];
                    var fi = Array.IndexOf(desc.FieldNames, "value");
                    series.Values.Add(new List<object?> { cp.TimestampNs, cp.Values[fi].ToObject() });
                }
                return new QueryResponse { Results = [new QueryResult { Series = [series] }] };
            }));

            // GROUP BY time(1s) fill(0): same as time group + fill missing buckets with 0.
            PrintQuery("compact GROUP BY time(1s) fill(0)", MeasureQuery(() =>
            {
                const long step = 1_000_000_000L;
                var buckets = new Dictionary<long, (double Sum, long Count)>();
                long minT = long.MaxValue, maxT = long.MinValue;
                foreach (var cp in compact.Points)
                {
                    var bucket = (cp.TimestampNs / step) * step;
                    if (bucket < minT) minT = bucket;
                    if (bucket > maxT) maxT = bucket;
                    var desc = compact.Series[cp.SeriesIndex];
                    var fi = Array.IndexOf(desc.FieldNames, "value");
                    var v = cp.Values[fi].AsDouble() ?? 0;
                    if (buckets.TryGetValue(bucket, out var agg)) buckets[bucket] = (agg.Sum + v, agg.Count + 1);
                    else buckets[bucket] = (v, 1);
                }
                var series = new QuerySeries { Name = "cpu", Columns = ["time", "mean"], Values = [] };
                for (var t = minT; t <= maxT; t += step)
                {
                    if (buckets.TryGetValue(t, out var agg))
                        series.Values.Add(new List<object?> { t, agg.Count == 0 ? 0.0 : agg.Sum / agg.Count });
                    else
                        series.Values.Add(new List<object?> { t, 0.0 });
                }
                return new QueryResponse { Results = [new QueryResult { Series = [series] }] };
            }));

            // JSON serialization of the compact raw-select result (same output layer).
            PrintQuery("compact JSON serialize (raw SELECT 1000)", MeasureQuery(() =>
            {
                var series = new QuerySeries { Name = "cpu", Columns = ["time", "value"], Values = [] };
                var limit = 1000;
                var emitted = 0;
                foreach (var cp in compact.Points)
                {
                    if (emitted >= limit) break;
                    var desc = compact.Series[cp.SeriesIndex];
                    var fi = Array.IndexOf(desc.FieldNames, "value");
                    series.Values.Add(new List<object?> { cp.TimestampNs, cp.Values[fi].ToObject() });
                    emitted++;
                }
                var resp = new QueryResponse { Results = [new QueryResult { Series = [series] }] };
                var json = JsonSerializer.SerializeToUtf8Bytes(resp);
                return json;
            }));

            GC.KeepAlive(compact);
            GC.KeepAlive(tagKeys);
            GC.KeepAlive(fieldKeys);
        }
        finally
        {
            try { Directory.Delete(dataDir, true); } catch { }
        }
    }

    // ---------- measurement helpers ----------

    private static (long allocBytes, long heldBytes, double ms, int gen0, int gen1, int gen2) MeasureQuery(Func<object> query)
    {
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        var heapBefore = GC.GetGCMemoryInfo().HeapSizeBytes;

        var g0 = GC.CollectionCount(0);
        var g1 = GC.CollectionCount(1);
        var g2 = GC.CollectionCount(2);
        var before = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        var result = query();
        sw.Stop();
        var after = GC.GetAllocatedBytesForCurrentThread();
        var gen0 = GC.CollectionCount(0) - g0;
        var gen1 = GC.CollectionCount(1) - g1;
        var gen2 = GC.CollectionCount(2) - g2;

        // Held bytes: force GC with the result still rooted, measure retained heap delta.
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        var heapAfter = GC.GetGCMemoryInfo().HeapSizeBytes;
        GC.KeepAlive(result);
        return (after - before, Math.Max(0, heapAfter - heapBefore), sw.Elapsed.TotalMilliseconds, gen0, gen1, gen2);
    }

    private static void PrintQuery(string label, (long alloc, long held, double ms, int g0, int g1, int g2) r)
    {
        Console.WriteLine($"  {label,-42} alloc={r.alloc,12:N0}  held={r.held,12:N0}  {r.ms,8:F2}ms  gc={r.g0}/{r.g1}/{r.g2}");
    }

    private static string BuildLineProtocol(int count, int seriesCount, int tags, int fields)
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
}
