using System.Text;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class ReadWritePathOptimizationTests : IDisposable
{
    private readonly string _testDir;

    public ReadWritePathOptimizationTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_testDir)) Directory.Delete(_testDir, true); } catch { }
    }

    [Fact]
    public void Utf8ParseMany_MatchesStringParser_OnTypicalPayload()
    {
        var text = new StringBuilder()
            .Append("cpu,host=a,region=us value=1.5,active=true,count=3i 1000\n")
            .Append("cpu,host=b value=-2.5 2000\n")
            .Append("# a comment line\n")
            .Append("   \n")
            .Append("mem free=10i\n") // no timestamp
            .Append("cpu,host=a,region=us value=2.5 1000\r\n")
            .Append("cpu,region=us,host=a value=3.5 4000\n") // unsorted tags
            .Append("boolv t=T,f=F,yes=false 5000\n")
            .ToString();

        var expected = LineProtocolParser.ParseMany(text, TimestampPrecision.Parse("ns"));
        var actual = LineProtocolParser.ParseMany(Encoding.UTF8.GetBytes(text), TimestampPrecision.Parse("ns"));

        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Measurement, actual[i].Measurement);
            Assert.Equal(expected[i].Tags, actual[i].Tags);
            // Lines without an explicit timestamp get "now" from each parser separately.
            Assert.True(expected[i].TimestampNs == actual[i].TimestampNs
                || Math.Abs(expected[i].TimestampNs - actual[i].TimestampNs) < 10_000_000_000L,
                $"timestamp mismatch at point {i}");
            Assert.Equal(expected[i].TagsCanonical, actual[i].TagsCanonical);
            Assert.Equal(expected[i].Fields.Count, actual[i].Fields.Count);
            foreach (var (key, value) in expected[i].Fields)
            {
                var other = actual[i].Fields[key];
                Assert.Equal(value.Kind, other.Kind);
                Assert.Equal(value.AsDouble(), other.AsDouble());
            }
        }
    }

    [Fact]
    public void Utf8ParseMany_MatchesStringParser_OnEscapedAndQuotedLines()
    {
        var text = "cp\\,u,host=a\\ b value=\"quoted value\",x=1i 1000\n"
            + "esc\\=tag,host=a\\,b value=2 2000\n";

        var expected = LineProtocolParser.ParseMany(text, TimestampPrecision.Parse("ns"));
        var actual = LineProtocolParser.ParseMany(Encoding.UTF8.GetBytes(text), TimestampPrecision.Parse("ns"));

        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Measurement, actual[i].Measurement);
            Assert.Equal(expected[i].Tags, actual[i].Tags);
            Assert.Equal(expected[i].TimestampNs, actual[i].TimestampNs);
            foreach (var (key, value) in expected[i].Fields)
                Assert.Equal(value.AsDouble(), actual[i].Fields[key].AsDouble());
        }
    }

    [Fact]
    public async Task AsyncFlush_LargeBatch_StaysReadableAndDurable()
    {
        const int pointCount = 6000; // above the 4096 inline-flush cutoff
        using (var engine = new TsdbEngine(_testDir, flushThreshold: 100,
                   flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0))
        {
            var points = new List<Point>(pointCount);
            for (var i = 0; i < pointCount; i++)
                points.Add(new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { ["host"] = "h1" },
                    Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(i) },
                    TimestampNs = (i + 1) * 1_000_000_000L
                });
            await engine.WriteAsync("db", "autogen", points);

            // Whether or not the background flush has finished, reads must see every point.
            Assert.Equal(pointCount, engine.ReadAllPoints("db", "autogen", "cpu", null, null).Count);

            // FlushAll waits for in-flight background flushes; afterwards the buffer is empty.
            engine.FlushAll();
            Assert.Equal(pointCount, engine.ReadAllPoints("db", "autogen", "cpu", null, null).Count);
        }

        // WAL checkpoint advanced with the completed flush: restart recovers from segments.
        using var restarted = new TsdbEngine(_testDir, flushThreshold: 100,
            flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0);
        restarted.Recover();
        Assert.Equal(pointCount, restarted.ReadAllPoints("db", "autogen", "cpu", null, null).Count);
    }

    [Fact]
    public async Task EnumeratePoints_StreamingMerge_MatchesReadAllPoints()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 20,
            flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0);

        // Batch 1: becomes a segment (sync inline flush at threshold 20).
        var batch1 = new List<Point>();
        for (var i = 1; i <= 50; i++)
            batch1.Add(new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "h1" },
                Fields = new Dictionary<string, FieldValue> { ["a"] = FieldValue.FromDouble(i) },
                TimestampNs = i * 1_000_000_000L
            });
        await engine.WriteAsync("db", "autogen", batch1);

        // Batch 2: overlapping timestamps with a *different* field, plus newer points; part of it
        // flushes, the rest stays buffered — exercising buffer/segment merge and field merging.
        var batch2 = new List<Point>();
        for (var i = 25; i <= 70; i++)
            batch2.Add(new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "h1" },
                Fields = new Dictionary<string, FieldValue> { ["b"] = FieldValue.FromDouble(i * 10) },
                TimestampNs = i * 1_000_000_000L
            });
        // Leave batch2 partially buffered: write then remove... simplest: small threshold flushes
        // all of it; instead write batch2 and batch3 so the last few points remain buffered.
        await engine.WriteAsync("db", "autogen", batch2);
        var batch3 = new List<Point>();
        for (var i = 71; i <= 75; i++)
            batch3.Add(new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "h2" },
                Fields = new Dictionary<string, FieldValue> { ["a"] = FieldValue.FromDouble(i) },
                TimestampNs = i * 1_000_000_000L
            });
        await engine.WriteAsync("db", "autogen", batch3);

        var materialized = engine.ReadAllPoints("db", "autogen", "cpu", null, null);
        var streamed = engine.EnumeratePoints("db", "autogen", "cpu", null, null).ToList();

        Assert.Equal(materialized.Count, streamed.Count);
        static string TagsKey(Point p) =>
            string.Join(",", p.Tags.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => kv.Key + "=" + kv.Value));
        var byKey = materialized.ToDictionary(
            p => (p.Measurement, TagsKey(p), p.TimestampNs),
            p => p.Fields);
        foreach (var point in streamed)
        {
            var expected = byKey[(point.Measurement, TagsKey(point), point.TimestampNs)];
            Assert.Equal(expected.Count, point.Fields.Count);
            foreach (var (key, value) in expected)
                Assert.Equal(value.AsDouble(), point.Fields[key].AsDouble());
        }

        // Streamed output is globally time-ordered.
        for (var i = 1; i < streamed.Count; i++)
            Assert.True(streamed[i - 1].TimestampNs <= streamed[i].TimestampNs);

        // Field-merge semantics: the overlapping range carries both fields on one point.
        var merged = streamed.First(p => p.TimestampNs == 30 * 1_000_000_000L && p.Tags["host"] == "h1");
        Assert.True(merged.Fields.ContainsKey("a"));
        Assert.True(merged.Fields.ContainsKey("b"));
    }

    [Fact]
    public async Task EnumeratePoints_TimeRangeAndProjection_MatchReadAllPoints()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 20,
            flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0);

        var batch = new List<Point>();
        for (var i = 1; i <= 40; i++)
            batch.Add(new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "h1" },
                Fields = new Dictionary<string, FieldValue>
                {
                    ["a"] = FieldValue.FromDouble(i),
                    ["b"] = FieldValue.FromDouble(i * 2)
                },
                TimestampNs = i * 1_000_000_000L
            });
        await engine.WriteAsync("db", "autogen", batch);

        long min = 10 * 1_000_000_000L, max = 20 * 1_000_000_000L;
        var fields = new HashSet<string> { "a" };
        var materialized = engine.ReadAllPoints("db", "autogen", "cpu", min, max, requestedFields: fields);
        var streamed = engine.EnumeratePoints("db", "autogen", "cpu", min, max, requestedFields: fields).ToList();

        Assert.Equal(materialized.Count, streamed.Count);
        Assert.All(streamed, p =>
        {
            Assert.InRange(p.TimestampNs, min, max);
            Assert.Single(p.Fields);
            Assert.True(p.Fields.ContainsKey("a"));
        });
    }
}
