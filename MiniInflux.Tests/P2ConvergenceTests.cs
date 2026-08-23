using System.IO.Compression;
using System.Reflection;
using System.Text;
using Microsoft.AspNetCore.Http;
using MiniInflux.Net10;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

/// <summary>
/// Phase-5 P2 convergence regressions: _timeCache boundedness, MaxBufferBytes flush
/// triggering via the incremental byte counter, and gzip response wrapping.
/// </summary>
public class P2ConvergenceTests : IDisposable
{
    private readonly string _testDir;

    public P2ConvergenceTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p2conv_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    // ---- 5.0 _timeCache boundedness ----

    [Fact]
    public void TimeCache_StaysBounded_AfterManyUniqueTimestamps()
    {
        // The cache is [ThreadStatic]; run everything on one dedicated thread so the
        // reflection reads observe the same dictionary the Time() calls populate.
        Exception? failure = null;
        var thread = new Thread(() =>
        {
            try
            {
                var cacheField = typeof(QueryExecutor).GetField("_timeCache",
                    BindingFlags.NonPublic | BindingFlags.Static);
                Assert.NotNull(cacheField);

                // Invoke Time() with more unique timestamps than the 4096 cap.
                var timeMethod = typeof(QueryExecutor).GetMethod("Time",
                    BindingFlags.NonPublic | BindingFlags.Static);
                Assert.NotNull(timeMethod);
                for (long i = 0; i < 6000; i++)
                    timeMethod.Invoke(null, [i * 1_000_000_000L + i]);

                var cache = (Dictionary<long, string>)cacheField.GetValue(null)!;
                Assert.True(cache.Count <= 4096,
                    $"_timeCache grew to {cache.Count} entries; expected bounded at 4096");
            }
            catch (Exception ex)
            {
                failure = ex;
            }
        });
        thread.Start();
        thread.Join();
        Assert.Null(failure);
    }

    // ---- 5.1 MaxBufferBytes flush trigger via incremental counter ----

    [Fact]
    public async Task MaxBufferBytes_TriggersFlush_UsingIncrementalCounter()
    {
        // flushThreshold is set far above the point count so only the byte-based trigger
        // can fire the flush; this proves the incremental counter drives it. Points are
        // written in small batches because a single batch larger than MaxBufferBytes is
        // rejected by CheckBufferLimit before the flush trigger ever runs (pre-existing
        // semantics).
        using var engine = new TsdbEngine(_testDir,
            flushThreshold: 1_000_000,
            flushIntervalMs: 0,
            compactionIntervalMs: 0,
            rpCheckIntervalMs: 0,
            maxBufferPoints: 1_000_000,
            maxBufferBytes: 20_000); // ~25 pts/batch x ~326 B ≈ 8 KB; flush at >=16 KB

        long timestamp = 0;
        for (var batch = 0; batch < 4; batch++)
        {
            var points = new List<Point>();
            for (int i = 0; i < 25; i++)
            {
                timestamp += 1_000_000_000L;
                points.Add(new Point
                {
                    Measurement = "cpu",
                    Tags = new Dictionary<string, string> { { "host", "h1" } },
                    Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(timestamp) } },
                    TimestampNs = timestamp
                });
            }
            await engine.WriteAsync("db", "autogen", points);
        }

        // The byte trigger must have flushed partway through: buffer drained below the
        // total written count and at least one segment file exists.
        Assert.True(engine.GetBufferedPointCount() < 100,
            $"expected partial flush, buffered={engine.GetBufferedPointCount()}");
        var segFiles = Directory.GetFiles(_testDir, "*.seg", SearchOption.AllDirectories);
        Assert.NotEmpty(segFiles);
    }

    // ---- 5.4 compaction throttle ----

    [Fact]
    public void CompactionThrottle_ConfiguresBudget()
    {
        // The option is plumbed through TsdbEngine -> Compactor; verify the constructor accepts it
        // and that a zero budget (default) keeps the compactor fully functional.
        using var engine = new TsdbEngine(_testDir,
            flushThreshold: 1,
            flushIntervalMs: 0,
            compactionIntervalMs: 0,
            rpCheckIntervalMs: 0,
            compactionMaxWriteBytesPerSecond: 0);
        Assert.NotNull(engine);
        Assert.Equal(0, engine.GetCompactionStats().BacklogTasks);
    }

    // ---- 5.2 gzip response wrapping ----

    static (DefaultHttpContext Context, HttpResponse Response) NewContext(string path, string acceptEncoding)
    {
        var context = new DefaultHttpContext();
        context.Request.Path = path;
        context.Request.Headers.AcceptEncoding = acceptEncoding;
        return (context, context.Response);
    }

    [Fact]
    public void Gzip_WrapsCompressiblePath_WhenAccepted()
    {
        var (context, response) = NewContext("/query", "gzip, deflate");
        using var stream = ResponseCompressionSupport.TryWrap(context.Request, response);
        Assert.NotNull(stream);
        Assert.Equal("gzip", response.Headers.ContentEncoding.ToString());
        Assert.Equal("Accept-Encoding", response.Headers.Vary.ToString());
    }

    [Fact]
    public void Gzip_DoesNotWrap_WithoutAcceptEncoding()
    {
        var (context, response) = NewContext("/query", "identity");
        using var stream = ResponseCompressionSupport.TryWrap(context.Request, response);
        Assert.Null(stream);
        Assert.Equal(0, response.Headers.ContentEncoding.Count);
    }

    [Fact]
    public void Gzip_DoesNotWrap_NonCompressiblePath()
    {
        var (context, response) = NewContext("/write", "gzip");
        using var stream = ResponseCompressionSupport.TryWrap(context.Request, response);
        Assert.Null(stream);
    }

    [Fact]
    public async Task Gzip_WrappedBody_RoundTripsThroughGzipDecompression()
    {
        var (context, response) = NewContext("/query", "gzip");
        var underlying = new MemoryStream();
        response.Body = underlying;

        // Repetitive ASCII payload: compresses well and needs no quote escaping.
        var payload = new string('x', 8192);
        using (var gzip = ResponseCompressionSupport.TryWrap(context.Request, response))
        {
            Assert.NotNull(gzip);
            var bytes = Encoding.UTF8.GetBytes(payload);
            await gzip.WriteAsync(bytes);
        } // dispose flushes the gzip trailer

        Assert.True(underlying.Length > 0);
        Assert.True(underlying.Length < Encoding.UTF8.GetByteCount(payload),
            "repetitive JSON payload should compress smaller");

        underlying.Position = 0;
        using var decompressed = new GZipStream(underlying, CompressionMode.Decompress);
        using var reader = new StreamReader(decompressed, Encoding.UTF8);
        Assert.Equal(payload, reader.ReadToEnd());
    }
}