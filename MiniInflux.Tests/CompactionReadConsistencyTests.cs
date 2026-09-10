using MiniInflux.Net10.Model;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public sealed class CompactionReadConsistencyTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Compaction_ActiveStreamingReader_PreservesInputsAndCount(bool warmMetadata)
    {
        var root = Path.Combine(Path.GetTempPath(), "miniinflux-reader-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var engine = new TsdbEngine(root, flushThreshold: 10, walFsync: false,
                flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0);
            for (var batch = 0; batch < 6; batch++)
            {
                await engine.WriteAsync("db", "autogen", Enumerable.Range(batch * 10, 10).Select(i => new Point
                {
                    Measurement = "cpu", Tags = new() { ["host"] = "a" },
                    Fields = new() { ["value"] = FieldValue.FromInteger(i) }, TimestampNs = i + 1
                }).ToList());
            }
            var query = new QueryExecutor();
            if (warmMetadata) AssertCount(query, engine);
            var inputs = Directory.GetFiles(root, "*.seg", SearchOption.AllDirectories);
            Assert.Equal(6, inputs.Length);
            using var reader = engine.EnumeratePoints("db", "autogen", "cpu", null, null).GetEnumerator();
            Assert.True(reader.MoveNext());
            var compaction = Task.Run(engine.CompactNow);
            try
            {
                // Output exists before publication: encoding proceeds while the reader is paused.
                Assert.True(SpinWait.SpinUntil(() => Directory.GetFiles(root, "l1-*.seg", SearchOption.AllDirectories).Length > 0,
                    TimeSpan.FromSeconds(10)));
                await Task.WhenAny(compaction, Task.Delay(100));
                Assert.False(compaction.IsCompleted);
                Assert.All(inputs, path => Assert.True(File.Exists(path)));
                AssertCount(query, engine); // Nested reads must not deadlock a waiting publisher.
                var values = new List<long> { reader.Current.Fields["value"].Integer };
                while (reader.MoveNext()) values.Add(reader.Current.Fields["value"].Integer);
                Assert.Equal(Enumerable.Range(0, 60).Select(i => (long)i), values);
            }
            finally
            {
                reader.Dispose();
                await compaction.WaitAsync(TimeSpan.FromSeconds(10));
            }
            Assert.True(await compaction.WaitAsync(TimeSpan.FromSeconds(10)) > 0);
            AssertCount(query, engine);
            Assert.Equal(60, engine.ReadAllPoints("db", "autogen", "cpu", null, null).Count);
        }
        finally { if (Directory.Exists(root)) Directory.Delete(root, true); }
    }

    [Fact]
    public async Task SegmentRead_DisposedOnAnotherThread_IsIdempotentAndHonorsCancellation()
    {
        var root = Path.Combine(Path.GetTempPath(), "miniinflux-lease-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var engine = new TsdbEngine(root, flushIntervalMs: 0, compactionIntervalMs: 0, rpCheckIntervalMs: 0);
            var read = engine.AcquireSegmentRead();
            await Task.Run(read.Dispose);
            read.Dispose();
            using var canceled = new CancellationTokenSource();
            canceled.Cancel();
            Assert.Throws<OperationCanceledException>(() => engine.AcquireSegmentRead(canceled.Token));
        }
        finally { if (Directory.Exists(root)) Directory.Delete(root, true); }
    }

    private static void AssertCount(QueryExecutor query, TsdbEngine engine)
    {
        var result = query.ExecuteWithReport(engine, "db", "SELECT count(value) FROM cpu");
        Assert.Null(result.Report.Error);
        Assert.Equal(60L, Convert.ToInt64(result.Response.Results[0].Series![0].Values[0][1]));
    }
}
