using MiniInflux.Net10.Model;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public sealed class WriteQueueTests : IDisposable
{
    private readonly string _dataPath = Path.Combine(Path.GetTempPath(), $"miniinflux_queue_{Guid.NewGuid():N}");

    [Fact]
    public async Task EnqueueAsync_ValidPoint_WritesThroughWorker()
    {
        using var engine = new TsdbEngine(_dataPath, flushIntervalMs: 0, compactionIntervalMs: 0);
        using var queue = new WriteQueue(engine, capacity: 1, batchSize: 1);
        var point = new Point
        {
            Measurement = "cpu",
            Tags = [],
            Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromInteger(1) },
            TimestampNs = 1
        };

        await queue.EnqueueAsync("db", "autogen", [point]);

        Assert.Equal(1, engine.GetBufferedPointCount());
    }

    [Fact]
    public async Task EnqueueAsync_ConcurrentWritesDuringPeriodicFlush_CompletesWithoutDeadlock()
    {
        using var engine = new TsdbEngine(
            _dataPath,
            flushThreshold: 25,
            flushIntervalMs: 1,
            compactionIntervalMs: 0);
        using var queue = new WriteQueue(engine, capacity: 500, batchSize: 50);

        var writes = Enumerable.Range(1, 200).Select(timestamp => queue.EnqueueAsync(
            "db",
            "autogen",
            [new Point
            {
                Measurement = "cpu",
                Tags = [],
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(timestamp) },
                TimestampNs = timestamp
            }]));

        var results = await Task.WhenAll(writes).WaitAsync(TimeSpan.FromSeconds(15));

        Assert.All(results, Assert.True);
        engine.FlushAll();
        Assert.Equal(200, engine.ReadAllPoints("db", "autogen", "cpu", null, null).Count);
    }

    [Fact]
    public async Task Dispose_WithAcceptedRequests_CompletesEveryWaiter()
    {
        using var engine = new TsdbEngine(_dataPath, flushIntervalMs: 0, compactionIntervalMs: 0);
        var queue = new WriteQueue(engine, capacity: 256, batchSize: 256);
        var requests = Enumerable.Range(1, 256)
            .Select(timestamp => queue.EnqueueAsync("db", "autogen",
            [
                new Point
                {
                    Measurement = "cpu",
                    Tags = [],
                    Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromInteger(timestamp) },
                    TimestampNs = timestamp
                }
            ]))
            .ToArray();

        Assert.True(SpinWait.SpinUntil(() => queue.PendingRequests == requests.Length, TimeSpan.FromSeconds(5)));

        var dispose = Task.Run(queue.Dispose);
        var results = await Task.WhenAll(requests).WaitAsync(TimeSpan.FromSeconds(10));
        await dispose.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.All(results, Assert.True);
        Assert.Equal(0, queue.PendingRequests);
        Assert.Equal(256, engine.ReadAllPoints("db", "autogen", "cpu", null, null).Count);
    }

    [Fact]
    public async Task EnqueueAsync_ExceedsPointsBudget_ThrowsImmediatelyWithoutWriting()
    {
        using var engine = new TsdbEngine(_dataPath, flushIntervalMs: 0, compactionIntervalMs: 0);
        using var queue = new WriteQueue(engine, capacity: 100, batchSize: 1, maxQueuedPoints: 5);

        var points = Enumerable.Range(1, 10)
            .Select(i => new Point
            {
                Measurement = "cpu",
                Tags = [],
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromInteger(i) },
                TimestampNs = i
            })
            .ToList();

        await Assert.ThrowsAsync<WriteQueueFullException>(() => queue.EnqueueAsync("db", "autogen", points));
        Assert.Equal(0, engine.GetBufferedPointCount());
        Assert.Equal(0, queue.QueuedPoints);
    }

    [Fact]
    public async Task EnqueueAsync_ExceedsBytesBudget_ThrowsImmediatelyWithoutWriting()
    {
        using var engine = new TsdbEngine(_dataPath, flushIntervalMs: 0, compactionIntervalMs: 0);
        using var queue = new WriteQueue(engine, capacity: 100, batchSize: 1, maxQueuedBytes: 64);

        var points = Enumerable.Range(1, 50)
            .Select(i => new Point
            {
                Measurement = "cpu",
                Tags = [],
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromInteger(i) },
                TimestampNs = i
            })
            .ToList();

        await Assert.ThrowsAsync<WriteQueueFullException>(() => queue.EnqueueAsync("db", "autogen", points));
        Assert.Equal(0, engine.GetBufferedPointCount());
        Assert.Equal(0, queue.QueuedBytes);
    }

    [Fact]
    public async Task EnqueueAsync_WithinBudget_WritesAndResetsCounters()
    {
        using var engine = new TsdbEngine(_dataPath, flushIntervalMs: 0, compactionIntervalMs: 0);
        using var queue = new WriteQueue(engine, capacity: 100, batchSize: 1, maxQueuedPoints: 1000, maxQueuedBytes: 10_000_000);

        var point = new Point
        {
            Measurement = "cpu",
            Tags = [],
            Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromInteger(1) },
            TimestampNs = 1
        };

        await queue.EnqueueAsync("db", "autogen", [point]);

        Assert.True(SpinWait.SpinUntil(() => queue.QueuedPoints == 0, TimeSpan.FromSeconds(5)));
        Assert.Equal(0, queue.QueuedBytes);
        Assert.Equal(1, engine.GetBufferedPointCount());
    }

    public void Dispose()
    {
        if (Directory.Exists(_dataPath))
            Directory.Delete(_dataPath, true);
    }
}
