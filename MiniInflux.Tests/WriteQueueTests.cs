using MiniInflux.Net10.Model;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public sealed class WriteQueueTests : IDisposable
{
    private readonly string _dataPath = Path.Combine(Path.GetTempPath(), $"miniinflux_queue_{Guid.NewGuid():N}");

    private static Point MakePoint(long value) => new()
    {
        Measurement = "cpu",
        Tags = [],
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromInteger(value) },
        TimestampNs = value
    };

    // TsdbEngine subclass that blocks WriteInternalAsync until explicitly released, so tests can
    // deterministically hold the worker and leave items sitting in the channel.
    private sealed class BlockingEngine : TsdbEngine
    {
        private readonly SemaphoreSlim _gate = new(0);
        public BlockingEngine(string path) : base(path, flushIntervalMs: 0, compactionIntervalMs: 0) { }
        public override async Task WriteInternalAsync(string db, string rp, List<Point> pts)
        {
            await _gate.WaitAsync().ConfigureAwait(false);
            await base.WriteInternalAsync(db, rp, pts).ConfigureAwait(false);
        }
        public void Release(int count = 1) => _gate.Release(count);
    }

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

    [Fact]
    public async Task EnqueueAsync_MultipleDatabases_WritesAllWithoutDeadlock()
    {
        using var engine = new TsdbEngine(_dataPath, flushThreshold: 100, flushIntervalMs: 0, compactionIntervalMs: 0);
        // Multiple workers route by db hash; different dbs may land on different workers.
        using var queue = new WriteQueue(engine, capacity: 1000, batchSize: 50, workerCount: 4);

        var writes = new List<Task>();
        for (var db = 0; db < 8; db++)
            for (var i = 0; i < 50; i++)
                writes.Add(queue.EnqueueAsync($"db{db}", "autogen", [new Point
                {
                    Measurement = "cpu",
                    Tags = [],
                    Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(i) },
                    TimestampNs = i
                }]));

        await Task.WhenAll(writes).WaitAsync(TimeSpan.FromSeconds(15));

        for (var db = 0; db < 8; db++)
            Assert.Equal(50, engine.ReadAllPoints($"db{db}", "autogen", "cpu", null, null).Count);
    }

    [Fact]
    public async Task EnqueueAsync_CallerCancelledBeforeEnqueue_ThrowsOperationCanceledException_ReleasesBudget()
    {
        using var engine = new TsdbEngine(_dataPath, flushIntervalMs: 0, compactionIntervalMs: 0);
        using var queue = new WriteQueue(engine, capacity: 100, batchSize: 1, maxQueuedPoints: 1000);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => queue.EnqueueAsync("db", "autogen", [MakePoint(1)], cts.Token));

        Assert.Equal(0, queue.QueuedPointsRaw);
        Assert.Equal(0, queue.QueuedBytesRaw);
        Assert.Equal(0, queue.PendingRequestsRaw);
    }

    [Fact]
    public async Task EnqueueAsync_CallerCancelledAfterEnqueue_ThrowsOperationCanceledException_BudgetReleasedOnce()
    {
        using var engine = new BlockingEngine(_dataPath);
        using var queue = new WriteQueue(engine, capacity: 8, batchSize: 1, maxQueuedPoints: 1000);

        // Block the worker on the first write so the second item stays queued.
        var occupy = queue.EnqueueAsync("db", "autogen", [MakePoint(1)]);
        Assert.True(SpinWait.SpinUntil(() => queue.PendingRequestsRaw == 1, TimeSpan.FromSeconds(5)));

        using var cts = new CancellationTokenSource();
        var second = queue.EnqueueAsync("db", "autogen", [MakePoint(2)], cts.Token);
        // WriteAsync to a non-full channel completes synchronously: item is now enqueued.
        Assert.True(SpinWait.SpinUntil(() => queue.PendingRequestsRaw == 2, TimeSpan.FromSeconds(5)));

        // Cancel after enqueue: surfaces OCE (not WriteQueueFullException) and does NOT release
        // the budget here — the worker owns it via ProcessBatch finally.
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => second);

        engine.Release(2);
        await occupy;

        Assert.True(SpinWait.SpinUntil(() => queue.PendingRequestsRaw == 0, TimeSpan.FromSeconds(5)));
        Assert.Equal(0, queue.QueuedPointsRaw);
        Assert.Equal(0, queue.QueuedBytesRaw);
    }

    [Fact]
    public async Task EnqueueAsync_QueueFullTimeout_ThrowsWriteQueueFullException_ReleasesBudget()
    {
        using var engine = new BlockingEngine(_dataPath);
        using var queue = new WriteQueue(engine, capacity: 1, batchSize: 1, maxQueuedPoints: 1000);

        var occupy = queue.EnqueueAsync("db", "autogen", [MakePoint(1)]);
        Assert.True(SpinWait.SpinUntil(() => queue.PendingRequestsRaw == 1, TimeSpan.FromSeconds(5)));

        // Fill the single channel slot (worker is blocked on item 1).
        var fill = queue.EnqueueAsync("db", "autogen", [MakePoint(2)]);
        Assert.True(SpinWait.SpinUntil(() => queue.PendingRequestsRaw == 2, TimeSpan.FromSeconds(5)));

        // Channel is full: the third write blocks on WriteAsync and times out as retryable full.
        await Assert.ThrowsAsync<WriteQueueFullException>(
            () => queue.EnqueueAsync("db", "autogen", [MakePoint(3)]));

        // The timed-out request never entered the channel, so its budget was released by the caller.
        Assert.True(queue.QueuedPointsRaw >= 0);

        engine.Release(2);
        await Task.WhenAll(occupy, fill);
        Assert.True(SpinWait.SpinUntil(() => queue.PendingRequestsRaw == 0, TimeSpan.FromSeconds(5)));
        Assert.Equal(0, queue.QueuedPointsRaw);
    }

    [Fact]
    public async Task Dispose_WithCancelledPendingRequests_BudgetReturnsToZero()
    {
        using var engine = new BlockingEngine(_dataPath);
        var queue = new WriteQueue(engine, capacity: 8, batchSize: 1, maxQueuedPoints: 1000);

        var occupy = queue.EnqueueAsync("db", "autogen", [MakePoint(1)]);
        Assert.True(SpinWait.SpinUntil(() => queue.PendingRequestsRaw == 1, TimeSpan.FromSeconds(5)));

        using var cts = new CancellationTokenSource();
        var pending = new List<Task<bool>>();
        for (var i = 0; i < 3; i++)
            pending.Add(queue.EnqueueAsync("db", "autogen", [MakePoint(i + 2)], cts.Token));
        Assert.True(SpinWait.SpinUntil(() => queue.PendingRequestsRaw == 4, TimeSpan.FromSeconds(5)));

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Task.WhenAll(pending));

        engine.Release(4);
        await occupy;
        queue.Dispose();

        Assert.True(SpinWait.SpinUntil(() => queue.PendingRequestsRaw == 0, TimeSpan.FromSeconds(5)));
        Assert.Equal(0, queue.QueuedPointsRaw);
        Assert.Equal(0, queue.QueuedBytesRaw);
    }

    [Fact]
    public async Task EnqueueAsync_ConcurrentCancellation_BudgetNeverNegative()
    {
        using var engine = new TsdbEngine(_dataPath, flushIntervalMs: 0, compactionIntervalMs: 0);
        using var queue = new WriteQueue(engine, capacity: 1024, batchSize: 32,
            maxQueuedPoints: 100_000, maxQueuedBytes: 100_000_000);

        var rng = new Random(42);
        var tasks = new List<Task>();
        for (var i = 0; i < 300; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                using var cts = new CancellationTokenSource();
                if (rng.Next(2) == 0)
                    cts.CancelAfter(rng.Next(2, 80));
                try
                {
                    await queue.EnqueueAsync("db", "autogen", [MakePoint(i)], cts.Token);
                }
                catch (OperationCanceledException) { }
                catch (WriteQueueFullException) { }
            }));
        }

        long minPoints = long.MaxValue, minBytes = long.MaxValue;
        using var samplerCts = new CancellationTokenSource();
        var sampler = Task.Run(async () =>
        {
            try
            {
                while (!samplerCts.IsCancellationRequested)
                {
                    minPoints = Math.Min(minPoints, queue.QueuedPointsRaw);
                    minBytes = Math.Min(minBytes, queue.QueuedBytesRaw);
                    await Task.Delay(1, samplerCts.Token);
                }
            }
            catch (OperationCanceledException) { }
        });

        await Task.WhenAll(tasks);
        samplerCts.Cancel();
        await sampler;

        Assert.True(minPoints >= 0, $"QueuedPoints went negative: {minPoints}");
        Assert.True(minBytes >= 0, $"QueuedBytes went negative: {minBytes}");
        Assert.True(SpinWait.SpinUntil(() => queue.QueuedPointsRaw == 0, TimeSpan.FromSeconds(10)));
        Assert.Equal(0, queue.QueuedBytesRaw);
    }

    public void Dispose()
    {
        if (Directory.Exists(_dataPath))
            Directory.Delete(_dataPath, true);
    }
}