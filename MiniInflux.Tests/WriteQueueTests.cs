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

    public void Dispose()
    {
        if (Directory.Exists(_dataPath))
            Directory.Delete(_dataPath, true);
    }
}
