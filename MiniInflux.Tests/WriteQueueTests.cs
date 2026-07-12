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

    public void Dispose()
    {
        if (Directory.Exists(_dataPath))
            Directory.Delete(_dataPath, true);
    }
}
