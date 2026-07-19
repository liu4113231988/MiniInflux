using System.Threading.Channels;
using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Represents a write request to be processed by the write queue.
/// </summary>
public sealed record WriteRequest(string Db, string Rp, List<Point> Points, TaskCompletionSource<bool> Completion);

/// <summary>
/// Bounded write queue with backpressure. Uses Channel to limit concurrent writes.
/// </summary>
public sealed class WriteQueue : IDisposable
{
    private readonly Channel<WriteRequest> _channel;
    private readonly TsdbEngine _engine;
    private readonly int _batchSize;
    private readonly Task _worker;
    private readonly CancellationTokenSource _cts = new();
    private long _pendingRequests;

    public WriteQueue(TsdbEngine engine, int capacity = 100_000, int batchSize = 10_000)
    {
        _engine = engine;
        _batchSize = batchSize;
        _channel = Channel.CreateBounded<WriteRequest>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true
        });
        _worker = Task.Run(ProcessAsync);
    }

    /// <summary>
    /// Enqueue a write request. Throws ChannelClosedException if the queue is shutting down.
    /// Times out after 5 seconds if the queue is full (backpressure).
    /// </summary>
    public async Task<bool> EnqueueAsync(string db, string rp, List<Point> points, CancellationToken ct = default)
    {
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = new WriteRequest(db, rp, points, tcs);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(5));

        try
        {
            Interlocked.Increment(ref _pendingRequests);
            await _channel.Writer.WriteAsync(request, timeoutCts.Token);
            return await tcs.Task;
        }
        catch (OperationCanceledException)
        {
            Interlocked.Decrement(ref _pendingRequests);
            throw new WriteQueueFullException("Write queue is full or shutting down");
        }
    }

    private async Task ProcessAsync()
    {
        var batch = new List<WriteRequest>(_batchSize);

        await foreach (var request in _channel.Reader.ReadAllAsync(_cts.Token))
        {
            batch.Add(request);

            // Drain available items up to batch size
            while (batch.Count < _batchSize && _channel.Reader.TryRead(out var next))
                batch.Add(next);

            await ProcessBatch(batch);
            batch.Clear();
        }

        // Process remaining items
        while (_channel.Reader.TryRead(out var remaining))
            batch.Add(remaining);
        if (batch.Count > 0)
            await ProcessBatch(batch);
    }

    private async Task ProcessBatch(List<WriteRequest> batch)
    {
        var grouped = new Dictionary<(string Db, string Rp), List<WriteRequest>>();
        foreach (var req in batch)
        {
            var key = (req.Db, req.Rp);
            if (!grouped.TryGetValue(key, out var requests))
            {
                requests = [];
                grouped[key] = requests;
            }

            requests.Add(req);
        }

        try
        {
            foreach (var entry in grouped)
            {
                var requests = entry.Value;
                try
                {
                    List<Point> mergedPoints;
                    if (requests.Count == 1)
                    {
                        mergedPoints = requests[0].Points;
                    }
                    else
                    {
                        var pointCount = requests.Sum(x => x.Points.Count);
                        mergedPoints = new List<Point>(pointCount);
                        foreach (var req in requests)
                            mergedPoints.AddRange(req.Points);
                    }

                    await _engine.WriteInternalAsync(entry.Key.Db, entry.Key.Rp, mergedPoints);
                    foreach (var req in requests)
                        req.Completion.TrySetResult(true);
                }
                catch (Exception ex)
                {
                    foreach (var req in requests)
                        req.Completion.TrySetException(ex);
                }
            }
        }
        finally { Interlocked.Add(ref _pendingRequests, -batch.Count); }
    }

    public long PendingRequests => Math.Max(0, Interlocked.Read(ref _pendingRequests));

    public void Dispose()
    {
        _cts.Cancel();
        _channel.Writer.Complete();
        try { _worker.Wait(TimeSpan.FromSeconds(5)); } catch { /* best effort */ }
        _cts.Dispose();
    }
}

/// <summary>
/// Thrown when the write queue is full and cannot accept more writes.
/// </summary>
public sealed class WriteQueueFullException : Exception
{
    public WriteQueueFullException(string message) : base(message) { }
}
