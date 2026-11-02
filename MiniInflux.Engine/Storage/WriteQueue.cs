using System.Threading.Channels;
using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Represents a write request to be processed by the write queue.
/// </summary>
public sealed record WriteRequest(
    string Db, string Rp, List<Point> Points, int PointCount, long EstimatedBytes,
    TaskCompletionSource<bool> Completion);

/// <summary>
/// Bounded write queue with backpressure. Writes are routed by database hash to a fixed pool of
/// workers: the same database always lands on the same worker (so its writes stay ordered), while
/// different databases can be processed in parallel. Optional point/byte budgets cap the in-flight
/// memory held by queued (not yet processed) requests.
/// </summary>
public sealed class WriteQueue : IDisposable
{
    private readonly Channel<WriteRequest>[] _channels;
    private readonly TsdbEngine _engine;
    private readonly int _batchSize;
    private readonly long _maxQueuedPoints;
    private readonly long _maxQueuedBytes;
    private readonly Task[] _workers;
    private readonly CancellationTokenSource _cts = new();
    private long _pendingRequests;
    private long _queuedPoints;
    private long _queuedBytes;

    public WriteQueue(TsdbEngine engine, int capacity = 100_000, int batchSize = 10_000,
        long maxQueuedPoints = 0, long maxQueuedBytes = 0, int workerCount = 0)
    {
        _engine = engine;
        _batchSize = batchSize;
        _maxQueuedPoints = maxQueuedPoints;
        _maxQueuedBytes = maxQueuedBytes;
        var workers = workerCount > 0 ? workerCount : Math.Max(1, Environment.ProcessorCount);
        _channels = new Channel<WriteRequest>[workers];
        _workers = new Task[workers];
        for (var i = 0; i < workers; i++)
        {
            _channels[i] = Channel.CreateBounded<WriteRequest>(new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true
            });
            var index = i;
            _workers[i] = Task.Run(() => ProcessAsync(index));
        }
    }

    /// <summary>
    /// Enqueue a write request. Throws ChannelClosedException if the queue is shutting down.
    /// Times out after 5 seconds if the queue is full (backpressure), throwing WriteQueueFullException.
    /// Throws WriteQueueFullException when the configured points/bytes budget would be exceeded.
    /// Throws OperationCanceledException when the caller's token is cancelled (not retryable).
    /// </summary>
    public async Task<bool> EnqueueAsync(string db, string rp, List<Point> points, CancellationToken ct = default)
    {
        var pointCount = points.Count;
        var estimatedBytes = EstimateQueuedBytes(points);

        // Reserve the points/bytes budget before touching the channel so overload is reported
        // immediately instead of blocking. The budget is released by the worker once processed,
        // or by this method if the request never reaches the worker.
        long curPoints = Interlocked.Add(ref _queuedPoints, pointCount);
        long curBytes = Interlocked.Add(ref _queuedBytes, estimatedBytes);
        if ((_maxQueuedPoints > 0 && curPoints > _maxQueuedPoints) ||
            (_maxQueuedBytes > 0 && curBytes > _maxQueuedBytes))
        {
            Interlocked.Add(ref _queuedPoints, -pointCount);
            Interlocked.Add(ref _queuedBytes, -estimatedBytes);
            throw new WriteQueueFullException("write queue points/bytes limit exceeded; retry later");
        }

        // Route by database: same db always hits the same worker so its writes stay ordered;
        // different dbs spread across workers and run in parallel.
        var channel = _channels[(uint)db.GetHashCode() % _channels.Length];

        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = new WriteRequest(db, rp, points, pointCount, estimatedBytes, tcs);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(5));

        Interlocked.Increment(ref _pendingRequests);
        using var registration = ct.Register(static state => ((TaskCompletionSource<bool>)state!).TrySetCanceled(), tcs);

        // `enqueued` records whether WriteAsync successfully handed the item to the channel.
        // Budget/pending ownership: if enqueued, the worker's ProcessBatch finally releases them;
        // otherwise this method must release them exactly once.
        bool enqueued = false;
        try
        {
            await channel.Writer.WriteAsync(request, timeoutCts.Token);
            enqueued = true;
            return await tcs.Task;
        }
        catch (OperationCanceledException)
        {
            if (!enqueued)
            {
                // WriteAsync never delivered the item; release budget here.
                Interlocked.Add(ref _queuedPoints, -pointCount);
                Interlocked.Add(ref _queuedBytes, -estimatedBytes);
                Interlocked.Decrement(ref _pendingRequests);

                // Caller cancellation must surface as OperationCanceledException so callers don't
                // retry (which could double-write). A queue-full timeout is retryable.
                if (ct.IsCancellationRequested)
                    throw;
                throw new WriteQueueFullException("write queue full; retry later");
            }
            // Item already enqueued: worker owns budget release. Caller cancelled while waiting.
            throw;
        }
        catch (ChannelClosedException)
        {
            Interlocked.Add(ref _queuedPoints, -pointCount);
            Interlocked.Add(ref _queuedBytes, -estimatedBytes);
            Interlocked.Decrement(ref _pendingRequests);
            throw new WriteQueueFullException("write queue is shutting down");
        }
    }

    private async Task ProcessAsync(int workerIndex)
    {
        var channel = _channels[workerIndex];
        var batch = new List<WriteRequest>(_batchSize);

        try
        {
            while (await channel.Reader.WaitToReadAsync(_cts.Token).ConfigureAwait(false))
            {
                while (batch.Count < _batchSize && channel.Reader.TryRead(out var next))
                    batch.Add(next);
                if (batch.Count == 0) continue;

                await ProcessBatch(batch);
                batch.Clear();
            }
        }
        catch (OperationCanceledException) { /* shutdown: fall through and drain */ }

        try
        {
            while (channel.Reader.TryRead(out var remaining))
                batch.Add(remaining);
            if (batch.Count > 0)
                await ProcessBatch(batch);
        }
        catch (Exception ex)
        {
            foreach (var request in batch)
                request.Completion.TrySetException(ex);
        }
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
        finally
        {
            long points = 0, bytes = 0;
            foreach (var req in batch)
            {
                points += req.PointCount;
                bytes += req.EstimatedBytes;
            }
            Interlocked.Add(ref _queuedPoints, -points);
            Interlocked.Add(ref _queuedBytes, -bytes);
            Interlocked.Add(ref _pendingRequests, -batch.Count);
        }
    }

    public long PendingRequests => Math.Max(0, Interlocked.Read(ref _pendingRequests));
    public long QueuedPoints => Math.Max(0, Interlocked.Read(ref _queuedPoints));
    public long QueuedBytes => Math.Max(0, Interlocked.Read(ref _queuedBytes));

    // Raw counters for tests: the public properties clamp negatives to 0, which would hide a
    // double-release bug. These expose the true underlying values.
    internal long PendingRequestsRaw => Interlocked.Read(ref _pendingRequests);
    internal long QueuedPointsRaw => Interlocked.Read(ref _queuedPoints);
    internal long QueuedBytesRaw => Interlocked.Read(ref _queuedBytes);

    private static long EstimateQueuedBytes(List<Point> points)
    {
        long size = 0;
        foreach (var p in points)
        {
            long s = 96 + EstimateStringBytes(p.Measurement) + 8;
            foreach (var tag in p.Tags)
                s += 32 + EstimateStringBytes(tag.Key) + EstimateStringBytes(tag.Value);
            foreach (var field in p.Fields)
                s += 48 + EstimateStringBytes(field.Key) + EstimateFieldValueBytes(field.Value);
            size += s;
        }
        return size;
    }

    private static long EstimateStringBytes(string? s) => string.IsNullOrEmpty(s) ? 0 : (long)s.Length * 2 + 24;
    private static long EstimateFieldValueBytes(FieldValue v) => v.Kind switch
    {
        FieldKind.String => EstimateStringBytes(v.String),
        FieldKind.Boolean => 1,
        _ => 8
    };

    public void Dispose()
    {
        foreach (var ch in _channels)
            ch.Writer.TryComplete();
        try { Task.WaitAll(_workers, TimeSpan.FromSeconds(5)); } catch { /* best effort */ }
        try { _cts.Cancel(); } catch (ObjectDisposedException) { }
        try { Task.WaitAll(_workers, TimeSpan.FromSeconds(5)); } catch { /* best effort */ }
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
