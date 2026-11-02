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
/// Bounded write queue with backpressure. Uses Channel to limit concurrent writes and optional
/// point/byte budgets to cap the in-flight memory held by queued (not yet processed) requests.
/// </summary>
public sealed class WriteQueue : IDisposable
{
    private readonly Channel<WriteRequest> _channel;
    private readonly TsdbEngine _engine;
    private readonly int _batchSize;
    private readonly long _maxQueuedPoints;
    private readonly long _maxQueuedBytes;
    private readonly Task _worker;
    private readonly CancellationTokenSource _cts = new();
    private long _pendingRequests;
    private long _queuedPoints;
    private long _queuedBytes;

    public WriteQueue(TsdbEngine engine, int capacity = 100_000, int batchSize = 10_000,
        long maxQueuedPoints = 0, long maxQueuedBytes = 0)
    {
        _engine = engine;
        _batchSize = batchSize;
        _maxQueuedPoints = maxQueuedPoints;
        _maxQueuedBytes = maxQueuedBytes;
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
    /// Throws WriteQueueFullException when the configured points/bytes budget would be exceeded.
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

        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = new WriteRequest(db, rp, points, pointCount, estimatedBytes, tcs);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(5));

        Interlocked.Increment(ref _pendingRequests);
        using var registration = ct.Register(static state => ((TaskCompletionSource<bool>)state!).TrySetCanceled(), tcs);
        try
        {
            await _channel.Writer.WriteAsync(request, timeoutCts.Token);
            return await tcs.Task;
        }
        catch (OperationCanceledException)
        {
            // If the request never entered the channel the worker won't process it, so release
            // the budget and pending counter here. If it was already enqueued before cancellation
            // the worker releases them in ProcessBatch's finally block.
            if (!_channel.Reader.TryPeek(out var peeked) || !ReferenceEquals(peeked, request))
            {
                Interlocked.Add(ref _queuedPoints, -pointCount);
                Interlocked.Add(ref _queuedBytes, -estimatedBytes);
                Interlocked.Decrement(ref _pendingRequests);
            }
            throw new WriteQueueFullException("Write queue is full or shutting down");
        }
        catch (ChannelClosedException)
        {
            Interlocked.Add(ref _queuedPoints, -pointCount);
            Interlocked.Add(ref _queuedBytes, -estimatedBytes);
            Interlocked.Decrement(ref _pendingRequests);
            throw new WriteQueueFullException("Write queue is shutting down");
        }
    }

    private async Task ProcessAsync()
    {
        var batch = new List<WriteRequest>(_batchSize);

        try
        {
            while (await _channel.Reader.WaitToReadAsync(_cts.Token).ConfigureAwait(false))
            {
                while (batch.Count < _batchSize && _channel.Reader.TryRead(out var next))
                    batch.Add(next);
                if (batch.Count == 0) continue;

                await ProcessBatch(batch);
                batch.Clear();
            }
        }
        catch (OperationCanceledException) { /* shutdown: fall through and drain */ }

        try
        {
            while (_channel.Reader.TryRead(out var remaining))
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
        _channel.Writer.Complete();
        try { _worker.Wait(TimeSpan.FromSeconds(5)); } catch { /* best effort */ }
        try { _cts.Cancel(); } catch (ObjectDisposedException) { }
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
