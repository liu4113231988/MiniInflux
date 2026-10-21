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

        Interlocked.Increment(ref _pendingRequests);
        using var registration = ct.Register(static state => ((TaskCompletionSource<bool>)state!).TrySetCanceled(), tcs);
        try
        {
            await _channel.Writer.WriteAsync(request, timeoutCts.Token);
            return await tcs.Task;
        }
        catch (OperationCanceledException)
        {
            // Request never entered the channel — the worker won't process it, so decrement here.
            // If the request was already enqueued before cancellation, the worker will process it
            // and decrement _pendingRequests in ProcessBatch's finally block.
            if (!_channel.Reader.TryPeek(out var peeked) || !ReferenceEquals(peeked, request))
                Interlocked.Decrement(ref _pendingRequests);
            throw new WriteQueueFullException("Write queue is full or shutting down");
        }
        catch (ChannelClosedException)
        {
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

        // Process remaining items and resolve every completion source; otherwise a caller that
        // already enqueued a request would await its TCS forever after Dispose cancels the worker.
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
        finally { Interlocked.Add(ref _pendingRequests, -batch.Count); }
    }

    public long PendingRequests => Math.Max(0, Interlocked.Read(ref _pendingRequests));

    public void Dispose()
    {
        // Complete the channel first so the worker drains and resolves queued completions; only
        // cancel afterwards to unblock a worker that is stuck on a slow write.
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
