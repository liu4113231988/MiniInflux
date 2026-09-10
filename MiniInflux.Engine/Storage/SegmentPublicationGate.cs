namespace MiniInflux.Net10.Storage;

// Leases are not thread-affine: streaming responses can resume on another thread.
// Nested readers are allowed even when a publisher is waiting. Blocking them would
// deadlock a query that calls another engine read while retaining its outer lease.
internal sealed class SegmentPublicationGate
{
    private readonly object sync = new();
    private int readers;
    private bool publishing;

    public IDisposable Read(CancellationToken cancellationToken = default)
    {
        lock (sync)
        {
            while (publishing)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Monitor.Wait(sync, 50);
            }
            cancellationToken.ThrowIfCancellationRequested();
            readers++;
            return new Lease(this, false);
        }
    }

    public IDisposable Publish()
    {
        lock (sync)
        {
            while (publishing || readers != 0) Monitor.Wait(sync);
            publishing = true;
            return new Lease(this, true);
        }
    }

    private sealed class Lease(SegmentPublicationGate owner, bool writer) : IDisposable
    {
        private SegmentPublicationGate? owner = owner;
        public void Dispose()
        {
            var gate = Interlocked.Exchange(ref owner, null);
            if (gate == null) return;
            lock (gate.sync)
            {
                if (writer) gate.publishing = false;
                else gate.readers--;
                Monitor.PulseAll(gate.sync);
            }
        }
    }
}
