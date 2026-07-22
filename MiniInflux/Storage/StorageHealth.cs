namespace MiniInflux.Net10.Storage;

public sealed class StorageHealth
{
    private long _failureCount;
    private string? _lastFailure;
    private string? _lastFailureComponent;
    private DateTimeOffset? _lastFailureUtc;
    private volatile int _writeAvailable = 1; // 1 = true, 0 = false

    public bool WriteAvailable => _writeAvailable == 1;
    public long FailureCount => Interlocked.Read(ref _failureCount);
    public string? LastFailure => Volatile.Read(ref _lastFailure);
    public string? LastFailureComponent => Volatile.Read(ref _lastFailureComponent);
    public DateTimeOffset? LastFailureUtc => Interlocked.CompareExchange(ref _lastFailureUtc, null, null);

    public void RecordFailure(string component, Exception exception, bool blocksWrites = false)
    {
        Interlocked.Increment(ref _failureCount);
        Volatile.Write(ref _lastFailureComponent, component);
        Volatile.Write(ref _lastFailure, exception.Message);
        Interlocked.Exchange(ref _lastFailureUtc, DateTimeOffset.UtcNow);
        if (blocksWrites)
            Interlocked.Exchange(ref _writeAvailable, 0);
    }

    public void RecordWriteSuccess() => Interlocked.Exchange(ref _writeAvailable, 1);
}
