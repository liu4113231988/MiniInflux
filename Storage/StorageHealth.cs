namespace MiniInflux.Net10.Storage;

public sealed class StorageHealth
{
    private long _failureCount;
    private string? _lastFailure;
    private string? _lastFailureComponent;
    private DateTimeOffset? _lastFailureUtc;

    public bool WriteAvailable { get; private set; } = true;
    public long FailureCount => Interlocked.Read(ref _failureCount);
    public string? LastFailure => Volatile.Read(ref _lastFailure);
    public string? LastFailureComponent => Volatile.Read(ref _lastFailureComponent);
    public DateTimeOffset? LastFailureUtc => _lastFailureUtc;

    public void RecordFailure(string component, Exception exception, bool blocksWrites = false)
    {
        Interlocked.Increment(ref _failureCount);
        Volatile.Write(ref _lastFailureComponent, component);
        Volatile.Write(ref _lastFailure, exception.Message);
        _lastFailureUtc = DateTimeOffset.UtcNow;
        if (blocksWrites)
            WriteAvailable = false;
    }

    public void RecordWriteSuccess() => WriteAvailable = true;
}
