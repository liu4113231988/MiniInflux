namespace MiniInflux.Net10.Storage;

public sealed class StorageHealth
{
    private long _failureCount;
    private long _lastDiskCheckTicks;
    private volatile int _lastDiskCheckResult = 1; // 1 = sufficient, 0 = below floor
    private string? _lastFailure;
    private string? _lastFailureComponent;
    private long _lastFailureUtcMilliseconds; // 0 = none; Interlocked works on long, not structs
    private volatile int _writeAvailable = 1; // 1 = true, 0 = false

    public bool WriteAvailable => _writeAvailable == 1;
    public long FailureCount => Interlocked.Read(ref _failureCount);
    public string? LastFailure => Volatile.Read(ref _lastFailure);
    public string? LastFailureComponent => Volatile.Read(ref _lastFailureComponent);
    public DateTimeOffset? LastFailureUtc
    {
        get
        {
            var ms = Interlocked.Read(ref _lastFailureUtcMilliseconds);
            return ms == 0 ? null : DateTimeOffset.FromUnixTimeMilliseconds(ms);
        }
    }

    public void RecordFailure(string component, Exception exception, bool blocksWrites = false)
    {
        Interlocked.Increment(ref _failureCount);
        Volatile.Write(ref _lastFailureComponent, component);
        Volatile.Write(ref _lastFailure, exception.Message);
        Interlocked.Exchange(ref _lastFailureUtcMilliseconds, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        if (blocksWrites)
            Interlocked.Exchange(ref _writeAvailable, 0);
    }

    public void RecordWriteSuccess() => Interlocked.Exchange(ref _writeAvailable, 1);

    private Func<long>? _diskProbe;
    private long _diskFloorBytes;

    /// <summary>Wire the disk-space probe (drive free bytes) and floor; called once at engine start.</summary>
    public void SetDiskSpaceProbe(Func<long> availableBytesProvider, long minFreeDiskBytes)
    {
        _diskProbe = availableBytesProvider;
        _diskFloorBytes = Math.Max(0, minFreeDiskBytes);
    }

    /// <summary>
    /// Cached (refreshed at most every 5 seconds) check that free disk space is above the configured
    /// floor, so writes fail fast with a clear error instead of tripping the WAL into a latched
    /// failure state. A failing probe (e.g. unknown drive) never blocks writes.
    /// </summary>
    public bool IsDiskSpaceSufficient()
    {
        var probe = _diskProbe;
        if (probe == null || _diskFloorBytes <= 0) return true;
        var now = Environment.TickCount64;
        if (now - Volatile.Read(ref _lastDiskCheckTicks) >= 5000)
        {
            bool sufficient;
            try { sufficient = probe() >= _diskFloorBytes; }
            catch { sufficient = true; }
            Volatile.Write(ref _lastDiskCheckResult, sufficient ? 1 : 0);
            Volatile.Write(ref _lastDiskCheckTicks, now);
        }
        return Volatile.Read(ref _lastDiskCheckResult) == 1;
    }

    /// <summary>True while writes are latched off by a storage failure.</summary>
    public bool NeedsRecoveryProbe => _writeAvailable == 0;

    /// <summary>Run a recovery probe and unlatch writes when it succeeds.</summary>
    public bool TryRecover(Func<bool> probe)
    {
        if (Volatile.Read(ref _writeAvailable) == 1) return true;
        if (probe())
        {
            Interlocked.Exchange(ref _writeAvailable, 1);
            return true;
        }
        return false;
    }
}
