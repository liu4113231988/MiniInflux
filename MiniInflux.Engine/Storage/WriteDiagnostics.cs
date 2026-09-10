using System.Diagnostics;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Opt-in, process-wide batch timings. Nested/concurrent stages overlap: totals must not
/// be added together or interpreted as CPU time. Enable before startup for a profiling run.
/// </summary>
public static class WriteDiagnostics
{
    public static bool Enabled { get; } = string.Equals(
        Environment.GetEnvironmentVariable("MiniInflux__WriteDiagnostics"), "true", StringComparison.OrdinalIgnoreCase);

    public enum Stage
    {
        WriteLockWait, WriteLocked, WalAppend, BufferAppend, FlushSnapshot,
        FlushEncode, FlushCleanupWait, FlushCleanup, SegmentColumns,
        TimestampEncode, ValueEncode, SegmentPersist,
        WalEncode, WalCrc, WalLockWait, WalFileWrite, WalRotate, WalFsync
    }

    private static readonly Stage[] stages = Enum.GetValues<Stage>();
    private static readonly long[] ticks = new long[stages.Length];
    private static readonly long[] counts = new long[stages.Length];

    public static Scope Measure(Stage stage) => Enabled ? new(stage, Stopwatch.GetTimestamp()) : default;

    public readonly struct Scope(Stage stage, long started) : IDisposable
    {
        public void Dispose()
        {
            if (started == 0) return;
            Interlocked.Add(ref ticks[(int)stage], Stopwatch.GetTimestamp() - started);
            Interlocked.Increment(ref counts[(int)stage]);
        }
    }

    public static Dictionary<string, double> Milliseconds() => stages.ToDictionary(
        stage => stage.ToString(), stage => Interlocked.Read(ref ticks[(int)stage]) * 1000.0 / Stopwatch.Frequency);

    public static Dictionary<string, long> Counts() => stages.ToDictionary(
        stage => stage.ToString(), stage => Interlocked.Read(ref counts[(int)stage]));
}
