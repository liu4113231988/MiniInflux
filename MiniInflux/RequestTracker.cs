using System.Collections.Concurrent;
using System.Diagnostics;

namespace MiniInflux.Net10;

/// <summary>
/// Tracks in-flight HTTP requests for the /debug/requests endpoint.
/// Entries are keyed by HttpContext.TraceIdentifier and removed when the
/// request completes (even on fault), so the snapshot never leaks entries.
/// </summary>
public sealed class RequestTracker
{
    private readonly ConcurrentDictionary<string, ActiveRequest> _active = new(StringComparer.Ordinal);

    public void Start(string id, string method, string path)
    {
        _active[id] = new ActiveRequest(method, path, Stopwatch.GetTimestamp());
    }

    public void End(string id)
    {
        _active.TryRemove(id, out _);
    }

    public List<ActiveRequestSnapshot> GetActive()
    {
        var now = Stopwatch.GetTimestamp();
        var result = new List<ActiveRequestSnapshot>(_active.Count);
        foreach (var kvp in _active)
        {
            var elapsedMs = (now - kvp.Value.StartTimestamp) * 1000.0 / Stopwatch.Frequency;
            result.Add(new ActiveRequestSnapshot { Method = kvp.Value.Method, Path = kvp.Value.Path, DurationMs = elapsedMs });
        }
        return result;
    }

    public int ActiveCount => _active.Count;

    private sealed record ActiveRequest(string Method, string Path, long StartTimestamp);
}

public sealed class ActiveRequestSnapshot
{
    public string Method { get; set; } = "";
    public string Path { get; set; } = "";
    public double DurationMs { get; set; }
}
