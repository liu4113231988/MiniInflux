using Microsoft.AspNetCore.Http;

namespace MiniInflux.Net10.Storage;

public static class HttpLoggingSupport
{
    public static bool ShouldLogRequest(HttpOptions options, PathString path, int statusCode)
    {
        if (!options.LogEnabled)
            return false;

        if (options.SuppressWriteLog && path.StartsWithSegments("/write"))
            return false;

        if (options.AccessLogStatusFilters.Count == 0)
            return true;

        return options.AccessLogStatusFilters.Any(filter => MatchesStatusFilter(filter, statusCode));
    }

    public static string FormatAccessLogLine(HttpContext context, long elapsedMs)
    {
        var remoteIp = context.Connection.RemoteIpAddress?.ToString() ?? "-";
        var method = context.Request.Method;
        var target = context.Request.Path + context.Request.QueryString;
        var protocol = context.Request.Protocol;
        var status = context.Response.StatusCode;
        var length = context.Response.ContentLength ?? 0;
        return $"{DateTimeOffset.UtcNow:O} {remoteIp} \"{method} {target} {protocol}\" {status} {length} {elapsedMs}ms";
    }

    private static bool MatchesStatusFilter(string filter, int statusCode)
    {
        var trimmed = filter.Trim();
        if (string.IsNullOrWhiteSpace(trimmed))
            return false;

        if (trimmed.EndsWith("xx", StringComparison.OrdinalIgnoreCase) && trimmed.Length == 3 && char.IsDigit(trimmed[0]))
            return statusCode / 100 == trimmed[0] - '0';

        return int.TryParse(trimmed, out var exact) && exact == statusCode;
    }
}

public sealed class AccessLogWriter : IDisposable
{
    private readonly object _lock = new();
    private readonly StreamWriter? _writer;

    public AccessLogWriter(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return;

        var fullPath = Path.GetFullPath(path);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        _writer = new StreamWriter(new FileStream(fullPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
        {
            AutoFlush = true
        };
    }

    public bool Enabled => _writer != null;

    public void Write(string line)
    {
        if (_writer == null)
            return;

        lock (_lock)
            _writer.WriteLine(line);
    }

    public void Dispose()
    {
        if (_writer == null)
            return;

        lock (_lock)
            _writer.Dispose();
    }
}
