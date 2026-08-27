using System.IO.Compression;
using Microsoft.AspNetCore.Http;

namespace MiniInflux.Net10;

/// <summary>
/// Lightweight gzip response compression for text/JSON endpoints. Opt-in per request via
/// <c>Accept-Encoding: gzip</c>; only wraps known compressible paths so streaming/static
/// endpoints keep their exact semantics. The wrapper writes the gzip header lazily (on the
/// first body write), so empty bodies (204/304) stay byte-for-byte empty.
/// </summary>
public static class ResponseCompressionSupport
{
    /// <summary>Path prefixes whose JSON/text responses are safe to gzip.</summary>
    private static readonly string[] CompressiblePrefixes =
    [
        "/query",
        "/metrics",
        "/debug/stats",
        "/debug/benchmark",
        "/admin/api",
        "/api/v3/query_influxql"
    ];

    /// <summary>
    /// Wrap the response body in a gzip stream when the request accepts gzip and the path is
    /// compressible. Returns the wrapper (caller must dispose it to flush the gzip trailer) or
    /// null when the response should pass through untouched.
    /// </summary>
    public static GZipStream? TryWrap(HttpRequest request, HttpResponse response)
    {
        if (response.HasStarted)
            return null;
        if (!AcceptsGzip(request))
            return null;
        if (!IsCompressiblePath(request.Path))
            return null;
        // Never double-encode; never touch responses that already carry an encoding.
        if (response.Headers.ContentEncoding.Count > 0)
            return null;

        response.Headers.ContentEncoding = "gzip";
        response.Headers.Vary = "Accept-Encoding";
        // Compressed length is unknown up front; drop any pre-set Content-Length.
        response.Headers.ContentLength = null;

        return new GZipStream(response.Body, CompressionLevel.Fastest, leaveOpen: true);
    }

    public static bool AcceptsGzip(HttpRequest request)
    {
        // Do not treat `gzip;q=0` as accepted: clients use it to explicitly forbid gzip.
        // A simple substring check also matched unrelated parameters such as `x-gzip`.
        foreach (var entry in request.Headers.AcceptEncoding.ToString().Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = entry.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 0 || !string.Equals(parts[0], "gzip", StringComparison.OrdinalIgnoreCase))
                continue;

            foreach (var parameter in parts.Skip(1))
            {
                if (parameter.StartsWith("q=", StringComparison.OrdinalIgnoreCase)
                    && double.TryParse(parameter[2..], System.Globalization.NumberStyles.AllowDecimalPoint,
                        System.Globalization.CultureInfo.InvariantCulture, out var quality)
                    && quality <= 0)
                    return false;
            }
            return true;
        }
        return false;
    }

    public static bool IsCompressiblePath(PathString path)
    {
        foreach (var prefix in CompressiblePrefixes)
            if (path.StartsWithSegments(prefix))
                return true;
        return false;
    }
}
