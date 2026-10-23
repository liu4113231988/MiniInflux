using System.IO.Compression;
using Microsoft.AspNetCore.Http;

namespace MiniInflux.Net10;

/// <summary>
/// Lightweight gzip response compression for text/JSON endpoints. Opt-in per request via
/// <c>Accept-Encoding: gzip</c>; only wraps known compressible paths so streaming/static
/// endpoints keep their exact semantics. The encoding is advertised up front (so clients and
/// intermediaries observe the negotiated encoding immediately); when the endpoint writes no body
/// at all, the header is retracted again before the response starts, so empty bodies
/// (204/304/bodyless errors) never carry a gzip header without gzip bytes.
/// </summary>
public static class ResponseCompressionSupport
{
    /// <summary>HttpContext.Items key carrying the number of compressed body bytes written.</summary>
    public const string WrittenBytesItemKey = "miniinflux.response.written_bytes";

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
    /// Wrap the response body in a lazily-started gzip stream when the request accepts gzip and
    /// the path is compressible. Returns the wrapper (caller must dispose it to flush the gzip
    /// trailer) or null when the response should pass through untouched.
    /// </summary>
    public static Stream? TryWrap(HttpRequest request, HttpResponse response)
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

        // Advertise gzip up front so intermediaries and clients see the negotiated encoding.
        // The wrapper removes the header again when no body byte was written, so an empty
        // response never claims a gzip encoding it does not have.
        response.Headers.ContentEncoding = "gzip";
        response.Headers.Vary = "Accept-Encoding";
        // Compressed length is unknown up front; drop any pre-set Content-Length.
        response.Headers.ContentLength = null;

        return new LazyGzipStream(response, response.Body);
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

    // The target stream is captured eagerly: the middleware assigns the wrapper to
    // HttpResponse.Body, so resolving response.Body lazily would make the gzip stream write
    // into itself.
    private sealed class LazyGzipStream(HttpResponse response, Stream target) : Stream
    {
        private GZipStream? _gzip;
        private CountingStream? _counter;
        private bool _disposed;

        private GZipStream Gzip
        {
            get
            {
                if (_gzip != null) return _gzip;

                // The encoding header was already advertised by TryWrap; the body is the
                // compressed payload from here on.
                response.Headers.ContentLength = null;
                _counter = new CountingStream(target);
                _gzip = new GZipStream(_counter, CompressionLevel.Fastest, leaveOpen: true);
                return _gzip;
            }
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() => _gzip?.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => _gzip?.FlushAsync(cancellationToken) ?? Task.CompletedTask;
        public override void Write(byte[] buffer, int offset, int count) => Gzip.Write(buffer, offset, count);
        public override void Write(ReadOnlySpan<byte> buffer) => Gzip.Write(buffer);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            Gzip.WriteAsync(buffer, offset, count, cancellationToken);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
            Gzip.WriteAsync(buffer, cancellationToken);
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                DisposeAsync().AsTask().GetAwaiter().GetResult();
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;
            _disposed = true;

            if (_gzip != null)
            {
                await _gzip.DisposeAsync();
                response.HttpContext.Items[WrittenBytesItemKey] = _counter!.BytesWritten;
            }

            // The endpoint produced no body at all: the eagerly advertised gzip encoding
            // would be a lie (and some clients fail to decode an empty gzip body), so the
            // header is retracted before the response starts.
            if (!response.HasStarted && _counter == null)
            {
                response.Headers.Remove("Content-Encoding");
                response.Headers.ContentLength = null;
            }

            await base.DisposeAsync();
        }
    }

    private sealed class CountingStream(Stream inner) : Stream
    {
        public long BytesWritten { get; private set; }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() => inner.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);
        public override void Write(byte[] buffer, int offset, int count)
        {
            inner.Write(buffer, offset, count);
            BytesWritten += count;
        }
        public override void Write(ReadOnlySpan<byte> buffer)
        {
            inner.Write(buffer);
            BytesWritten += buffer.Length;
        }
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            BytesWritten += count;
            return inner.WriteAsync(buffer, offset, count, cancellationToken);
        }
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            BytesWritten += buffer.Length;
            return inner.WriteAsync(buffer, cancellationToken);
        }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }
}
