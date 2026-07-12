using Microsoft.Extensions.Logging;

namespace MiniInflux.Net10.Storage;

public sealed class FileLoggerProvider : ILoggerProvider
{
    private readonly FileLogSink _sink;

    public FileLoggerProvider(string path, long maxBytes = 10 * 1024 * 1024, int retainedFileCount = 5)
    {
        _sink = new FileLogSink(path, maxBytes, retainedFileCount);
    }

    public ILogger CreateLogger(string categoryName) => new FileLogger(categoryName, _sink);

    public void Dispose() => _sink.Dispose();

    private sealed class FileLogger : ILogger
    {
        private readonly string _categoryName;
        private readonly FileLogSink _sink;

        public FileLogger(string categoryName, FileLogSink sink)
        {
            _categoryName = categoryName;
            _sink = sink;
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
                return;

            var message = formatter(state, exception);
            _sink.Write(FormatLine(logLevel, _categoryName, eventId, message, exception));
        }

        private static string FormatLine(LogLevel logLevel, string category, EventId eventId, string message, Exception? exception)
        {
            var line = $"{DateTimeOffset.UtcNow:O} [{logLevel}] {category}";
            if (eventId.Id != 0)
                line += $"({eventId.Id})";
            line += $": {message}";
            if (exception != null)
                line += Environment.NewLine + exception;
            return line;
        }
    }

    private sealed class FileLogSink : IDisposable
    {
        private readonly object _lock = new();
        private readonly string _path;
        private readonly long _maxBytes;
        private readonly int _retainedFileCount;
        private StreamWriter _writer;

        public FileLogSink(string path, long maxBytes, int retainedFileCount)
        {
            _path = Path.GetFullPath(path);
            _maxBytes = maxBytes;
            _retainedFileCount = retainedFileCount;
            Directory.CreateDirectory(Path.GetDirectoryName(_path)!);
            _writer = OpenWriter();
        }

        public void Write(string line)
        {
            lock (_lock)
            {
                if (_maxBytes > 0 && new FileInfo(_path).Length >= _maxBytes)
                    Rotate();
                _writer.WriteLine(line);
            }
        }

        public void Dispose()
        {
            lock (_lock)
                _writer.Dispose();
        }

        private StreamWriter OpenWriter() => new(new FileStream(_path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { AutoFlush = true };

        private void Rotate()
        {
            _writer.Dispose();
            for (var index = _retainedFileCount - 1; index >= 1; index--)
            {
                var source = $"{_path}.{index}";
                var destination = $"{_path}.{index + 1}";
                if (File.Exists(source))
                    File.Move(source, destination, overwrite: true);
            }
            if (File.Exists(_path))
                File.Move(_path, _path + ".1", overwrite: true);
            _writer = OpenWriter();
        }
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose() { }
    }
}
