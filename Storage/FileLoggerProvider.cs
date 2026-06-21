using Microsoft.Extensions.Logging;

namespace MiniInflux.Net10.Storage;

public sealed class FileLoggerProvider : ILoggerProvider
{
    private readonly FileLogSink _sink;

    public FileLoggerProvider(string path)
    {
        _sink = new FileLogSink(path);
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
        private readonly StreamWriter _writer;

        public FileLogSink(string path)
        {
            var fullPath = Path.GetFullPath(path);
            Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
            _writer = new StreamWriter(new FileStream(fullPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
            {
                AutoFlush = true
            };
        }

        public void Write(string line)
        {
            lock (_lock)
                _writer.WriteLine(line);
        }

        public void Dispose()
        {
            lock (_lock)
                _writer.Dispose();
        }
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose() { }
    }
}
