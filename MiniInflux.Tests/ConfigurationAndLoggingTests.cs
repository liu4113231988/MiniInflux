using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public sealed class ConfigurationAndLoggingTests : IDisposable
{
    private readonly string _testDir;

    public ConfigurationAndLoggingTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_config_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public void MiniInfluxOptions_LoadsInfluxStyleSections()
    {
        var settings = new Dictionary<string, string?>
        {
            ["Data:Dir"] = "./custom-data",
            ["Data:QueryLogEnabled"] = "false",
            ["Http:Enabled"] = "true",
            ["Http:BindAddress"] = ":18086",
            ["Http:LogEnabled"] = "true",
            ["Http:SuppressWriteLog"] = "true",
            ["Http:AccessLogPath"] = "./logs/access.log",
            ["Http:AccessLogStatusFilters:0"] = "4xx",
            ["Http:WriteTracing"] = "true",
            ["Http:AuthEnabled"] = "true",
            ["Auth:Enabled"] = "true",
            ["Auth:Username"] = "root",
            ["Auth:Password"] = "secret",
            ["Auth:AuditFailures"] = "false",
            ["Auth:MaxFailedAttempts"] = "3",
            ["Auth:FailureWindowMs"] = "15000",
            ["Auth:LockoutMs"] = "45000",
            ["Logging:Level"] = "Debug",
            ["Logging:ConsoleEnabled"] = "false",
            ["Logging:FileEnabled"] = "true",
            ["Logging:FilePath"] = "./logs/app.log"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(settings)
            .Build();

        var options = MiniInfluxOptions.Load(configuration);

        Assert.Equal("./custom-data", options.DataPath);
        Assert.False(options.Data.QueryLogEnabled);
        Assert.Equal(":18086", options.Http.BindAddress);
        Assert.Equal("http://0.0.0.0:18086", options.Urls);
        Assert.True(options.Http.SuppressWriteLog);
        Assert.Single(options.Http.AccessLogStatusFilters);
        Assert.True(options.Auth.Enabled);
        Assert.Equal("root", options.Auth.Username);
        Assert.Equal("secret", options.Auth.Password);
        Assert.False(options.Auth.AuditFailures);
        Assert.Equal(3, options.Auth.MaxFailedAttempts);
        Assert.Equal(15_000, options.Auth.FailureWindowMs);
        Assert.Equal(45_000, options.Auth.LockoutMs);
        Assert.Equal("Debug", options.Logging.Level);
        Assert.True(options.Logging.FileEnabled);
        Assert.False(options.Logging.ConsoleEnabled);
    }

    [Fact]
    public void MiniInfluxOptions_PrefersAuthEnabledOverLegacyHttpAuthEnabled()
    {
        var settings = new Dictionary<string, string?>
        {
            ["Http:AuthEnabled"] = "false",
            ["Auth:Enabled"] = "true",
            ["Auth:Username"] = "admin",
            ["Auth:Password"] = "secret"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(settings)
            .Build();

        var options = MiniInfluxOptions.Load(configuration);

        Assert.True(options.Auth.Enabled);
        Assert.True(options.Http.AuthEnabled);
    }

    [Fact]
    public void MiniInfluxOptions_StillSupportsLegacyHttpAuthEnabled()
    {
        var settings = new Dictionary<string, string?>
        {
            ["Http:AuthEnabled"] = "true",
            ["Auth:Username"] = "admin",
            ["Auth:Password"] = "secret"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(settings)
            .Build();

        var options = MiniInfluxOptions.Load(configuration);

        Assert.True(options.Auth.Enabled);
        Assert.True(options.Http.AuthEnabled);
    }

    [Fact]
    public void HttpLoggingSupport_HonorsWriteSuppression_AndStatusFilters()
    {
        var options = new HttpOptions
        {
            LogEnabled = true,
            SuppressWriteLog = true,
            AccessLogStatusFilters = ["4xx", "503"]
        };

        Assert.False(HttpLoggingSupport.ShouldLogRequest(options, "/write", 204));
        Assert.True(HttpLoggingSupport.ShouldLogRequest(options, "/query", 404));
        Assert.True(HttpLoggingSupport.ShouldLogRequest(options, "/query", 503));
        Assert.False(HttpLoggingSupport.ShouldLogRequest(options, "/query", 200));
    }

    [Fact]
    public void FileLoggerProvider_WritesLinesToConfiguredFile()
    {
        var logPath = Path.Combine(_testDir, "app.log");
        using (var provider = new FileLoggerProvider(logPath))
        {
            var logger = provider.CreateLogger("MiniInflux.Tests");
            logger.Log(LogLevel.Information, new EventId(12, "test"), "hello world", null,
                static (state, _) => state);
        }

        var text = File.ReadAllText(logPath);
        Assert.Contains("hello world", text);
        Assert.Contains("MiniInflux.Tests", text);
    }
}
