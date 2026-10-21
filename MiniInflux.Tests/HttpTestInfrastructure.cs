using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Microsoft.AspNetCore.Mvc.Testing;

namespace MiniInflux.Tests;

/// <summary>
/// Spins up the real MiniInflux HTTP pipeline (top-level statements in <c>MiniInflux/Program.cs</c>)
/// on an isolated temporary data directory so ingestion/query parsing can be tested end to end.
/// </summary>
public sealed class MiniInfluxAppFactory : WebApplicationFactory<Program>
{
    private readonly string _dataPath = Path.Combine(Path.GetTempPath(), $"miniinflux_http_{Guid.NewGuid():N}");

    protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
    {
        builder.UseSetting("Data:Dir", _dataPath);
        builder.UseSetting("MiniInflux:DataPath", _dataPath);
        builder.UseSetting("Storage:MinFreeDiskBytes", "0");
        builder.UseSetting("Http:LogEnabled", "false");
        builder.UseSetting("Data:QueryLogEnabled", "false");
        builder.UseSetting("Auth:Enabled", "false");
    }

    /// <summary>Absolute path of the data directory backing this host.</summary>
    public string DataPath => _dataPath;

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (!disposing)
            return;

        try
        {
            if (Directory.Exists(_dataPath))
                Directory.Delete(_dataPath, recursive: true);
            var lockPath = _dataPath + ".miniinflux.lock";
            if (File.Exists(lockPath))
                File.Delete(lockPath);
        }
        catch (IOException)
        {
            // Best effort cleanup; a leftover temp directory must not fail the test run.
        }
    }
}

public sealed class HttpSmokeTests : IClassFixture<MiniInfluxAppFactory>
{
    private readonly MiniInfluxAppFactory _factory;

    public HttpSmokeTests(MiniInfluxAppFactory factory) => _factory = factory;

    [Fact]
    public async Task Ping_ReturnsNoContent()
    {
        var client = _factory.CreateClient();
        var response = await client.GetAsync("/ping");
        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    [Fact]
    public async Task Health_ReturnsReady()
    {
        var client = _factory.CreateClient();
        var response = await client.GetAsync("/health");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("ready", body);
    }
}
