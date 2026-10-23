using System.IO.Compression;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;

namespace MiniInflux.Tests;

/// <summary>
/// End-to-end HTTP coverage for the write/query surfaces (v1, v2, v3), chunked and gzip bodies,
/// gzip responses and token permissions. Uses the real pipeline in Program.cs.
/// </summary>
public sealed class HttpIntegrationTests : IClassFixture<MiniInfluxAppFactory>
{
    private readonly MiniInfluxAppFactory _factory;

    public HttpIntegrationTests(MiniInfluxAppFactory factory) => _factory = factory;

    [Fact]
    public async Task Write_ThenQueryGet_ReturnsWrittenPoint()
    {
        using var client = _factory.CreateClient();
        using var write = new StringContent("cpu,host=a value=1.5 1000000000", Encoding.UTF8);
        using var writeResponse = await client.PostAsync("/write?db=httpit_write&precision=ns", write);
        Assert.Equal(HttpStatusCode.NoContent, writeResponse.StatusCode);

        using var queryResponse = await client.GetAsync(
            "/query?db=httpit_write&epoch=ns&q=" + Uri.EscapeDataString("SELECT value FROM cpu"));

        Assert.Equal(HttpStatusCode.OK, queryResponse.StatusCode);
        var body = await queryResponse.Content.ReadAsStringAsync();
        Assert.Contains("1000000000", body);
        Assert.Contains("1.5", body);
    }

    [Fact]
    public async Task Write_WithChunkedBody_IsAccepted()
    {
        using var client = _factory.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, "/write?db=httpit_chunked&precision=ns")
        {
            Content = new UnknownLengthContent("mem,host=b value=2i 2000000000\n")
        };

        using var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);

        using var queryResponse = await client.GetAsync(
            "/query?db=httpit_chunked&epoch=ns&q=" + Uri.EscapeDataString("SELECT value FROM mem"));
        var body = await queryResponse.Content.ReadAsStringAsync();
        Assert.Contains("2000000000", body);
    }

    [Fact]
    public async Task Write_WithGzipRequestBody_IsAccepted()
    {
        using var client = _factory.CreateClient();
        using var content = GzipContent("disk,host=c value=3i 3000000000\n", "text/plain");
        using var response = await client.PostAsync("/write?db=httpit_gzip&precision=ns", content);
        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);

        using var queryResponse = await client.GetAsync(
            "/query?db=httpit_gzip&epoch=ns&q=" + Uri.EscapeDataString("SELECT value FROM disk"));
        Assert.Contains("3000000000", await queryResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task V2Write_WithDatabaseBucket_IsQueryable()
    {
        using var client = _factory.CreateClient();
        using var write = new StringContent("net,host=d value=4i 4000000000", Encoding.UTF8);
        using var writeResponse = await client.PostAsync("/api/v2/write?bucket=httpit_v2/autogen&precision=ns", write);
        Assert.Equal(HttpStatusCode.NoContent, writeResponse.StatusCode);

        using var queryResponse = await client.GetAsync(
            "/query?db=httpit_v2&epoch=ns&q=" + Uri.EscapeDataString("SELECT value FROM net"));

        Assert.Equal(HttpStatusCode.OK, queryResponse.StatusCode);
        Assert.Contains("4000000000", await queryResponse.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task V3Query_WithJsonBody_ReturnsResults()
    {
        using var client = _factory.CreateClient();
        using var write = new StringContent("v3m,host=e value=5i 5000000000", Encoding.UTF8);
        using var writeResponse = await client.PostAsync("/write?db=httpit_v3&precision=ns", write);
        Assert.Equal(HttpStatusCode.NoContent, writeResponse.StatusCode);

        using var content = new StringContent(
            "{\"db\":\"httpit_v3\",\"q\":\"SELECT value FROM v3m\",\"epoch\":\"ns\"}", Encoding.UTF8, "application/json");
        using var response = await client.PostAsync("/api/v3/query_influxql", content);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("5000000000", body);
        Assert.Contains("results", body);
    }

    [Fact]
    public async Task Query_WithGzipAcceptEncoding_ReturnsCompressedJson()
    {
        using var client = _factory.CreateClient();
        using var request = new HttpRequestMessage(
            HttpMethod.Get, "/query?db=httpit_gzip&q=" + Uri.EscapeDataString("SHOW DATABASES"));
        request.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));

        using var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("gzip", response.Content.Headers.ContentEncoding.Single());
        Assert.Contains("results", Decompress(await response.Content.ReadAsByteArrayAsync()));
    }

    private static string Decompress(byte[] compressed)
    {
        using var gzip = new GZipStream(new MemoryStream(compressed), CompressionMode.Decompress);
        using var reader = new StreamReader(gzip, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    private static ByteArrayContent GzipContent(string text, string mediaType)
    {
        using var buffer = new MemoryStream();
        using (var gzip = new GZipStream(buffer, CompressionLevel.Fastest, leaveOpen: true))
            gzip.Write(Encoding.UTF8.GetBytes(text));

        var content = new ByteArrayContent(buffer.ToArray());
        content.Headers.ContentType = new MediaTypeHeaderValue(mediaType);
        content.Headers.ContentEncoding.Add("gzip");
        return content;
    }

    private sealed class UnknownLengthContent(string payload) : HttpContent
    {
        protected override Task SerializeToStreamAsync(Stream stream, System.Net.TransportContext? context) =>
            stream.WriteAsync(Encoding.UTF8.GetBytes(payload)).AsTask();

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }
    }
}

/// <summary>Same real pipeline with Basic auth enabled and rate limiting disabled for deterministic 403 checks.</summary>
public sealed class MiniInfluxAuthAppFactory : WebApplicationFactory<Program>
{
    private const string User = "admin";
    private const string Password = "integration-password";
    private readonly string _dataPath = Path.Combine(Path.GetTempPath(), $"miniinflux_http_auth_{Guid.NewGuid():N}");

    public string DataPath => _dataPath;

    protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
    {
        builder.UseSetting("Data:Dir", _dataPath);
        builder.UseSetting("MiniInflux:DataPath", _dataPath);
        builder.UseSetting("Storage:MinFreeDiskBytes", "0");
        builder.UseSetting("Http:LogEnabled", "false");
        builder.UseSetting("Data:QueryLogEnabled", "false");
        builder.UseSetting("Auth:Enabled", "true");
        builder.UseSetting("Auth:Username", User);
        builder.UseSetting("Auth:Password", Password);
        builder.UseSetting("Auth:MaxFailedAttempts", "0");
    }

    /// <summary>Authorization header value for the configured superuser.</summary>
    public static string BasicHeader() =>
        "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes($"{User}:{Password}"));

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

public sealed class HttpAuthorizationTests : IClassFixture<MiniInfluxAuthAppFactory>
{
    private readonly MiniInfluxAuthAppFactory _factory;

    public HttpAuthorizationTests(MiniInfluxAuthAppFactory factory) => _factory = factory;

    [Fact]
    public async Task UnauthenticatedWriteAndQuery_ReturnUnauthorized()
    {
        using var client = _factory.CreateClient();

        using var write = new StringContent("cpu,host=a value=1 1000000000", Encoding.UTF8);
        using var writeResponse = await client.PostAsync("/write?db=httpit_auth", write);
        Assert.Equal(HttpStatusCode.Unauthorized, writeResponse.StatusCode);

        using var queryResponse = await client.GetAsync("/query?q=SHOW%20DATABASES");
        Assert.Equal(HttpStatusCode.Unauthorized, queryResponse.StatusCode);
    }

    [Fact]
    public async Task ReadToken_CanSelectButCannotMutate()
    {
        using var client = _factory.CreateClient();
        var token = await CreateTokenAsync(client, "read-token", "read");
        await SeedAsync(client, "httpit_read", "cpu,host=a value=1 1000000000");

        using var selectResponse = await client.SendAsync(
            AuthorizedGet("/query?db=httpit_read&q=" + Uri.EscapeDataString("SELECT value FROM cpu"), token));
        Assert.Equal(HttpStatusCode.OK, selectResponse.StatusCode);

        using var write = AuthorizedRequest(HttpMethod.Post, "/write?db=httpit_read", token,
            new StringContent("cpu,host=a value=2 2000000000", Encoding.UTF8));
        using var writeResponse = await client.SendAsync(write);
        Assert.Equal(HttpStatusCode.Forbidden, writeResponse.StatusCode);
        Assert.Contains("does not allow", await writeResponse.Content.ReadAsStringAsync());

        using var ddlResponse = await client.SendAsync(AuthorizedGet(
            "/query?q=" + Uri.EscapeDataString("CREATE DATABASE httpit_should_not_exist"), token));
        Assert.Equal(HttpStatusCode.Forbidden, ddlResponse.StatusCode);

        using var intoResponse = await client.SendAsync(AuthorizedGet(
            "/query?q=" + Uri.EscapeDataString("SELECT value INTO httpit_copy FROM cpu"), token));
        Assert.Equal(HttpStatusCode.Forbidden, intoResponse.StatusCode);
    }

    [Fact]
    public async Task WriteToken_CanWriteButCannotQuery()
    {
        using var client = _factory.CreateClient();
        var token = await CreateTokenAsync(client, "write-token", "write");

        using var write = AuthorizedRequest(HttpMethod.Post, "/write?db=httpit_writetoken&precision=ns", token,
            new StringContent("cpu,host=a value=7 7000000000", Encoding.UTF8));
        using var writeResponse = await client.SendAsync(write);
        Assert.Equal(HttpStatusCode.NoContent, writeResponse.StatusCode);

        using var queryResponse = await client.SendAsync(AuthorizedGet("/query?q=SHOW%20DATABASES", token));
        Assert.Equal(HttpStatusCode.Forbidden, queryResponse.StatusCode);
    }

    [Fact]
    public async Task ReadToken_OnV2Write_ReturnsV2ForbiddenShape()
    {
        using var client = _factory.CreateClient();
        var token = await CreateTokenAsync(client, "read-v2", "read");

        using var request = AuthorizedRequest(HttpMethod.Post, "/api/v2/write?bucket=httpit_v2auth", token,
            new StringContent("cpu,host=a value=1 1000000000", Encoding.UTF8));
        using var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("forbidden", json.RootElement.GetProperty("code").GetString());
    }

    [Fact]
    public async Task ReadToken_OnAdminConsoleSelectInto_IsRejected()
    {
        using var client = _factory.CreateClient();
        var token = await CreateTokenAsync(client, "read-console", "read");

        using var content = new FormUrlEncodedContent(
        [
            new KeyValuePair<string, string>("q", "SELECT value INTO httpit_console_copy FROM cpu")
        ]);
        using var request = AuthorizedRequest(HttpMethod.Post, "/admin/api/query", token, content);
        using var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("read-only", await response.Content.ReadAsStringAsync());
    }

    private async Task SeedAsync(HttpClient client, string db, string line)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"/write?db={db}&precision=ns")
        {
            Content = new StringContent(line, Encoding.UTF8)
        };
        request.Headers.TryAddWithoutValidation("Authorization", MiniInfluxAuthAppFactory.BasicHeader());
        using var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    private static async Task<string> CreateTokenAsync(HttpClient client, string name, string permissions)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/admin/api/tokens")
        {
            Content = new StringContent(
                $"{{\"name\":\"{name}-{Guid.NewGuid():N}\",\"permissions\":\"{permissions}\"}}",
                Encoding.UTF8,
                "application/json")
        };
        request.Headers.TryAddWithoutValidation("Authorization", MiniInfluxAuthAppFactory.BasicHeader());
        using var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return json.RootElement.GetProperty("token").GetString()!;
    }

    private static HttpRequestMessage AuthorizedGet(string path, string token) =>
        AuthorizedRequest(HttpMethod.Get, path, token, content: null);

    private static HttpRequestMessage AuthorizedRequest(HttpMethod method, string path, string token, HttpContent? content)
    {
        var request = new HttpRequestMessage(method, path) { Content = content };
        request.Headers.TryAddWithoutValidation("Authorization", "Token " + token);
        return request;
    }
}
