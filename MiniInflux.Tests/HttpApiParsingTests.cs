using System.Net;
using System.Text;

namespace MiniInflux.Tests;

public sealed class HttpApiParsingTests : IClassFixture<MiniInfluxAppFactory>
{
    private readonly MiniInfluxAppFactory _factory;

    public HttpApiParsingTests(MiniInfluxAppFactory factory) => _factory = factory;

    [Fact]
    public async Task V3Query_ChunkedJsonBody_IsParsed()
    {
        using var client = _factory.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/v3/query_influxql")
        {
            Content = new UnknownLengthJsonContent("{\"db\":\"testdb\",\"q\":\"SHOW DATABASES\"}")
        };

        using var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task V3Query_MalformedJsonBody_ReturnsBadRequest()
    {
        using var client = _factory.CreateClient();
        using var content = new StringContent("{", Encoding.UTF8, "application/json");

        using var response = await client.PostAsync("/api/v3/query_influxql", content);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("malformed JSON body", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Query_PostFormFormatCsv_ReturnsCsvContent()
    {
        using var client = _factory.CreateClient();
        using var content = new FormUrlEncodedContent(
        [
            new KeyValuePair<string, string>("db", "testdb"),
            new KeyValuePair<string, string>("q", "SHOW DATABASES"),
            new KeyValuePair<string, string>("format", "csv")
        ]);

        using var response = await client.PostAsync("/query", content);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/csv", response.Content.Headers.ContentType?.MediaType);
    }

    private sealed class UnknownLengthJsonContent : HttpContent
    {
        private readonly byte[] _payload;

        public UnknownLengthJsonContent(string json)
        {
            _payload = Encoding.UTF8.GetBytes(json);
            Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
        }

        protected override Task SerializeToStreamAsync(Stream stream, System.Net.TransportContext? context) =>
            stream.WriteAsync(_payload).AsTask();

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }

    }
}
