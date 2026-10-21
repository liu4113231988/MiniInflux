using System.Net;
using System.Text;

namespace MiniInflux.Tests;

public sealed class HttpAdminJsonTests : IClassFixture<MiniInfluxAppFactory>
{
    private readonly MiniInfluxAppFactory _factory;

    public HttpAdminJsonTests(MiniInfluxAppFactory factory) => _factory = factory;

    [Theory]
    [InlineData("/admin/api/backup")]
    [InlineData("/admin/api/restore")]
    [InlineData("/admin/api/tokens")]
    public async Task AdminJsonEndpoint_MalformedBody_ReturnsBadRequest(string endpoint)
    {
        using var client = _factory.CreateClient();
        using var content = new StringContent("{", Encoding.UTF8, "application/json");

        using var response = await client.PostAsync(endpoint, content);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("malformed JSON body", await response.Content.ReadAsStringAsync());
    }
}
