using System.Net;
using Microsoft.AspNetCore.Http;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public sealed class TokenStoreTests : IDisposable
{
    private readonly string _dir;
    public TokenStoreTests() { _dir = Path.Combine(Path.GetTempPath(), $"tok_{Guid.NewGuid():N}"); Directory.CreateDirectory(_dir); }
    public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, true); }

    [Fact]
    public void Create_List_Revoke_Persist()
    {
        var store = new TokenStore(_dir);
        Assert.Empty(store.List());
        var (rec, raw) = store.Create("ci-token");
        Assert.NotEmpty(raw);
        Assert.StartsWith("mini_", raw);
        Assert.Equal("ci-token", rec.Name);
        Assert.NotEmpty(rec.Prefix);
        var list = store.List();
        Assert.Single(list);
        Assert.Null(store.Validate("wrong"));
        Assert.NotNull(store.Validate(raw));
        // name duplicate should throw 409 equivalent
        Assert.Throws<InvalidOperationException>(() => store.Create("ci-token"));
        // invalid name
        Assert.Throws<ArgumentException>(() => store.Create(""));
        Assert.Throws<ArgumentException>(() => store.Create("bad name!"));

        Assert.True(store.Revoke(rec.Id));
        Assert.Empty(store.List());
        Assert.Null(store.Validate(raw));
        Assert.False(store.Revoke(rec.Id));

        // persistence: new store reloads empty after revoke
        var store2 = new TokenStore(_dir);
        Assert.Empty(store2.List());

        var (_, raw2) = store2.Create("second");
        var store3 = new TokenStore(_dir);
        Assert.Single(store3.List());
        Assert.NotNull(store3.Validate(raw2));
    }

    [Fact]
    public void AuthenticationGuard_Bearer_WithTokenStore_AndBasicCoexists()
    {
        var store = new TokenStore(_dir);
        var (_, raw) = store.Create("my-token");
        var guard = new AuthenticationGuard(new AuthOptions { Username = "admin", Password = "secret", MaxFailedAttempts = 0 });
        guard.SetTokenStore(store);

        // Bearer via named token succeeds
        Assert.Equal(AuthenticationAttemptStatus.Success, guard.Evaluate(BearerRequest(raw)).Status);
        Assert.Equal(AuthenticationAttemptStatus.Success, guard.Evaluate(TokenRequest(raw)).Status);
        // Basic via admin still succeeds
        Assert.Equal(AuthenticationAttemptStatus.Success, guard.Evaluate(BasicRequest("admin", "secret")).Status);
        // Wrong token fails
        Assert.Equal(AuthenticationAttemptStatus.InvalidCredentials, guard.Evaluate(BearerRequest("mini_wrongtoken1234567890")).Status);
        // Revoked token fails
        var id = store.List()[0].Id;
        store.Revoke(id);
        Assert.Equal(AuthenticationAttemptStatus.InvalidCredentials, guard.Evaluate(BearerRequest(raw)).Status);
        // Admin password as Bearer still works (fallback)
        Assert.Equal(AuthenticationAttemptStatus.Success, guard.Evaluate(BearerRequest("secret")).Status);
        Assert.Equal(AuthenticationAttemptStatus.Success, guard.Evaluate(TokenRequest("admin:secret")).Status);
    }

    [Fact]
    public void TokenStore_Hash_IsNotPlaintext()
    {
        var store = new TokenStore(_dir);
        var (_, raw) = store.Create("t1");
        var rec = store.List()[0];
        Assert.NotEqual(raw, rec.TokenHash);
        Assert.NotEmpty(rec.TokenHash);
        Assert.Equal(raw[..8], rec.Prefix);
        // file should not contain raw token
        var file = Path.Combine(_dir, "meta", "tokens.json");
        var content = File.ReadAllText(file);
        Assert.DoesNotContain(raw, content);
        Assert.Contains("TokenHash", content);
        // reload verifies hash persisted correctly
        var store2 = new TokenStore(_dir);
        Assert.Equal(rec.TokenHash, store2.List()[0].TokenHash);
        Assert.NotNull(store2.Validate(raw));
    }

    private static HttpRequest BearerRequest(string token)
    {
        var ctx = new DefaultHttpContext();
        ctx.Connection.RemoteIpAddress = IPAddress.Parse("127.0.0.1");
        ctx.Request.Headers.Authorization = $"Bearer {token}";
        return ctx.Request;
    }
    private static HttpRequest TokenRequest(string token)
    {
        var ctx = new DefaultHttpContext();
        ctx.Connection.RemoteIpAddress = IPAddress.Parse("127.0.0.1");
        ctx.Request.Headers.Authorization = $"Token {token}";
        return ctx.Request;
    }
    private static HttpRequest BasicRequest(string user, string pass)
    {
        var ctx = new DefaultHttpContext();
        ctx.Connection.RemoteIpAddress = IPAddress.Parse("127.0.0.1");
        var raw = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{user}:{pass}"));
        ctx.Request.Headers.Authorization = $"Basic {raw}";
        return ctx.Request;
    }
}
