using System.Net;
using System.Text;
using Microsoft.AspNetCore.Http;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

/// <summary>
/// Token permission tiers: all（默认，向后兼容）| read | write. Read tokens may query but not
/// write; write tokens may write but not query; all tokens can do everything.
/// </summary>
public sealed class TokenPermissionTests
{
    private static (TokenStore Store, string Raw) CreateToken(string name, string permissions)
    {
        var dir = Path.Combine(Path.GetTempPath(), $"miniinflux_tok_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        var store = new TokenStore(dir);
        var (record, raw) = store.Create(name, permissions);
        return (store, raw);
    }

    private static AuthenticationGuard NewGuard(TokenStore store, bool enabled)
    {
        var guard = new AuthenticationGuard(new AuthOptions
        {
            Enabled = enabled,
            Username = "admin",
            Password = "secret"
        });
        guard.SetTokenStore(store);
        return guard;
    }

    private static HttpRequest RequestWithToken(string raw)
    {
        var context = new DefaultHttpContext();
        context.Connection.RemoteIpAddress = IPAddress.Parse("127.0.0.1");
        context.Request.Headers.Authorization = $"Bearer {raw}";
        return context.Request;
    }

    [Fact]
    public void Evaluate_ReadOnlyToken_GrantsReadOnlyPermission()
    {
        var (store, raw) = CreateToken("readonly", "read");
        var guard = NewGuard(store, enabled: true);

        var attempt = guard.Evaluate(RequestWithToken(raw));

        Assert.Equal(AuthenticationAttemptStatus.Success, attempt.Status);
        Assert.Equal("read", attempt.Permission);
        Assert.True(AuthorizationSupport.PermissionCovers(attempt.Permission, "read"));
        Assert.False(AuthorizationSupport.PermissionCovers(attempt.Permission, "write"));
    }

    [Fact]
    public void Evaluate_WriteOnlyToken_GrantsWriteOnlyPermission()
    {
        var (store, raw) = CreateToken("writeonly", "write");
        var guard = NewGuard(store, enabled: true);

        var attempt = guard.Evaluate(RequestWithToken(raw));

        Assert.Equal("write", attempt.Permission);
        Assert.True(AuthorizationSupport.PermissionCovers(attempt.Permission, "write"));
        Assert.False(AuthorizationSupport.PermissionCovers(attempt.Permission, "read"));
    }

    [Fact]
    public void Evaluate_LegacyTokenWithoutPermissions_GrantsAll()
    {
        var (store, raw) = CreateToken("legacy", "all");
        var guard = NewGuard(store, enabled: true);

        var attempt = guard.Evaluate(RequestWithToken(raw));

        Assert.Equal("all", attempt.Permission);
        Assert.True(AuthorizationSupport.PermissionCovers(attempt.Permission, "read"));
        Assert.True(AuthorizationSupport.PermissionCovers(attempt.Permission, "write"));
    }

    [Fact]
    public void Evaluate_BasicCredentials_GrantAll()
    {
        var guard = NewGuard(new TokenStore(Path.Combine(Path.GetTempPath(), $"miniinflux_tok_{Guid.NewGuid():N}")), enabled: true);

        var context = new DefaultHttpContext();
        context.Connection.RemoteIpAddress = IPAddress.Parse("127.0.0.1");
        context.Request.Headers.Authorization = $"Basic {Convert.ToBase64String(Encoding.UTF8.GetBytes("admin:secret"))}";

        var attempt = guard.Evaluate(context.Request);
        Assert.Equal("all", attempt.Permission);
    }

    [Fact]
    public void TokenStore_CreateWithInvalidPermission_Throws()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"miniinflux_tok_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        var store = new TokenStore(dir);

        Assert.Throws<ArgumentException>(() => store.Create("bad", "root"));
        var (record, _) = store.Create("ok", "READ");
        Assert.Equal("read", record.Permissions);
    }
}
