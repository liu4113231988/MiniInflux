using System.Net;
using Microsoft.AspNetCore.Http;

namespace MiniInflux.Tests;

public sealed class AuthenticationGuardTests
{
    [Fact]
    public void AuthenticationGuard_DoesNotCountMissingCredentials_AsFailure()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 6, 26, 0, 0, 0, TimeSpan.Zero));
        var guard = new AuthenticationGuard(new AuthOptions
        {
            Username = "admin",
            Password = "secret",
            MaxFailedAttempts = 2,
            FailureWindowMs = 60_000,
            LockoutMs = 120_000
        }, clock);

        var missing = guard.Evaluate(CreateRequest("127.0.0.1"));
        var success = guard.Evaluate(CreateRequest("127.0.0.1", "admin", "secret"));

        Assert.Equal(AuthenticationAttemptStatus.MissingCredentials, missing.Status);
        Assert.Equal(AuthenticationAttemptStatus.Success, success.Status);
    }

    [Fact]
    public void AuthenticationGuard_LocksClientAfterRepeatedFailures_AndExpires()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 6, 26, 0, 0, 0, TimeSpan.Zero));
        var guard = new AuthenticationGuard(new AuthOptions
        {
            Username = "admin",
            Password = "secret",
            MaxFailedAttempts = 2,
            FailureWindowMs = 60_000,
            LockoutMs = 120_000
        }, clock);

        var failed = guard.Evaluate(CreateRequest("127.0.0.1", "admin", "bad-1"));
        var limited = guard.Evaluate(CreateRequest("127.0.0.1", "admin", "bad-2"));
        var blockedSuccess = guard.Evaluate(CreateRequest("127.0.0.1", "admin", "secret"));

        Assert.Equal(AuthenticationAttemptStatus.InvalidCredentials, failed.Status);
        Assert.Equal(1, failed.FailureCount);
        Assert.Equal(AuthenticationAttemptStatus.RateLimited, limited.Status);
        Assert.True(limited.RetryAfterSeconds >= 119);
        Assert.Equal(AuthenticationAttemptStatus.RateLimited, blockedSuccess.Status);

        clock.Advance(TimeSpan.FromMinutes(2));
        var success = guard.Evaluate(CreateRequest("127.0.0.1", "admin", "secret"));
        Assert.Equal(AuthenticationAttemptStatus.Success, success.Status);
    }

    [Fact]
    public void AuthorizationSupport_AllowsDisabledAuth_AndValidBasicCredentials()
    {
        var disabled = new AuthOptions { Enabled = false };
        var disabledGuard = new AuthenticationGuard(disabled);
        Assert.True(AuthorizationSupport.IsAuthorized(CreateRequest("127.0.0.1"), disabled, disabledGuard, out var disabledFailure));
        Assert.Null(disabledFailure);

        var enabled = new AuthOptions { Enabled = true, Username = "admin", Password = "secret" };
        var enabledGuard = new AuthenticationGuard(enabled);
        Assert.True(AuthorizationSupport.IsAuthorized(CreateRequest("127.0.0.1", "admin", "secret"), enabled, enabledGuard, out var successFailure));
        Assert.Null(successFailure);
        Assert.False(AuthorizationSupport.IsAuthorized(CreateRequest("127.0.0.1", "admin", "wrong"), enabled, enabledGuard, out var failedAttempt));
        Assert.Equal(AuthenticationAttemptStatus.InvalidCredentials, failedAttempt?.Status);
    }

    [Fact]
    public void AuthenticationGuard_IgnoresForwardedFor_UnlessRemoteIsTrustedProxy()
    {
        var untrusted = new AuthenticationGuard(new AuthOptions { Username = "admin", Password = "secret" });
        var request = CreateRequest("127.0.0.1", "admin", "wrong");
        request.Headers["X-Forwarded-For"] = "203.0.113.10";
        Assert.Equal("127.0.0.1", untrusted.Evaluate(request).ClientId);

        var trusted = new AuthenticationGuard(new AuthOptions
        {
            Username = "admin",
            Password = "secret",
            TrustedProxyAddresses = ["127.0.0.1"]
        });
        Assert.Equal("203.0.113.10", trusted.Evaluate(request).ClientId);
    }

    [Fact]
    public void AuthenticationGuard_DisablesQueryCredentialsByDefault()
    {
        var context = new DefaultHttpContext();
        context.Request.QueryString = new QueryString("?u=admin&p=secret");
        var guard = new AuthenticationGuard(new AuthOptions { Username = "admin", Password = "secret" });
        Assert.Equal(AuthenticationAttemptStatus.MissingCredentials, guard.Evaluate(context.Request).Status);
    }

    private static HttpRequest CreateRequest(string ip, string? user = null, string? password = null)
    {
        var context = new DefaultHttpContext();
        context.Connection.RemoteIpAddress = IPAddress.Parse(ip);
        if (user != null || password != null)
        {
            var raw = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{user}:{password}"));
            context.Request.Headers.Authorization = $"Basic {raw}";
        }

        return context.Request;
    }

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan duration) => _utcNow = _utcNow.Add(duration);
    }
}
