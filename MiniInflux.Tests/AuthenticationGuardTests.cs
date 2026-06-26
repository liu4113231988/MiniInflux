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

    private static DefaultHttpContext CreateRequest(string ip, string? user = null, string? password = null)
    {
        var context = new DefaultHttpContext();
        context.Connection.RemoteIpAddress = IPAddress.Parse(ip);
        if (user != null || password != null)
        {
            var raw = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{user}:{password}"));
            context.Request.Headers.Authorization = $"Basic {raw}";
        }

        return context;
    }

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan duration) => _utcNow = _utcNow.Add(duration);
    }
}
