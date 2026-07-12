using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Net;
using System.Text;

public enum AuthenticationAttemptStatus
{
    MissingCredentials,
    Success,
    InvalidCredentials,
    RateLimited
}

public sealed class AuthenticationAttempt
{
    public AuthenticationAttemptStatus Status { get; init; }
    public string ClientId { get; init; } = "unknown";
    public string CredentialSource { get; init; } = "none";
    public string? PresentedUserName { get; init; }
    public int FailureCount { get; init; }
    public int MaxFailedAttempts { get; init; }
    public int RetryAfterSeconds { get; init; }

    public bool Authenticated => Status == AuthenticationAttemptStatus.Success;
    public bool IsRateLimited => Status == AuthenticationAttemptStatus.RateLimited;
}

public sealed class AuthenticationGuard
{
    private readonly AuthOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<string, ClientFailureState> _clientFailures = new(StringComparer.Ordinal);
    private readonly TimeSpan _failureWindow;
    private readonly TimeSpan _lockoutDuration;

    public AuthenticationGuard(AuthOptions options, TimeProvider? timeProvider = null)
    {
        _options = options;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _failureWindow = TimeSpan.FromMilliseconds(Math.Max(1000, options.FailureWindowMs));
        _lockoutDuration = TimeSpan.FromMilliseconds(Math.Max(0, options.LockoutMs));
    }

    public AuthenticationAttempt Evaluate(HttpRequest request)
    {
        var clientId = GetClientId(request);
        var (userName, password, source) = ExtractCredentials(request);

        if (source == "none")
        {
            return new AuthenticationAttempt
            {
                Status = AuthenticationAttemptStatus.MissingCredentials,
                ClientId = clientId,
                CredentialSource = source
            };
        }

        var state = _clientFailures.GetOrAdd(clientId, static _ => new ClientFailureState());
        var now = _timeProvider.GetUtcNow();
        lock (state.SyncRoot)
        {
            if (state.LockedUntilUtc > now)
            {
                return new AuthenticationAttempt
                {
                    Status = AuthenticationAttemptStatus.RateLimited,
                    ClientId = clientId,
                    CredentialSource = source,
                    PresentedUserName = NormalizeUserName(userName),
                    FailureCount = state.FailureCount,
                    MaxFailedAttempts = _options.MaxFailedAttempts,
                    RetryAfterSeconds = GetRetryAfterSeconds(now, state.LockedUntilUtc)
                };
            }

            if (state.FailureCount > 0 && now - state.WindowStartedUtc >= _failureWindow)
                state.Reset();

            if (CredentialsMatch(userName, password))
            {
                state.Reset();
                return new AuthenticationAttempt
                {
                    Status = AuthenticationAttemptStatus.Success,
                    ClientId = clientId,
                    CredentialSource = source,
                    PresentedUserName = _options.Username
                };
            }

            if (!RateLimitEnabled)
            {
                return new AuthenticationAttempt
                {
                    Status = AuthenticationAttemptStatus.InvalidCredentials,
                    ClientId = clientId,
                    CredentialSource = source,
                    PresentedUserName = NormalizeUserName(userName)
                };
            }

            if (state.FailureCount == 0)
                state.WindowStartedUtc = now;

            state.FailureCount++;
            if (state.FailureCount >= _options.MaxFailedAttempts)
            {
                state.LockedUntilUtc = now.Add(_lockoutDuration);
                return new AuthenticationAttempt
                {
                    Status = AuthenticationAttemptStatus.RateLimited,
                    ClientId = clientId,
                    CredentialSource = source,
                    PresentedUserName = NormalizeUserName(userName),
                    FailureCount = state.FailureCount,
                    MaxFailedAttempts = _options.MaxFailedAttempts,
                    RetryAfterSeconds = GetRetryAfterSeconds(now, state.LockedUntilUtc)
                };
            }

            return new AuthenticationAttempt
            {
                Status = AuthenticationAttemptStatus.InvalidCredentials,
                ClientId = clientId,
                CredentialSource = source,
                PresentedUserName = NormalizeUserName(userName),
                FailureCount = state.FailureCount,
                MaxFailedAttempts = _options.MaxFailedAttempts
            };
        }
    }

    private bool CredentialsMatch(string? userName, string? password)
    {
        if (string.IsNullOrEmpty(userName) || string.IsNullOrEmpty(password))
            return false;

        var expectedUser = Encoding.UTF8.GetBytes(_options.Username);
        var actualUser = Encoding.UTF8.GetBytes(userName);
        var expectedPassword = Encoding.UTF8.GetBytes(_options.Password);
        var actualPassword = Encoding.UTF8.GetBytes(password);
        return expectedUser.Length == actualUser.Length
            && expectedPassword.Length == actualPassword.Length
            && CryptographicOperations.FixedTimeEquals(expectedUser, actualUser)
            && CryptographicOperations.FixedTimeEquals(expectedPassword, actualPassword);
    }

    private bool RateLimitEnabled =>
        _options.MaxFailedAttempts > 0
        && _failureWindow > TimeSpan.Zero
        && _lockoutDuration > TimeSpan.Zero;

    private (string? UserName, string? Password, string Source) ExtractCredentials(HttpRequest request)
    {
        var userName = request.Query["u"].ToString();
        var password = request.Query["p"].ToString();
        if (_options.AllowQueryCredentials && (!string.IsNullOrEmpty(userName) || !string.IsNullOrEmpty(password)))
            return (userName, password, "query");

        var authorization = request.Headers.Authorization.ToString();
        if (authorization.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase))
        {
            try
            {
                var raw = Encoding.UTF8.GetString(Convert.FromBase64String(authorization["Basic ".Length..].Trim()));
                var separatorIndex = raw.IndexOf(':');
                if (separatorIndex >= 0)
                    return (raw[..separatorIndex], raw[(separatorIndex + 1)..], "basic");
            }
            catch
            {
                return (null, null, "basic");
            }

            return (null, null, "basic");
        }

        return (null, null, "none");
    }

    private string GetClientId(HttpRequest request)
    {
        var remoteIp = request.HttpContext.Connection.RemoteIpAddress;
        if (!IsTrustedProxy(remoteIp))
            return remoteIp?.ToString() ?? "unknown";

        var forwardedFor = request.Headers["X-Forwarded-For"].ToString();
        if (!string.IsNullOrWhiteSpace(forwardedFor))
        {
            var first = forwardedFor.Split(',', 2)[0].Trim();
            if (!string.IsNullOrWhiteSpace(first))
                return first;
        }

        return remoteIp?.ToString() ?? "unknown";
    }

    private bool IsTrustedProxy(IPAddress? remoteIp) =>
        remoteIp != null && _options.TrustedProxyAddresses.Any(value =>
            IPAddress.TryParse(value, out var trusted) && trusted.Equals(remoteIp));

    private static int GetRetryAfterSeconds(DateTimeOffset now, DateTimeOffset lockedUntilUtc)
    {
        var remaining = lockedUntilUtc - now;
        if (remaining <= TimeSpan.Zero)
            return 0;
        return (int)Math.Ceiling(remaining.TotalSeconds);
    }

    private static string? NormalizeUserName(string? userName)
    {
        if (string.IsNullOrWhiteSpace(userName))
            return null;

        var trimmed = userName.Trim();
        return trimmed.Length <= 128 ? trimmed : trimmed[..128];
    }

    private sealed class ClientFailureState
    {
        public object SyncRoot { get; } = new();
        public int FailureCount { get; set; }
        public DateTimeOffset WindowStartedUtc { get; set; }
        public DateTimeOffset LockedUntilUtc { get; set; }

        public void Reset()
        {
            FailureCount = 0;
            WindowStartedUtc = default;
            LockedUntilUtc = default;
        }
    }
}
