using Microsoft.AspNetCore.Http;

public static class AuthorizationSupport
{
    public static bool IsAuthorized(HttpRequest request, AuthOptions options, AuthenticationGuard authenticationGuard,
        out AuthenticationAttempt? failedAttempt)
    {
        if (!options.Enabled)
        {
            failedAttempt = null;
            return true;
        }

        var attempt = authenticationGuard.Evaluate(request);
        if (attempt.Authenticated)
        {
            failedAttempt = null;
            return true;
        }

        failedAttempt = attempt;
        return false;
    }
}
