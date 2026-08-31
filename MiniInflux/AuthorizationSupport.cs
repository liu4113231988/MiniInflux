using Microsoft.AspNetCore.Http;

public static class AuthorizationSupport
{
    public static bool IsAuthorized(HttpRequest request, AuthOptions options, AuthenticationGuard authenticationGuard,
        out AuthenticationAttempt? failedAttempt)
    {
        return IsAuthorized(request, options, authenticationGuard, out failedAttempt, out _);
    }

    public static bool IsAuthorized(HttpRequest request, AuthOptions options, AuthenticationGuard authenticationGuard,
        out AuthenticationAttempt? failedAttempt, out string grantedPermission)
    {
        grantedPermission = "all";
        if (!options.Enabled)
        {
            failedAttempt = null;
            return true;
        }

        var attempt = authenticationGuard.Evaluate(request);
        if (attempt.Authenticated)
        {
            failedAttempt = null;
            grantedPermission = attempt.Permission;
            return true;
        }

        failedAttempt = attempt;
        return false;
    }

    /// <summary>"all" covers every tier; otherwise the granted tier must match the requirement.</summary>
    public static bool PermissionCovers(string granted, string required) =>
        granted == "all" || string.Equals(granted, required, StringComparison.Ordinal);
}
