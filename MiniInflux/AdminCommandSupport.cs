using MiniInflux.Net10.Protocol;

namespace MiniInflux.Net10;

/// <summary>
/// Limits the admin console's management-command endpoint to the small set of
/// operations exposed by the UI. Read queries remain on the separate read-only endpoint.
/// </summary>
public static class AdminCommandSupport
{
    public static bool IsAllowed(QueryKind kind) => kind is
        QueryKind.CreateDatabase
        or QueryKind.DropDatabase
        or QueryKind.CreateRetentionPolicy
        or QueryKind.DropRetentionPolicy
        or QueryKind.CreateContinuousQuery
        or QueryKind.DropContinuousQuery;
}
