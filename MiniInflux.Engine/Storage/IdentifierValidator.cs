namespace MiniInflux.Net10.Storage;

/// <summary>
/// Validates user-supplied database and retention-policy names before they are used as path
/// segments. InfluxQL identifiers may be quoted, but they must never escape the data directory.
/// </summary>
public static class IdentifierValidator
{
    private static readonly HashSet<char> InvalidFileNameChars = BuildInvalidFileNameChars();

    public static bool IsValidDatabaseName(string? name) => IsValidName(name);
    public static bool IsValidRetentionPolicyName(string? name) => IsValidName(name);

    public static void ValidateDatabaseName(string? name)
    {
        if (!IsValidDatabaseName(name))
            throw new ArgumentException($"invalid database name: {name}", nameof(name));
    }

    public static void ValidateRetentionPolicyName(string? name)
    {
        if (!IsValidRetentionPolicyName(name))
            throw new ArgumentException($"invalid retention policy name: {name}", nameof(name));
    }

    private static bool IsValidName(string? name)
    {
        if (string.IsNullOrWhiteSpace(name) || name.Length > 255)
            return false;
        if (name is "." or "..")
            return false;
        if (name.Contains("..", StringComparison.Ordinal))
            return false;
        if (Path.IsPathRooted(name))
            return false;

        foreach (var ch in name)
        {
            if (ch == '/' || ch == '\\' || char.IsControl(ch) || InvalidFileNameChars.Contains(ch))
                return false;
        }

        return true;
    }

    private static HashSet<char> BuildInvalidFileNameChars()
    {
        var chars = new HashSet<char>(Path.GetInvalidFileNameChars());
        // Path.GetInvalidFileNameChars is platform-specific. Reject the Windows set everywhere so
        // a backup created on Windows cannot become path-traversing on Unix (or vice versa).
        foreach (var ch in "<>:\"|?*")
            chars.Add(ch);
        return chars;
    }
}
