using System.Text.Json;
using System.Text.Json.Serialization;
using System.Security.Cryptography;
using System.Text;

namespace MiniInflux.Net10.Storage;

public sealed class AuthStore
{
    private readonly string _path;
    private readonly object _lock = new();
    private AuthStoreData _data = new();

    public AuthStore(string dataPath)
    {
        var metaDir = Path.Combine(dataPath, "meta");
        Directory.CreateDirectory(metaDir);
        _path = Path.Combine(metaDir, "auth.json");
        Load();
    }

    public void CreateUser(string userName, string password, bool isAdmin)
    {
        lock (_lock)
        {
            if (_data.Users.ContainsKey(userName))
                throw new InvalidOperationException($"user already exists: {userName}");
            _data.Users[userName] = new AuthUserRecord
            {
                UserName = userName,
                Password = HashPassword(password),
                IsAdmin = isAdmin
            };
            Save();
        }
    }

    public void DropUser(string userName)
    {
        lock (_lock)
        {
            if (_data.Users.Remove(userName))
                Save();
        }
    }

    public void SetPassword(string userName, string password)
    {
        lock (_lock)
        {
            if (!_data.Users.TryGetValue(userName, out var user))
                throw new InvalidOperationException($"user not found: {userName}");
            user.Password = HashPassword(password);
            Save();
        }
    }

    public IReadOnlyList<AuthUserRecord> ListUsers()
    {
        lock (_lock) return _data.Users.Values.OrderBy(x => x.UserName, StringComparer.Ordinal).ToList();
    }

    public void Grant(string userName, string db, string privilege)
    {
        lock (_lock)
        {
            if (!_data.Users.TryGetValue(userName, out var user))
                throw new InvalidOperationException($"user not found: {userName}");
            user.Grants[db] = NormalizePrivilege(privilege);
            Save();
        }
    }

    public void Revoke(string userName, string db, string privilege)
    {
        lock (_lock)
        {
            if (!_data.Users.TryGetValue(userName, out var user))
                throw new InvalidOperationException($"user not found: {userName}");
            var normalized = NormalizePrivilege(privilege);
            if (!user.Grants.TryGetValue(db, out var existing))
                return;

            if (normalized == "ALL" || string.Equals(existing, normalized, StringComparison.OrdinalIgnoreCase))
                user.Grants.Remove(db);
            Save();
        }
    }

    public bool Validate(string userName, string password, out AuthIdentity? identity)
    {
        lock (_lock)
        {
            identity = null;
            if (!_data.Users.TryGetValue(userName, out var user))
                return false;
            var upgraded = false;
            if (!VerifyPassword(user, password, ref upgraded))
                return false;
            if (upgraded)
                Save();
            identity = ToIdentity(user);
            return true;
        }
    }

    public AuthIdentity? Find(string userName)
    {
        lock (_lock)
        {
            return _data.Users.TryGetValue(userName, out var user) ? ToIdentity(user) : null;
        }
    }

    public bool IsAuthorized(AuthIdentity? identity, string db, AuthPermission permission)
        => IsAuthorized(identity, db, null, null, permission);

    public bool IsAuthorized(AuthIdentity? identity, string db, string? rp, string? measurement, AuthPermission permission)
    {
        if (identity == null) return false;
        if (identity.IsAdmin) return true;
        if (!TryResolveGrant(identity.Grants, db, rp, measurement, out var grant)) return false;
        return permission switch
        {
            AuthPermission.Read => grant is "READ" or "WRITE" or "ALL",
            AuthPermission.Write => grant is "WRITE" or "ALL",
            AuthPermission.Admin => false,
            _ => false
        };
    }

    private static AuthIdentity ToIdentity(AuthUserRecord user) => new(user.UserName, user.IsAdmin,
        new Dictionary<string, string>(user.Grants, StringComparer.Ordinal));

    private static bool TryResolveGrant(Dictionary<string, string> grants, string db, string? rp, string? measurement, out string grant)
    {
        foreach (var scope in EnumerateScopes(db, rp, measurement))
        {
            if (grants.TryGetValue(scope, out grant!))
                return true;
        }

        grant = "";
        return false;
    }

    private static IEnumerable<string> EnumerateScopes(string db, string? rp, string? measurement)
    {
        if (!string.IsNullOrWhiteSpace(rp) && !string.IsNullOrWhiteSpace(measurement))
            yield return $"{db}.{rp}.{measurement}";
        if (!string.IsNullOrWhiteSpace(rp) && !string.IsNullOrWhiteSpace(measurement))
            yield return $"{db}.{rp}.*";
        if (!string.IsNullOrWhiteSpace(rp))
            yield return $"{db}.{rp}";
        yield return $"{db}.*";
        yield return db;
    }

    private static bool VerifyPassword(AuthUserRecord user, string password, ref bool upgraded)
    {
        if (IsHashedPassword(user.Password))
            return string.Equals(user.Password, HashPassword(password, user.Password.Split(':')[1]), StringComparison.Ordinal);

        if (!string.Equals(user.Password, password, StringComparison.Ordinal))
            return false;

        user.Password = HashPassword(password);
        upgraded = true;
        return true;
    }

    private static bool IsHashedPassword(string password) =>
        password.StartsWith("sha256:", StringComparison.OrdinalIgnoreCase);

    private static string HashPassword(string password)
    {
        Span<byte> saltBytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(saltBytes);
        return HashPassword(password, Convert.ToHexString(saltBytes));
    }

    private static string HashPassword(string password, string saltHex)
    {
        var saltBytes = Convert.FromHexString(saltHex);
        var passwordBytes = Encoding.UTF8.GetBytes(password);
        var combined = new byte[saltBytes.Length + passwordBytes.Length];
        Buffer.BlockCopy(saltBytes, 0, combined, 0, saltBytes.Length);
        Buffer.BlockCopy(passwordBytes, 0, combined, saltBytes.Length, passwordBytes.Length);
        var hash = SHA256.HashData(combined);
        return $"sha256:{saltHex}:{Convert.ToHexString(hash)}";
    }

    private static string NormalizePrivilege(string privilege)
    {
        privilege = privilege.Trim().ToUpperInvariant();
        return privilege switch
        {
            "ALL" => "ALL",
            "WRITE" => "WRITE",
            _ => "READ"
        };
    }

    private void Load()
    {
        if (!File.Exists(_path)) return;
        try
        {
            var data = JsonSerializer.Deserialize(File.ReadAllText(_path), AuthStoreJsonContext.Default.AuthStoreData);
            if (data != null) _data = data;
        }
        catch { }
    }

    private void Save()
    {
        var tmp = _path + ".tmp";
        File.WriteAllText(tmp, JsonSerializer.Serialize(_data, AuthStoreJsonContext.Default.AuthStoreData));
        File.Move(tmp, _path, overwrite: true);
    }
}

public sealed record AuthIdentity(string UserName, bool IsAdmin, Dictionary<string, string> Grants);
public enum AuthPermission { Read, Write, Admin }

public sealed class AuthUserRecord
{
    public string UserName { get; set; } = "";
    public string Password { get; set; } = "";
    public bool IsAdmin { get; set; }
    public Dictionary<string, string> Grants { get; set; } = new(StringComparer.Ordinal);
}

internal sealed class AuthStoreData
{
    public Dictionary<string, AuthUserRecord> Users { get; set; } = new(StringComparer.Ordinal);
}

[JsonSerializable(typeof(AuthStoreData))]
[JsonSerializable(typeof(AuthUserRecord))]
[JsonSerializable(typeof(Dictionary<string, AuthUserRecord>))]
[JsonSerializable(typeof(Dictionary<string, string>))]
internal partial class AuthStoreJsonContext : JsonSerializerContext { }
