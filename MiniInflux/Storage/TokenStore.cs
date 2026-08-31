using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Named token record persisted to data/meta/tokens.json.
/// 等权 token：与 Basic 的 admin 等效，后续可扩展为 db 级权限.
/// </summary>
public sealed class ApiToken
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    /// <summary>SHA256(rawToken) base64，不落盘明文.</summary>
    public string TokenHash { get; set; } = "";
    /// <summary>明文前缀（前 8 字符）用于列表展示，不泄露完整 token.</summary>
    public string Prefix { get; set; } = "";
    /// <summary>权限分级：all（默认）| read | write。缺省字段（旧数据）按 all 处理.</summary>
    public string Permissions { get; set; } = "all";
    public long CreatedAtNs { get; set; }

    public static bool IsValidPermission(string permission) =>
        permission is "all" or "read" or "write";
}

public sealed class TokenStore
{
    private readonly string _path;
    private readonly object _lock = new();
    private readonly Dictionary<string, ApiToken> _byId = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _hashToId = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _nameToId = new(StringComparer.Ordinal);

    public TokenStore(string dataPath)
    {
        var metaDir = Path.Combine(dataPath, "meta");
        Directory.CreateDirectory(metaDir);
        _path = Path.Combine(metaDir, "tokens.json");
        Load();
    }

    public IReadOnlyList<ApiToken> List()
    {
        lock (_lock) return _byId.Values.OrderBy(t => t.CreatedAtNs).ToList();
    }

    public ApiToken? FindById(string id)
    {
        lock (_lock) return _byId.TryGetValue(id, out var t) ? t : null;
    }

    /// <summary>
    /// Validate raw token against stored hashes (fixed-time). Returns matching token or null.
    /// </summary>
    public ApiToken? Validate(string rawToken)
    {
        if (string.IsNullOrEmpty(rawToken)) return null;
        var hash = ComputeHash(rawToken);
        string? id = null;
        lock (_lock)
        {
            // constant-time lookup emulation: iterate to avoid timing leak via hash map
            foreach (var kv in _hashToId)
            {
                if (CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(kv.Key), Encoding.UTF8.GetBytes(hash)))
                {
                    id = kv.Value;
                    break;
                }
            }
            if (id != null && _byId.TryGetValue(id, out var token)) return token;
            // fallback direct (fast path) if fixed-time loop missed due to base64 padding variations
            if (_hashToId.TryGetValue(hash, out var directId) && _byId.TryGetValue(directId, out var direct)) return direct;
        }
        return null;
    }

    /// <summary>
    /// Create a new named token. Name must be unique, 1..64 chars, [A-Za-z0-9_-].
    /// Returns (record, rawToken) — rawToken is shown only once.
    /// </summary>
    public (ApiToken Record, string RawToken) Create(string name, string permissions = "all")
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("token name is required", nameof(name));
        name = name.Trim();
        if (name.Length > 64) throw new ArgumentException("token name must be <=64 chars", nameof(name));
        if (!IsValidName(name)) throw new ArgumentException("token name may only contain A-Za-z0-9 _ -", nameof(name));
        permissions = permissions?.Trim().ToLowerInvariant() ?? "all";
        if (!ApiToken.IsValidPermission(permissions)) throw new ArgumentException("token permissions must be all, read or write", nameof(permissions));

        lock (_lock)
        {
            if (_nameToId.ContainsKey(name)) throw new InvalidOperationException($"token name '{name}' already exists");
            var raw = GenerateRawToken();
            var hash = ComputeHash(raw);
            // extremely unlikely collision, regenerate
            if (_hashToId.ContainsKey(hash))
            {
                raw = GenerateRawToken();
                hash = ComputeHash(raw);
            }
            var id = Guid.NewGuid().ToString("N");
            var prefix = raw.Length >= 8 ? raw[..8] : raw;
            var rec = new ApiToken
            {
                Id = id,
                Name = name,
                TokenHash = hash,
                Prefix = prefix,
                Permissions = permissions,
                CreatedAtNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000
            };
            _byId[id] = rec;
            _hashToId[hash] = id;
            _nameToId[name] = id;
            SaveLocked();
            return (rec, raw);
        }
    }

    public bool Revoke(string id)
    {
        lock (_lock)
        {
            if (!_byId.TryGetValue(id, out var rec)) return false;
            _byId.Remove(id);
            _hashToId.Remove(rec.TokenHash);
            _nameToId.Remove(rec.Name);
            SaveLocked();
            return true;
        }
    }

    private void Load()
    {
        lock (_lock)
        {
            _byId.Clear(); _hashToId.Clear(); _nameToId.Clear();
            if (!File.Exists(_path)) return;
            try
            {
                var json = File.ReadAllText(_path);
                var list = JsonSerializer.Deserialize(json, TokenJsonContext.Default.ListApiToken);
                if (list == null) return;
                foreach (var t in list)
                {
                    _byId[t.Id] = t;
                    _hashToId[t.TokenHash] = t.Id;
                    _nameToId[t.Name] = t.Id;
                }
            }
            catch { /* corrupted file -> start empty */ }
        }
    }

    private void SaveLocked()
    {
        var list = _byId.Values.OrderBy(t => t.CreatedAtNs).ToList();
        var json = JsonSerializer.Serialize(list, TokenJsonContext.Default.ListApiToken);
        var tmp = _path + ".tmp";
        Directory.CreateDirectory(Path.GetDirectoryName(_path)!);
        File.WriteAllText(tmp, json);
        File.Move(tmp, _path, overwrite: true);
    }

    private static bool IsValidName(string name)
    {
        foreach (var ch in name)
            if (!(ch is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '_' or '-')) return false;
        return true;
    }

    private static string GenerateRawToken()
    {
        Span<byte> bytes = stackalloc byte[32];
        RandomNumberGenerator.Fill(bytes);
        var b64 = Convert.ToBase64String(bytes).Replace('+', '-').Replace('/', '_').TrimEnd('=');
        return "mini_" + b64;
    }

    public static string ComputeHash(string rawToken)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(rawToken));
        return Convert.ToBase64String(hash);
    }
}

[JsonSerializable(typeof(List<ApiToken>))]
internal partial class TokenJsonContext : JsonSerializerContext { }
