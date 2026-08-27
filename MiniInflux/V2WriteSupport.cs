/// <summary>
/// Helpers for the InfluxDB 2.x compatible API surface.
/// </summary>
public static class V2WriteSupport
{
    /// <summary>
    /// Map a v2 `bucket` parameter to (db, rp). Buckets are plain database names; the
    /// `db/rp` form (used by InfluxDB 2.x for v1 compatibility) selects a retention policy.
    /// </summary>
    public static bool TryResolveBucket(string? bucket, out string db, out string rp, out string error)
    {
        db = "";
        rp = "autogen";
        if (string.IsNullOrWhiteSpace(bucket))
        {
            error = "missing required parameter bucket";
            return false;
        }

        bucket = bucket.Trim();
        var slash = bucket.IndexOf('/');
        if (slash < 0)
        {
            db = bucket;
        }
        else
        {
            db = bucket[..slash].Trim();
            rp = bucket[(slash + 1)..].Trim();
        }

        if (db.Length == 0 || rp.Length == 0)
        {
            error = $"invalid bucket name: {bucket}";
            return false;
        }

        error = "";
        return true;
    }
}
