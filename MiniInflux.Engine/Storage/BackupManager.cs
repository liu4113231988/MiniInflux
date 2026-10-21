using System.Security.Cryptography;
using System.Text.Json;

namespace MiniInflux.Net10.Storage;

public static class BackupManager
{
    private const string MetadataFileName = "backup.metadata.json";

    public static void CreateBackup(string source, string destination)
    {
        EnsureDestinationOutsideSource(source, destination);

        var staging = destination + ".staging";
        ReplaceDirectory(staging, source);
        // manifest.json is copied last: an earlier snapshot could reference segment files that were
        // only registered (flush/compaction) after the bulk copy enumerated the source tree.
        CopyManifest(source, staging);
        // A manifest that was updated concurrently may reference segments the bulk copy missed;
        // the shard-file fixup runs after the manifest copy so every referenced segment is present.
        CopyMissingShardFiles(source, staging);
        var metadata = BuildMetadata(staging, source);
        File.WriteAllText(Path.Combine(staging, MetadataFileName), JsonSerializer.Serialize(metadata, EngineJsonContext.Default.BackupMetadata));

        if (Directory.Exists(destination)) Directory.Delete(destination, recursive: true);
        Directory.Move(staging, destination);
    }

    public static void PrepareRestore(string backupPath, string dataRoot)
    {
        if (!Directory.Exists(backupPath))
            throw new DirectoryNotFoundException("backup path does not exist");

        ValidateBackup(backupPath);

        var pending = dataRoot + ".restore-pending";
        var staging = pending + ".staging";
        ReplaceDirectory(staging, backupPath);
        if (Directory.Exists(pending)) Directory.Delete(pending, recursive: true);
        Directory.Move(staging, pending);
    }

    public static void ApplyPendingRestore(string dataRoot)
    {
        var pending = dataRoot + ".restore-pending";
        if (!Directory.Exists(pending)) return;

        ValidateBackup(pending);

        var previous = dataRoot + ".restore-previous";
        if (Directory.Exists(previous)) Directory.Delete(previous, recursive: true);

        try
        {
            if (Directory.Exists(dataRoot))
                Directory.Move(dataRoot, previous);
            Directory.Move(pending, dataRoot);
            if (Directory.Exists(previous)) Directory.Delete(previous, recursive: true);
        }
        catch
        {
            if (!Directory.Exists(dataRoot) && Directory.Exists(previous))
                Directory.Move(previous, dataRoot);
            throw;
        }
    }

    public static void ValidateBackup(string backupPath)
    {
        var metadataPath = Path.Combine(backupPath, MetadataFileName);
        if (!File.Exists(metadataPath))
            throw new InvalidDataException("backup metadata is missing; refusing to restore an unverifiable backup");

        var metadata = JsonSerializer.Deserialize(File.ReadAllText(metadataPath), EngineJsonContext.Default.BackupMetadata)
            as BackupMetadata
            ?? throw new InvalidDataException("backup metadata is invalid");
        if (metadata.Files == null)
            throw new InvalidDataException("backup metadata has no file list");

        var backupRoot = Path.GetFullPath(backupPath)
            .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            + Path.DirectorySeparatorChar;

        foreach (var file in metadata.Files)
        {
            if (string.IsNullOrWhiteSpace(file.RelativePath)
                || Path.IsPathRooted(file.RelativePath)
                || HasParentTraversalOrDrive(file.RelativePath))
                throw new InvalidDataException($"backup metadata contains an invalid relative path: {file.RelativePath}");

            var fullPath = Path.GetFullPath(Path.Combine(backupPath, file.RelativePath));
            if (!fullPath.StartsWith(backupRoot, StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException($"backup metadata path escapes the backup directory: {file.RelativePath}");

            if (!File.Exists(fullPath))
                throw new InvalidDataException($"backup file missing: {file.RelativePath}");

            var info = new FileInfo(fullPath);
            if (info.Length != file.Length)
                throw new InvalidDataException($"backup file length mismatch: {file.RelativePath}");

            var hash = ComputeSha256(fullPath);
            if (!string.Equals(hash, file.Sha256, StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException($"backup file checksum mismatch: {file.RelativePath}");
        }

        ValidateManifestReferences(backupPath);
    }

    /// <summary>
    /// Every segment referenced by the copied manifest must exist in the backup. The metadata file
    /// list normally guarantees this, but a hand-edited or truncated backup must not restore into a
    /// catalog that points at missing data.
    /// </summary>
    private static void ValidateManifestReferences(string backupPath)
    {
        var manifestPath = Path.Combine(backupPath, "meta", "manifest.json");
        if (!File.Exists(manifestPath)) return;

        ManifestData? data;
        try
        {
            data = JsonSerializer.Deserialize(File.ReadAllBytes(manifestPath), ManifestJsonContext.Default.ManifestData);
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException("backup manifest is invalid", ex);
        }

        if (data == null) throw new InvalidDataException("backup manifest is empty");

        foreach (var (dbName, db) in data.Databases)
        {
            foreach (var (rpName, rp) in db.RetentionPolicies)
            {
                foreach (var shard in rp.ShardGroups)
                {
                    var shardDir = Path.Combine(backupPath, "db", dbName, rpName, "shards", shard.Id.ToString("D6"));
                    foreach (var segment in shard.SegmentFiles)
                    {
                        var segmentPath = Path.Combine(shardDir, segment);
                        if (!File.Exists(segmentPath))
                            throw new InvalidDataException(
                                $"backup manifest references a missing segment: {dbName}/{rpName}/{shard.Id}/{segment}");
                    }
                }
            }
        }
    }

    private static void ReplaceDirectory(string destination, string source)
    {
        if (Directory.Exists(destination)) Directory.Delete(destination, recursive: true);
        CopyDirectory(source, destination);
    }

    private static void CopyManifest(string source, string staging)
    {
        var manifestSrc = Path.Combine(source, "meta", "manifest.json");
        if (!File.Exists(manifestSrc)) return;
        var manifestDst = Path.Combine(staging, "meta", "manifest.json");
        Directory.CreateDirectory(Path.GetDirectoryName(manifestDst)!);
        // Atomic tmp+rename on the source means a read sees one full version; copy via bytes so a
        // concurrent rename cannot interrupt File.Copy's open+create pair.
        File.WriteAllBytes(manifestDst, File.ReadAllBytes(manifestSrc));
    }

    /// <summary>
    /// Copy any segment file present in the source but missing from staging. Runs after the manifest
    /// copy: whatever the final manifest references exists in the source (compaction only deletes
    /// inputs after unregistering them) and therefore ends up in staging.
    /// </summary>
    private static void CopyMissingShardFiles(string source, string staging)
    {
        var sourceDb = Path.Combine(source, "db");
        if (!Directory.Exists(sourceDb)) return;
        foreach (var srcFile in Directory.EnumerateFiles(sourceDb, "*.seg", SearchOption.AllDirectories))
        {
            var dst = Path.Combine(staging, Path.GetRelativePath(source, srcFile));
            if (File.Exists(dst)) continue;
            Directory.CreateDirectory(Path.GetDirectoryName(dst)!);
            File.Copy(srcFile, dst, overwrite: true);
        }
    }

    private static BackupMetadata BuildMetadata(string root, string sourceRoot)
    {
        var files = Directory.GetFiles(root, "*", SearchOption.AllDirectories)
            .Where(path => !string.Equals(Path.GetFileName(path), MetadataFileName, StringComparison.OrdinalIgnoreCase))
            // WAL files keep appending during the backup, so their hash cannot be stable; they are
            // self-validating instead (per-record CRC + torn-tail tolerance on replay).
            .Where(path => !IsUnderDirectory(path, root, "wal"))
            .Where(path => !IsTransientPath(root, path))
            .Select(path => new BackupFileEntry(
                Path.GetRelativePath(root, path),
                new FileInfo(path).Length,
                ComputeSha256(path)))
            .OrderBy(x => x.RelativePath, StringComparer.Ordinal)
            .ToList();

        return new BackupMetadata(
            1,
            DateTimeOffset.UtcNow,
            Path.GetFullPath(sourceRoot),
            files);
    }

    private static bool IsUnderDirectory(string path, string root, string child)
    {
        var prefix = Path.Combine(root, child) + Path.DirectorySeparatorChar;
        return path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase);
    }

    private static string ComputeSha256(string path)
    {
        using var stream = File.OpenRead(path);
        var hash = SHA256.HashData(stream);
        return Convert.ToHexString(hash);
    }

    private static void EnsureDestinationOutsideSource(string source, string destination)
    {
        var sourceFull = Path.GetFullPath(source).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var destinationFull = Path.GetFullPath(destination).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        if (IsSameOrUnder(destinationFull, sourceFull))
            throw new InvalidOperationException("backup destination must be outside the data directory");

        // A destination that is an ancestor of the source is just as dangerous: deleting/replacing
        // the destination would remove the live data directory before the backup is complete.
        if (IsSameOrUnder(sourceFull, destinationFull))
            throw new InvalidOperationException("backup destination must not contain the data directory");
    }

    private static bool IsSameOrUnder(string path, string root) =>
        string.Equals(path, root, StringComparison.OrdinalIgnoreCase)
        || path.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)
        || path.StartsWith(root + Path.AltDirectorySeparatorChar, StringComparison.OrdinalIgnoreCase);

    private static bool HasParentTraversalOrDrive(string relativePath)
    {
        // Reject Windows drive/UNC forms even when validating on Unix, and reject `..` using either
        // separator so a backup created on one OS cannot carry an unsafe path to another.
        if (relativePath.Length >= 2 && relativePath[1] == ':') return true;
        if (relativePath.StartsWith(@"\\", StringComparison.Ordinal)
            || relativePath.StartsWith("//", StringComparison.Ordinal))
            return true;

        foreach (var part in relativePath.Split('/', '\\'))
            if (part == "..")
                return true;
        return false;
    }

    private static bool IsTransientFile(string path)
    {
        var name = Path.GetFileName(path);
        return string.Equals(name, ".miniinflux.lock", StringComparison.OrdinalIgnoreCase)
            || name.StartsWith(".miniinflux.lock.", StringComparison.OrdinalIgnoreCase)
            || name.StartsWith(".restore-", StringComparison.OrdinalIgnoreCase)
            || name.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsTransientPath(string root, string path)
    {
        if (IsTransientFile(path)) return true;
        var relative = Path.GetRelativePath(root, path);
        foreach (var part in relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar))
            if (part.StartsWith(".restore-", StringComparison.OrdinalIgnoreCase))
                return true;
        return false;
    }

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (var file in Directory.GetFiles(source))
        {
            if (IsTransientFile(file)) continue;
            File.Copy(file, Path.Combine(destination, Path.GetFileName(file)), overwrite: true);
        }
        foreach (var dir in Directory.GetDirectories(source))
        {
            var name = Path.GetFileName(dir);
            if (name.StartsWith(".restore-", StringComparison.OrdinalIgnoreCase)) continue;
            CopyDirectory(dir, Path.Combine(destination, name));
        }
    }
}

public sealed record BackupMetadata(int FormatVersion, DateTimeOffset CreatedAtUtc, string SourceRoot, IReadOnlyList<BackupFileEntry> Files);
public sealed record BackupFileEntry(string RelativePath, long Length, string Sha256);
