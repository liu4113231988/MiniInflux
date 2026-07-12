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
        var metadata = BuildMetadata(staging, source);
        File.WriteAllText(Path.Combine(staging, MetadataFileName), JsonSerializer.Serialize(metadata, AppJsonContext.Default.BackupMetadata));

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
            return;

        var metadata = JsonSerializer.Deserialize(File.ReadAllText(metadataPath), AppJsonContext.Default.BackupMetadata)
            as BackupMetadata
            ?? throw new InvalidDataException("backup metadata is invalid");

        foreach (var file in metadata.Files)
        {
            var fullPath = Path.Combine(backupPath, file.RelativePath);
            if (!File.Exists(fullPath))
                throw new InvalidDataException($"backup file missing: {file.RelativePath}");

            var info = new FileInfo(fullPath);
            if (info.Length != file.Length)
                throw new InvalidDataException($"backup file length mismatch: {file.RelativePath}");

            var hash = ComputeSha256(fullPath);
            if (!string.Equals(hash, file.Sha256, StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException($"backup file checksum mismatch: {file.RelativePath}");
        }
    }

    private static void ReplaceDirectory(string destination, string source)
    {
        if (Directory.Exists(destination)) Directory.Delete(destination, recursive: true);
        CopyDirectory(source, destination);
    }

    private static BackupMetadata BuildMetadata(string root, string sourceRoot)
    {
        var files = Directory.GetFiles(root, "*", SearchOption.AllDirectories)
            .Where(path => !string.Equals(Path.GetFileName(path), MetadataFileName, StringComparison.OrdinalIgnoreCase))
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

    private static string ComputeSha256(string path)
    {
        using var stream = File.OpenRead(path);
        var hash = SHA256.HashData(stream);
        return Convert.ToHexString(hash);
    }

    private static void EnsureDestinationOutsideSource(string source, string destination)
    {
        var sourceFull = Path.GetFullPath(source).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
        var destinationFull = Path.GetFullPath(destination).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
        if (destinationFull.StartsWith(sourceFull, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("backup destination must be outside the data directory");
    }

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (var file in Directory.GetFiles(source))
            File.Copy(file, Path.Combine(destination, Path.GetFileName(file)), overwrite: true);
        foreach (var dir in Directory.GetDirectories(source))
            CopyDirectory(dir, Path.Combine(destination, Path.GetFileName(dir)));
    }
}

public sealed record BackupMetadata(int FormatVersion, DateTimeOffset CreatedAtUtc, string SourceRoot, IReadOnlyList<BackupFileEntry> Files);
public sealed record BackupFileEntry(string RelativePath, long Length, string Sha256);
