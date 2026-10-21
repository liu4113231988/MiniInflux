namespace MiniInflux.Net10.Storage;

/// <summary>
/// Cross-process exclusive lock for a MiniInflux data directory. Both the server and
/// offline management CLI hold this lock for their entire operation so a repair,
/// compaction, or restore can never race a live writer.
/// </summary>
public sealed class DataDirectoryLock : IDisposable
{
    private readonly FileStream _stream;

    private DataDirectoryLock(FileStream stream) => _stream = stream;

    public static DataDirectoryLock Acquire(string dataPath)
    {
        var root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(dataPath));
        Directory.CreateDirectory(root);

        // Keep the lock next to (not inside) the data directory. Backup/restore move or copy the
        // data tree, and Windows refuses those operations while an open handle lives under it.
        // The sibling path is deterministic, so the server and CLI still contend for the lock.
        var lockPath = root + ".miniinflux.lock";
        try
        {
            var stream = new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            return new DataDirectoryLock(stream);
        }
        catch (IOException ex)
        {
            throw new IOException($"data directory is already in use: {root}. Stop MiniInflux before running an offline management command.", ex);
        }
    }

    public void Dispose() => _stream.Dispose();
}
