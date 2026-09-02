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
        var root = Path.GetFullPath(dataPath);
        Directory.CreateDirectory(root);
        var lockPath = Path.Combine(root, ".miniinflux.lock");
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
