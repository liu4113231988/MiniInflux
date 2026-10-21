using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public sealed class DataDirectoryLockTests : IDisposable
{
    private readonly string _dataPath = Path.Combine(Path.GetTempPath(), "miniinflux-lock-tests", Guid.NewGuid().ToString("N"));

    [Fact]
    public void Acquire_WhenAlreadyHeld_RejectsConcurrentOwner()
    {
        using var first = DataDirectoryLock.Acquire(_dataPath);

        Assert.True(File.Exists(Path.TrimEndingDirectorySeparator(Path.GetFullPath(_dataPath)) + ".miniinflux.lock"));
        Assert.False(File.Exists(Path.Combine(_dataPath, ".miniinflux.lock")));

        var exception = Assert.Throws<IOException>(() => DataDirectoryLock.Acquire(_dataPath));

        Assert.Contains("already in use", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_dataPath))
                Directory.Delete(_dataPath, recursive: true);
        }
        catch
        {
            // Test cleanup is best effort on platforms that defer file-handle release.
        }

        try
        {
            var lockPath = Path.TrimEndingDirectorySeparator(Path.GetFullPath(_dataPath)) + ".miniinflux.lock";
            if (File.Exists(lockPath))
                File.Delete(lockPath);
        }
        catch
        {
            // Test cleanup is best effort.
        }
    }
}
