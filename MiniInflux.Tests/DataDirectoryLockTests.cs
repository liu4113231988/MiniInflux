using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public sealed class DataDirectoryLockTests : IDisposable
{
    private readonly string _dataPath = Path.Combine(Path.GetTempPath(), "miniinflux-lock-tests", Guid.NewGuid().ToString("N"));

    [Fact]
    public void Acquire_WhenAlreadyHeld_RejectsConcurrentOwner()
    {
        using var first = DataDirectoryLock.Acquire(_dataPath);

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
    }
}
