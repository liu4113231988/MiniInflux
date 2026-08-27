namespace MiniInflux.Tests;

public sealed class V2WriteSupportTests
{
    [Fact]
    public void TryResolveBucket_PlainBucket_MapsToDatabaseWithAutogenRp()
    {
        Assert.True(V2WriteSupport.TryResolveBucket("metrics", out var db, out var rp, out _));
        Assert.Equal("metrics", db);
        Assert.Equal("autogen", rp);
    }

    [Fact]
    public void TryResolveBucket_DbSlashRpForm_SplitsRetentionPolicy()
    {
        Assert.True(V2WriteSupport.TryResolveBucket("metrics/7d", out var db, out var rp, out _));
        Assert.Equal("metrics", db);
        Assert.Equal("7d", rp);
    }

    [Fact]
    public void TryResolveBucket_MissingOrMalformed_ReturnsError()
    {
        Assert.False(V2WriteSupport.TryResolveBucket(null, out _, out _, out _));
        Assert.False(V2WriteSupport.TryResolveBucket("", out _, out _, out _));
        Assert.False(V2WriteSupport.TryResolveBucket("/7d", out _, out _, out _));
        Assert.False(V2WriteSupport.TryResolveBucket("metrics/", out _, out _, out _));
    }

    [Fact]
    public void TryResolveBucket_TrimsWhitespace_AroundBucketAndRp()
    {
        Assert.True(V2WriteSupport.TryResolveBucket(" metrics / 7d ", out var db, out var rp, out _));
        Assert.Equal("metrics", db);
        Assert.Equal("7d", rp);
    }

    [Fact]
    public void TryResolveBucket_ErrorMessage_MatchesV2Style()
    {
        Assert.False(V2WriteSupport.TryResolveBucket(null, out _, out _, out var err1));
        Assert.Equal("missing required parameter bucket", err1);
        Assert.False(V2WriteSupport.TryResolveBucket("metrics/", out _, out _, out var err2));
        Assert.Contains("invalid bucket name", err2);
    }
}
