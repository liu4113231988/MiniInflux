using MiniInflux.Net10;
using MiniInflux.Net10.Protocol;

namespace MiniInflux.Tests;

public sealed class AdminCommandSupportTests
{
    [Theory]
    [InlineData(QueryKind.CreateDatabase)]
    [InlineData(QueryKind.DropDatabase)]
    [InlineData(QueryKind.CreateRetentionPolicy)]
    [InlineData(QueryKind.DropRetentionPolicy)]
    [InlineData(QueryKind.CreateContinuousQuery)]
    [InlineData(QueryKind.DropContinuousQuery)]
    public void IsAllowed_ConsoleManagementOperation_ReturnsTrue(QueryKind kind) =>
        Assert.True(AdminCommandSupport.IsAllowed(kind));

    [Theory]
    [InlineData(QueryKind.Select)]
    [InlineData(QueryKind.ShowDatabases)]
    [InlineData(QueryKind.Delete)]
    [InlineData(QueryKind.DropMeasurement)]
    [InlineData(QueryKind.DropShard)]
    public void IsAllowed_ReadOrUnexposedOperation_ReturnsFalse(QueryKind kind) =>
        Assert.False(AdminCommandSupport.IsAllowed(kind));
}
