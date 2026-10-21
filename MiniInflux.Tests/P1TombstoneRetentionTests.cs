using MiniInflux.Net10.Model;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

/// <summary>
/// P1-9 regression tests: tombstones are scoped to a retention policy so deleting data in one RP
/// no longer hides identically named series/time ranges in another RP of the same database.
/// </summary>
public class P1TombstoneRetentionTests : IDisposable
{
    private readonly string _testDir;

    public P1TombstoneRetentionTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p1tomb_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDir))
                Directory.Delete(_testDir, true);
        }
        catch (IOException) { }
    }

    [Fact]
    public void AddMeasurementDelete_WithRetentionPolicy_FiltersOnlyThatPolicy()
    {
        var store = new TombstoneStore(_testDir);
        store.AddMeasurementDelete("db1", "cpu", 1, 10, retentionPolicy: "short");

        Assert.True(store.IsColumnDeleted("db1", "cpu", "host=server01", 1, 10, "short"));
        Assert.False(store.IsColumnDeleted("db1", "cpu", "host=server01", 1, 10, "long"));
    }

    [Fact]
    public void AddMeasurementDelete_LegacyWithoutRetentionPolicy_StillAppliesToEveryPolicy()
    {
        var store = new TombstoneStore(_testDir);
        store.AddMeasurementDelete("db1", "cpu", 1, 10);

        Assert.True(store.IsColumnDeleted("db1", "cpu", "host=server01", 1, 10, "short"));
        Assert.True(store.IsColumnDeleted("db1", "cpu", "host=server01", 1, 10, "long"));
    }

    [Fact]
    public void RemoveCoveredRange_OnlyRetiresMatchingRetentionPolicy()
    {
        var store = new TombstoneStore(_testDir);
        store.AddMeasurementDelete("db1", "cpu", 1, 10, retentionPolicy: "short");
        store.AddMeasurementDelete("db1", "cpu", 1, 10, retentionPolicy: "long");

        store.RemoveCoveredRange("db1", 1, 10, "short");

        Assert.False(store.IsColumnDeleted("db1", "cpu", "host=server01", 1, 10, "short"));
        Assert.True(store.IsColumnDeleted("db1", "cpu", "host=server01", 1, 10, "long"));
    }

    [Fact]
    public void DropRetentionPolicy_RemovesOnlyScopedTombstones_AndPersistsTheChange()
    {
        var store = new TombstoneStore(_testDir);
        store.AddMeasurementDelete("db1", "cpu", 1, 10, retentionPolicy: "short");
        store.AddMeasurementDelete("db1", "cpu", 20, 30, retentionPolicy: "long");
        store.AddMeasurementDelete("db1", "cpu", 40, 50);

        store.DropRetentionPolicy("db1", "short");

        var reloaded = new TombstoneStore(_testDir);
        Assert.False(reloaded.IsColumnDeleted("db1", "cpu", "host=server01", 1, 10, "short"));
        Assert.True(reloaded.IsColumnDeleted("db1", "cpu", "host=server01", 20, 30, "long"));
        Assert.True(reloaded.IsColumnDeleted("db1", "cpu", "host=server01", 40, 50, "short"));
    }

    [Fact]
    public void TombstoneRetentionPolicy_RoundTripsThroughPersistence()
    {
        var store = new TombstoneStore(_testDir);
        store.AddSeriesDelete("db1", "cpu", "host=server01", 5, 7, retentionPolicy: "short");

        var reloaded = new TombstoneStore(_testDir);
        Assert.True(reloaded.IsColumnDeleted("db1", "cpu", "host=server01", 5, 7, "short"));
        Assert.False(reloaded.IsColumnDeleted("db1", "cpu", "host=server01", 5, 7, "long"));
    }

    [Fact]
    public async Task DeleteFromMeasurement_OnlyDeletesMatchingRetentionPolicyData()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        engine.CreateDatabase("tombdb");
        engine.Meta.CreateRetentionPolicy("tombdb", "short", 3_600_000_000_000L, isDefault: false);
        engine.Meta.CreateRetentionPolicy("tombdb", "long", 0, isDefault: false);

        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000;
        await engine.WriteAsync("tombdb", "short", [PointAt("cpu", 1, "server01", ts)]);
        await engine.WriteAsync("tombdb", "long", [PointAt("cpu", 2, "server01", ts)]);
        engine.FlushAll();

        engine.DeleteFromMeasurement("tombdb", "short", "cpu", ts - 1_000_000_000L, ts + 1_000_000_000L);

        Assert.Empty(engine.ReadAllPoints("tombdb", "short", "cpu", null, null));
        var survivor = Assert.Single(engine.ReadAllPoints("tombdb", "long", "cpu", null, null));
        Assert.Equal(2d, survivor.Fields["value"].AsDouble());
    }

    [Fact]
    public async Task DropMeasurement_DoesNotHideSameMeasurementInOtherDatabase()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("db1", "autogen", [PointAt("cpu", 1, "server01", 1_000_000_000L)]);
        await engine.WriteAsync("db2", "autogen", [PointAt("cpu", 2, "server01", 1_000_000_000L)]);
        engine.FlushAll();

        engine.DropMeasurement("db1", "cpu");

        Assert.Empty(engine.ReadAllPoints("db1", "autogen", "cpu", null, null));
        Assert.Single(engine.ReadAllPoints("db2", "autogen", "cpu", null, null));
    }

    private static Point PointAt(string measurement, double value, string host, long timestampNs) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = timestampNs
    };
}
