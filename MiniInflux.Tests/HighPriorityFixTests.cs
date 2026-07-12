using System.Text.Json;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class HighPriorityFixTests : IDisposable
{
    private readonly string _testDir;

    public HighPriorityFixTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_high_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public void AppSettings_IsSingleValidJsonObject()
    {
        var path = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../MiniInflux/appsettings.json"));
        using var doc = JsonDocument.Parse(File.ReadAllText(path));

        Assert.Equal(JsonValueKind.Object, doc.RootElement.ValueKind);
        Assert.True(doc.RootElement.TryGetProperty("MiniInflux", out _));
        Assert.True(doc.RootElement.TryGetProperty("Wal", out _));
        Assert.True(doc.RootElement.TryGetProperty("Write", out _));
        Assert.True(doc.RootElement.TryGetProperty("Storage", out _));
    }

    [Fact]
    public async Task ListMeasurements_ReturnsMeasurementNames_NotFieldNames()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen", [Point("cpu", "value", 1.0, "server01", 1)]);

        var measurements = engine.ListMeasurements("testdb");

        Assert.Contains("cpu", measurements);
        Assert.DoesNotContain("value", measurements);
    }

    [Fact]
    public async Task FirstWrite_AutoCreatesDatabase_AndDefaultRetentionPolicy()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);

        await engine.WriteAsync("metrics", "autogen", [Point("cpu", "value", 1.0, "server01", 1)]);

        Assert.Contains("metrics", engine.ListDatabases());
        Assert.Equal("autogen", engine.GetDefaultRpName("metrics"));
        var point = Assert.Single(engine.ReadAllPoints("metrics", "autogen", "cpu", null, null));
        Assert.Equal(1.0, point.Fields["value"].Float);
    }

    [Fact]
    public async Task Select_UsesDefaultRetentionPolicy()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        engine.CreateDatabase("testdb");
        engine.Meta.CreateRetentionPolicy("testdb", "one_week", 7 * 86_400_000_000_000L, isDefault: true);
        await engine.WriteAsync("testdb", "one_week", [Point("cpu", "value", 1.5, "server01", 1)]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb", "SELECT value FROM cpu");

        var series = Assert.Single(response.Results[0].Series!);
        var row = Assert.Single(series.Values);
        Assert.Equal(1.5, row[^1]);
    }

    [Fact]
    public async Task Delete_WithTagPredicate_DeletesOnlyMatchingSeries()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1.0, "server01", 1),
            Point("cpu", "value", 2.0, "server02", 1)
        ]);

        await new QueryExecutor().ExecuteAsync(engine, "testdb", "DELETE FROM cpu WHERE host='server01'");

        var points = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);
        var point = Assert.Single(points);
        Assert.Equal("server02", point.Tags["host"]);
    }

    [Fact]
    public async Task Delete_WithFieldPredicate_DeletesOnlyMatchingPoints()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1.0, "server01", 1),
            Point("cpu", "value", 2.0, "server02", 2)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb", "DELETE FROM cpu WHERE value > 1");

        Assert.Null(response.Results[0].Error);
        var points = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);
        var point = Assert.Single(points);
        Assert.Equal("server01", point.Tags["host"]);
    }

    [Fact]
    public async Task DropSeries_WithFieldPredicate_DropsOnlyMatchingSeries()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1.0, "server01", 1),
            Point("cpu", "value", 10.0, "server02", 2),
            Point("cpu", "value", 20.0, "server02", 3)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb", "DROP SERIES FROM cpu WHERE value > 5");

        Assert.Null(response.Results[0].Error);
        var points = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);
        var point = Assert.Single(points);
        Assert.Equal("server01", point.Tags["host"]);
    }

    [Fact]
    public async Task Delete_WithTagAndFieldPredicate_DeletesOnlyMatchingSubset()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1.0, "server01", 1),
            Point("cpu", "value", 10.0, "server01", 2),
            Point("cpu", "value", 10.0, "server02", 3)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb", "DELETE FROM cpu WHERE host='server01' AND value > 5");

        Assert.Null(response.Results[0].Error);
        var points = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null)
            .OrderBy(p => p.TimestampNs)
            .ToList();
        Assert.Equal(2, points.Count);
        Assert.Equal("server01", points[0].Tags["host"]);
        Assert.Equal(1.0, points[0].Fields["value"].AsDouble());
        Assert.Equal("server02", points[1].Tags["host"]);
    }

    [Fact]
    public async Task Delete_WithQuotedMeasurementAndQuotedFieldPredicates_DeletesOnlyMatchingPoints()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu load", "server01", new Dictionary<string, FieldValue>
            {
                ["value"] = FieldValue.FromDouble(5),
                ["temp c"] = FieldValue.FromDouble(80)
            }, 1),
            Point("cpu load", "server01", new Dictionary<string, FieldValue>
            {
                ["value"] = FieldValue.FromDouble(15),
                ["temp c"] = FieldValue.FromDouble(80)
            }, 2),
            Point("cpu load", "server02", new Dictionary<string, FieldValue>
            {
                ["value"] = FieldValue.FromDouble(5),
                ["temp c"] = FieldValue.FromDouble(80)
            }, 3)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(
            engine,
            "testdb",
            "DELETE FROM \"cpu load\" WHERE host = 'server01' AND \"temp c\" >= 70 AND value < 10");

        Assert.Null(response.Results[0].Error);
        var points = engine.ReadAllPoints("testdb", "autogen", "cpu load", null, null)
            .OrderBy(p => p.TimestampNs)
            .ToList();
        Assert.Equal(2, points.Count);
        Assert.Equal("server01", points[0].Tags["host"]);
        Assert.Equal(15.0, points[0].Fields["value"].AsDouble());
        Assert.Equal("server02", points[1].Tags["host"]);
    }

    [Fact]
    public async Task Delete_WithFieldPredicate_OnFlushedSeries_DoesNotDeleteNonMatchingPoints()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1.0, "server01", 1),
            Point("cpu", "value", 10.0, "server01", 2)
        ]);
        engine.FlushAll();

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb", "DELETE FROM cpu WHERE value > 5");

        Assert.Null(response.Results[0].Error);
        var points = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null)
            .OrderBy(p => p.TimestampNs)
            .ToList();
        var point = Assert.Single(points);
        Assert.Equal(1_000_000_000L, point.TimestampNs);
        Assert.Equal(1.0, point.Fields["value"].AsDouble());
    }

    [Fact]
    public async Task Delete_WithFieldPredicate_AppliesToNonDefaultRetentionPolicy()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        engine.CreateDatabase("testdb");
        engine.Meta.CreateRetentionPolicy("testdb", "archive", 0, false);
        await engine.WriteAsync("testdb", "archive",
        [
            Point("cpu", "value", 1.0, "server01", 1),
            Point("cpu", "value", 10.0, "server01", 2)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb", "DELETE FROM cpu WHERE value > 5");

        Assert.Null(response.Results[0].Error);
        var points = engine.ReadAllPoints("testdb", "archive", "cpu", null, null);
        var point = Assert.Single(points);
        Assert.Equal(1.0, point.Fields["value"].AsDouble());
    }

    [Fact]
    public async Task Delete_WithQualifiedRetentionPolicy_DeletesOnlyTargetRp()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        engine.CreateDatabase("testdb");
        engine.Meta.CreateRetentionPolicy("testdb", "archive", 0, false);
        await engine.WriteAsync("testdb", "archive",
        [
            Point("cpu", "value", 1.0, "server01", 1),
            Point("cpu", "value", 10.0, "server01", 2)
        ]);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 10.0, "server01", 3)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb", "DELETE FROM archive.cpu WHERE value >= 10");

        Assert.Null(response.Results[0].Error);
        var archivePoints = engine.ReadAllPoints("testdb", "archive", "cpu", null, null);
        var autogenPoints = engine.ReadAllPoints("testdb", "autogen", "cpu", null, null);
        var archivePoint = Assert.Single(archivePoints);
        Assert.Equal(1.0, archivePoint.Fields["value"].AsDouble());
        var autogenPoint = Assert.Single(autogenPoints);
        Assert.Equal(10.0, autogenPoint.Fields["value"].AsDouble());
    }

    [Fact]
    public async Task DropSeries_WithFieldPredicate_FindsMatchesAcrossRetentionPolicies()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        engine.CreateDatabase("testdb");
        engine.Meta.CreateRetentionPolicy("testdb", "archive", 0, false);
        await engine.WriteAsync("testdb", "archive",
        [
            Point("cpu", "value", 10.0, "server02", 1)
        ]);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1.0, "server01", 1)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb", "DROP SERIES FROM cpu WHERE value > 5");

        Assert.Null(response.Results[0].Error);
        Assert.Single(engine.ReadAllPoints("testdb", "autogen", "cpu", null, null));
        Assert.Empty(engine.ReadAllPoints("testdb", "archive", "cpu", null, null));
    }

    [Fact]
    public async Task DropSeries_WithQuotedMeasurementAndMultipleFieldPredicates_DropsOnlyMatchingSeries()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu load", "server01", new Dictionary<string, FieldValue>
            {
                ["usage"] = FieldValue.FromDouble(5),
                ["temp c"] = FieldValue.FromDouble(80)
            }, 1),
            Point("cpu load", "server02", new Dictionary<string, FieldValue>
            {
                ["usage"] = FieldValue.FromDouble(5),
                ["temp c"] = FieldValue.FromDouble(60)
            }, 2),
            Point("cpu load", "server03", new Dictionary<string, FieldValue>
            {
                ["usage"] = FieldValue.FromDouble(0),
                ["temp c"] = FieldValue.FromDouble(80)
            }, 3)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(
            engine,
            "testdb",
            "DROP SERIES FROM \"cpu load\" WHERE \"temp c\" >= 70 AND usage != 0");

        Assert.Null(response.Results[0].Error);
        var points = engine.ReadAllPoints("testdb", "autogen", "cpu load", null, null)
            .OrderBy(p => p.Tags["host"], StringComparer.Ordinal)
            .ToList();
        Assert.Equal(2, points.Count);
        Assert.DoesNotContain(points, p => p.Tags["host"] == "server01");
        Assert.Contains(points, p => p.Tags["host"] == "server02");
        Assert.Contains(points, p => p.Tags["host"] == "server03");
    }

    [Fact]
    public async Task DropSeries_WithMultipleMeasurements_DropsMatchesAcrossListedMeasurementsOnly()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1.0, "server01", 1),
            Point("mem", "value", 2.0, "server01", 1),
            Point("disk", "value", 3.0, "server01", 1),
            Point("cpu", "value", 4.0, "server02", 2)
        ]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "testdb", "DROP SERIES FROM cpu,mem WHERE host='server01'");

        Assert.Null(response.Results[0].Error);
        Assert.Single(engine.ReadAllPoints("testdb", "autogen", "cpu", null, null));
        Assert.Empty(engine.ReadAllPoints("testdb", "autogen", "mem", null, null));
        Assert.Single(engine.ReadAllPoints("testdb", "autogen", "disk", null, null));
    }

    [Fact]
    public async Task TagSeriesIndex_TracksSeriesForPredicatePushdown()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1.0, "server01", 1),
            Point("cpu", "value", 2.0, "server02", 1)
        ]);

        var series = engine.GetSeriesForTagValue("testdb", "cpu", "host", "server01");

        var only = Assert.Single(series);
        Assert.Contains("host=server01", only);
    }

    [Fact]
    public async Task QueryExecutor_AppliesMaxResponseRows()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1.0, "server01", 1),
            Point("cpu", "value", 2.0, "server01", 2)
        ]);

        var response = await new QueryExecutor(maxResponseRows: 1).ExecuteAsync(engine, "testdb", "SELECT value FROM cpu");

        var series = Assert.Single(response.Results[0].Series!);
        Assert.Single(series.Values);
    }

    [Fact]
    public async Task CardinalityLimit_CountsDistinctNewSeries()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1000, maxSeriesPerDb: 1);
        await engine.WriteAsync("testdb", "autogen",
        [
            Point("cpu", "value", 1.0, "server01", 1),
            Point("cpu", "temp", 2.0, "server01", 2)
        ]);

        await Assert.ThrowsAsync<CardinalityLimitExceededException>(() =>
            engine.WriteAsync("testdb", "autogen", [Point("cpu", "value", 3.0, "server02", 3)]));
    }

    [Fact]
    public async Task RestartRecovery_DoesNotDuplicateFlushedCurrentWalFile()
    {
        await using (var engine = new AsyncEngine(new TsdbEngine(_testDir, flushThreshold: 1000)))
        {
            await engine.Value.WriteAsync("testdb", "autogen", [Point("cpu", "value", 1.0, "server01", 1)]);
        }

        using var restarted = new TsdbEngine(_testDir, flushThreshold: 1000);
        restarted.Recover();

        var points = restarted.ReadAllPoints("testdb", "autogen", "cpu", null, null);
        var point = Assert.Single(points);
        Assert.Equal(1.0, point.Fields["value"].Float);
    }

    private static Point Point(string measurement, string field, double value, string host, long seconds) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { [field] = FieldValue.FromDouble(value) },
        TimestampNs = seconds * 1_000_000_000
    };

    private static Point Point(string measurement, string host, Dictionary<string, FieldValue> fields, long seconds) => new()
    {
        Measurement = measurement,
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = fields,
        TimestampNs = seconds * 1_000_000_000
    };

    private sealed class AsyncEngine : IAsyncDisposable
    {
        public AsyncEngine(TsdbEngine value) => Value = value;
        public TsdbEngine Value { get; }
        public ValueTask DisposeAsync()
        {
            Value.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
