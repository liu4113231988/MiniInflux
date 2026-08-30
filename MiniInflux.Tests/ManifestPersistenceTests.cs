using System.Text.Json;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class ManifestPersistenceTests : IDisposable
{
    private readonly string _testDir;

    public ManifestPersistenceTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_mfp_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    private static Point PointFor(string host, double value) => new()
    {
        Measurement = "cpu",
        Tags = new Dictionary<string, string> { ["host"] = host },
        Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(value) },
        TimestampNs = 1_000_000_000
    };

    [Fact]
    public async Task ManifestSave_HighCardinality_ExcludesSeriesPayloadFromFile()
    {
        var dataPath = Path.Combine(_testDir, "data");
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            var points = Enumerable.Range(0, 300)
                .Select(i => PointFor($"host{i:000}", i))
                .ToList();
            await engine.WriteAsync("testdb", "autogen", points);
            engine.FlushAll();
        }

        var manifestJson = File.ReadAllText(Path.Combine(dataPath, "meta", "manifest.json"));
        // The series payload must not be serialized into the manifest...
        Assert.DoesNotContain("host299", manifestJson);
        Assert.DoesNotContain("SeriesIndex", manifestJson);
        // ...and the file stays small regardless of cardinality.
        Assert.True(manifestJson.Length < 4_000, $"manifest.json was {manifestJson.Length} bytes");
    }

    [Fact]
    public async Task Recover_RebuildsSeriesAndTagIndexesFromSegments()
    {
        var dataPath = Path.Combine(_testDir, "data2");
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            var points = Enumerable.Range(0, 50)
                .Select(i => PointFor($"host{i:00}", i))
                .ToList();
            await engine.WriteAsync("testdb", "autogen", points);
            engine.FlushAll();
        }

        using var restarted = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        restarted.Recover();

        var series = restarted.Meta.GetSeries("testdb", "cpu");
        Assert.Equal(50, series.Count);
        // 50 distinct host tag values, all rebuilt from segment metadata.
        Assert.Equal(50, restarted.Meta.GetTagValueCardinality("testdb", "cpu", "host"));

        var executor = new QueryExecutor();
        var result = await executor.ExecuteAsync(restarted, "testdb", "SHOW TAG VALUES WITH KEY = \"host\"");
        Assert.Null(result.Results[0].Error);
        var values = result.Results[0].Series![0].Values;
        Assert.Equal(50, values.Count);
    }

    [Fact]
    public async Task Manifest_LegacyFormatWithIndexPayload_LoadsWithoutError()
    {
        var dataPath = Path.Combine(_testDir, "data3");
        using (var engine = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0))
        {
            await engine.WriteAsync("testdb", "autogen", [PointFor("host0", 1)]);
            engine.FlushAll();
        }

        // Simulate a legacy manifest that still carries the serialized index payload.
        var manifestPath = Path.Combine(dataPath, "meta", "manifest.json");
        var json = File.ReadAllText(manifestPath);
        var doc = JsonDocument.Parse(json);
        var dbs = doc.RootElement.GetProperty("Databases");
        var testdb = dbs.GetProperty("testdb");
        var legacy = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["Databases"] = new Dictionary<string, object>
            {
                ["testdb"] = new Dictionary<string, object>
                {
                    ["RetentionPolicies"] = testdb.GetProperty("RetentionPolicies"),
                    ["ContinuousQueries"] = testdb.GetProperty("ContinuousQueries"),
                    ["SeriesIndex"] = new Dictionary<string, string[]> { ["cpu"] = ["host=host0"] },
                    ["TagIndex"] = new Dictionary<string, Dictionary<string, string[]>> { ["cpu"] = new() { ["host"] = ["host0"] } }
                }
            }
        });
        File.WriteAllText(manifestPath, legacy);

        // Old payloads are ignored (unmapped members skipped) without breaking the load.
        using var engine2 = new TsdbEngine(dataPath, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        Assert.True(engine2.Meta.HasDatabase("testdb"));
    }
}
