using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class CsvQueryResponseTests : IDisposable
{
    private readonly string _testDir;
    private readonly TsdbEngine _engine;
    private readonly QueryExecutor _executor;

    public CsvQueryResponseTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_csv_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
        _engine = new TsdbEngine(_testDir, flushThreshold: 1, rpCheckIntervalMs: 0, flushIntervalMs: 0, compactionIntervalMs: 0);
        _executor = new QueryExecutor();
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public async Task Write_RendersSeriesBlocks_WithTagsAndValues()
    {
        await _engine.WriteAsync("testdb", "autogen",
        [
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { ["host"] = "a" },
                Fields = new Dictionary<string, FieldValue> { ["value"] = FieldValue.FromDouble(1.5), ["note"] = FieldValue.FromString("ok") },
                TimestampNs = 1_000_000_000
            }
        ]);

        // GROUP BY host guarantees per-series tags and a stable column set.
        var result = await _executor.ExecuteAsync(_engine, "testdb", "SELECT mean(value), count(note) FROM cpu GROUP BY host");
        var csv = CsvQueryResponseWriter.Write(result);

        var lines = csv.Split('\n', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        // header + one data row (per series block)
        Assert.StartsWith("name,tags,", lines[0]);
        Assert.Contains("time", lines[0]);
        Assert.Contains("mean_value", lines[0]);
        Assert.Contains("count_note", lines[0]);

        var dataRow = lines[1];
        Assert.Contains("cpu", dataRow);
        Assert.Contains("host=a", dataRow);
        Assert.EndsWith("1.5,1", dataRow);
    }

    [Fact]
    public void Write_EscapesCommasQuotesAndNulls()
    {
        var response = new QueryResponse
        {
            Results =
            [
                new QueryResult
                {
                    Series =
                    [
                        new QuerySeries
                        {
                            Name = "cpu,meta",
                            Tags = new Dictionary<string, string> { ["host"] = "a\"b" },
                            Columns = ["time", "value"],
                            Values = [[ "1970-01-01T00:00:00.000000000Z", null, 2.5 ]]
                        }
                    ]
                }
            ]
        };

        var csv = CsvQueryResponseWriter.Write(response);
        var lines = csv.Split('\n', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

        // The header's column list contains a comma, so it is quoted as one CSV field.
        Assert.Contains("\"time,value\"", lines[0]);
        Assert.Contains("\"cpu,meta\"", lines[1]);
        Assert.Contains("\"host=", lines[1]);
        Assert.Contains("a\"\"b", lines[1]);
        Assert.EndsWith(",,2.5", lines[1]);
    }
}
