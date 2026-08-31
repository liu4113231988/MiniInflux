using System.Text.Json;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

/// <summary>
/// P2/P3 convergence: `$name` placeholder parsing (ParamFilter), AST-level parameter
/// binding (ApplyParams) with template parse-cache reuse, and end-to-end execution
/// through QueryExecutor with bound parameters.
/// </summary>
public class P3ParameterizedQueryTests : IDisposable
{
    private readonly string _testDir;

    public P3ParameterizedQueryTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_p3_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    static Dictionary<string, JsonElement> Params(string json) =>
        JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json)!;

    // ---- parser: placeholder extraction ----

    [Fact]
    public void InfluxQlParser_TagEqualityPlaceholder_ProducesParamFilter()
    {
        var parsed = InfluxQlParser.Parse("SELECT value FROM cpu WHERE host = $host");
        var param = Assert.Single(parsed.ParamFilters);
        Assert.Equal("host", param.Key);
        Assert.Equal("host", param.Name);
        Assert.Equal("=", param.Op);
        Assert.Empty(parsed.TagFilters);
    }

    [Fact]
    public void InfluxQlParser_FieldComparisonAndTimePlaceholders_ProduceParamFilters()
    {
        var parsed = InfluxQlParser.Parse("SELECT value FROM cpu WHERE value > $min AND time >= $start");
        Assert.Equal(2, parsed.ParamFilters.Count);
        Assert.Contains(parsed.ParamFilters, p => p.Key == "value" && p.Name == "min" && p.Op == ">");
        Assert.Contains(parsed.ParamFilters, p => p.Key == "time" && p.Name == "start" && p.Op == ">=");
    }

    [Fact]
    public void InfluxQlParser_RegexAndNotEqualPlaceholders_ProduceParamFilters()
    {
        var parsed = InfluxQlParser.Parse("SELECT value FROM cpu WHERE host =~ $hostRe AND region != $region");
        Assert.Equal(2, parsed.ParamFilters.Count);
        Assert.Contains(parsed.ParamFilters, p => p.Op == "=~");
        Assert.Contains(parsed.ParamFilters, p => p.Op == "!=");
    }

    [Fact]
    public void InfluxQlParser_PlaceholderInsideStringLiteral_StaysLiteral()
    {
        var parsed = InfluxQlParser.Parse("SELECT value FROM cpu WHERE msg = 'cost $amount total'");
        Assert.Empty(parsed.ParamFilters);
        var tag = Assert.Single(parsed.TagFilters);
        Assert.Equal("msg", tag.Key);
        Assert.Equal("cost $amount total", tag.Value);
    }

    [Fact]
    public void InfluxQlParser_OrPredicateWithPlaceholder_Throws()
    {
        Assert.Throws<FormatException>(() =>
            InfluxQlParser.Parse("SELECT value FROM cpu WHERE host = $host OR region = 'west'"));
    }

    [Fact]
    public void InfluxQlParser_SubqueryPlaceholder_NestsInsideSubquery()
    {
        var parsed = InfluxQlParser.Parse("SELECT mean(v) FROM (SELECT value AS v FROM cpu WHERE host = $host)");
        Assert.Empty(parsed.ParamFilters);
        Assert.NotNull(parsed.Subquery);
        Assert.Single(parsed.Subquery!.ParamFilters);
        Assert.True(QueryParamBinder.HasUnboundParams(parsed));
    }

    // ---- binder: ApplyParams semantics ----

    [Fact]
    public void ApplyParams_MissingBinding_ThrowsWithParamName()
    {
        var template = InfluxQlParser.Parse("SELECT value FROM cpu WHERE host = $host");
        var ex = Assert.Throws<FormatException>(() => QueryParamBinder.ApplyParams(template, null));
        Assert.Contains("$host", ex.Message);
        Assert.Throws<FormatException>(() => QueryParamBinder.ApplyParams(template, Params("{}")));
    }

    [Fact]
    public void ApplyParams_StringValue_BindsTagFilter()
    {
        var template = InfluxQlParser.Parse("SELECT value FROM cpu WHERE host = $host");
        var bound = QueryParamBinder.ApplyParams(template, Params("""{"host":"server01"}"""));
        Assert.Empty(bound.ParamFilters);
        var tag = Assert.Single(bound.TagFilters);
        Assert.Equal("host", tag.Key);
        Assert.Equal("server01", tag.Value);
        Assert.Equal(TagOp.Eq, tag.Op);
    }

    [Fact]
    public void ApplyParams_NumberValue_BindsFieldFilter()
    {
        var template = InfluxQlParser.Parse("SELECT value FROM cpu WHERE value > $min");
        var bound = QueryParamBinder.ApplyParams(template, Params("""{"min":50}"""));
        var field = Assert.Single(bound.FieldFilters);
        Assert.Equal("value", field.Field);
        Assert.Equal(50, field.Value);
        Assert.Equal(FieldOp.Gt, field.Op);
    }

    [Fact]
    public void ApplyParams_EqualityWithNumber_BindsFieldFilter()
    {
        var template = InfluxQlParser.Parse("SELECT value FROM cpu WHERE value = $v");
        var bound = QueryParamBinder.ApplyParams(template, Params("""{"v":42}"""));
        var field = Assert.Single(bound.FieldFilters);
        Assert.Equal(FieldOp.Eq, field.Op);
    }

    [Fact]
    public void ApplyParams_BoolAndNullValues_BindTagFilters()
    {
        var template = InfluxQlParser.Parse("SELECT value FROM cpu WHERE ok = $ok AND z != $z");
        var bound = QueryParamBinder.ApplyParams(template, Params("""{"ok":true,"z":null}"""));
        Assert.Equal(2, bound.TagFilters.Count);
        Assert.Contains(bound.TagFilters, t => t.Value == "true" && t.Op == TagOp.Eq);
        Assert.Contains(bound.TagFilters, t => t.Value == "null" && t.Op == TagOp.Neq);
    }

    [Fact]
    public void ApplyParams_RegexValue_BindsRegexTagFilter()
    {
        var template = InfluxQlParser.Parse("SELECT value FROM cpu WHERE host =~ $hostRe");
        var bound = QueryParamBinder.ApplyParams(template, Params("""{"hostRe":"^web-"}"""));
        var tag = Assert.Single(bound.TagFilters);
        Assert.Equal("^web-", tag.Value);
        Assert.Equal(TagOp.Regex, tag.Op);
    }

    [Fact]
    public void ApplyParams_TimeStringValue_BindsMinTimeNs()
    {
        var template = InfluxQlParser.Parse("SELECT value FROM cpu WHERE time >= $start");
        var bound = QueryParamBinder.ApplyParams(template, Params("""{"start":"2026-01-01T00:00:00Z"}"""));
        var expected = DateTimeOffset.Parse("2026-01-01T00:00:00Z").ToUnixTimeMilliseconds() * 1_000_000L;
        Assert.Equal(expected, bound.MinTimeNs);
    }

    [Fact]
    public void ApplyParams_NumberTimeValue_BindsTimeBound()
    {
        var template = InfluxQlParser.Parse("SELECT value FROM cpu WHERE time < $end");
        var bound = QueryParamBinder.ApplyParams(template, Params("""{"end":1700000000000000000}"""));
        Assert.Equal(1_699_999_999_999_999_999L, bound.MaxTimeNs);
    }

    [Fact]
    public void ApplyParams_TimeParamIntersects_WithLiteralTimeBound()
    {
        var template = InfluxQlParser.Parse("SELECT value FROM cpu WHERE time >= 1000 AND time >= $start");
        var bound = QueryParamBinder.ApplyParams(template, Params("""{"start":2000}"""));
        Assert.Equal(2000, bound.MinTimeNs);
    }

    [Fact]
    public void ApplyParams_NonNumericStringForRange_Throws()
    {
        var template = InfluxQlParser.Parse("SELECT value FROM cpu WHERE value > $min");
        Assert.Throws<FormatException>(() => QueryParamBinder.ApplyParams(template, Params("""{"min":"abc"}""")));
    }

    [Fact]
    public void ApplyParams_NonSelectStatement_Throws()
    {
        var template = InfluxQlParser.Parse("DELETE FROM cpu WHERE host = $host");
        Assert.Throws<FormatException>(() => QueryParamBinder.ApplyParams(template, Params("""{"host":"a"}""")));
    }

    [Fact]
    public void ApplyParams_InjectionAttempt_BindsAsLiteralTagValue()
    {
        var template = InfluxQlParser.Parse("SELECT value FROM cpu WHERE host = $host");
        var bound = QueryParamBinder.ApplyParams(template, Params("""{"host":"a' OR '1'='1"}"""));
        var tag = Assert.Single(bound.TagFilters);
        Assert.Equal("a' OR '1'='1", tag.Value);
    }

    [Fact]
    public void ApplyParams_DoesNotMutateCachedTemplate()
    {
        var template = InfluxQlParser.Parse("SELECT value FROM cpu WHERE host = $host");
        var first = QueryParamBinder.ApplyParams(template, Params("""{"host":"a"}"""));
        Assert.Equal("a", Assert.Single(first.TagFilters).Value);

        Assert.Single(template.ParamFilters);
        Assert.Empty(template.TagFilters);

        var second = QueryParamBinder.ApplyParams(template, Params("""{"host":"b"}"""));
        Assert.Equal("b", Assert.Single(second.TagFilters).Value);
        Assert.Single(template.ParamFilters);
        Assert.Empty(template.TagFilters);
    }

    [Fact]
    public void ApplyParams_SubqueryPlaceholder_BindsInsideSubquery()
    {
        var template = InfluxQlParser.Parse("SELECT mean(v) FROM (SELECT value AS v FROM cpu WHERE host = $host)");
        var bound = QueryParamBinder.ApplyParams(template, Params("""{"host":"server01"}"""));
        Assert.Empty(bound.ParamFilters);
        Assert.NotNull(bound.Subquery);
        var tag = Assert.Single(bound.Subquery!.TagFilters);
        Assert.Equal("server01", tag.Value);
    }

    // ---- parse cache reuse ----

    [Fact]
    public void ParseCached_SameTemplate_ReturnsSameInstance()
    {
        const string template = "SELECT value FROM cpu WHERE host = $host";
        Assert.Same(QueryParamBinder.ParseCached(template), QueryParamBinder.ParseCached(template));
    }

    // ---- executor end-to-end with engine ----

    [Fact]
    public async Task ExecuteAsync_BoundTagParam_FiltersRows()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1_000_000);
        await engine.WriteAsync("db", "autogen",
        [
            new Point { Measurement = "cpu", Tags = new() { ["host"] = "a" }, Fields = new() { ["value"] = FieldValue.FromDouble(1) }, TimestampNs = 1_000 },
            new Point { Measurement = "cpu", Tags = new() { ["host"] = "a" }, Fields = new() { ["value"] = FieldValue.FromDouble(2) }, TimestampNs = 2_000 },
            new Point { Measurement = "cpu", Tags = new() { ["host"] = "b" }, Fields = new() { ["value"] = FieldValue.FromDouble(3) }, TimestampNs = 3_000 },
        ]);

        var response = await new QueryExecutor().ExecuteAsync(
            engine, "db", "SELECT value FROM cpu WHERE host = $host", default, Params("""{"host":"a"}"""));

        Assert.Null(response.Results[0].Error);
        var series = Assert.Single(response.Results[0].Series!);
        Assert.Equal(2, series.Values.Count);
    }

    [Fact]
    public async Task ExecuteAsync_BoundTimeAndFieldParams_FiltersRows()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1_000_000);
        var points = Enumerable.Range(0, 10).Select(i => new Point
        {
            Measurement = "cpu",
            Tags = new() { ["host"] = "a" },
            Fields = new() { ["value"] = FieldValue.FromDouble(i) },
            TimestampNs = (i + 1) * 1_000
        }).ToList();
        await engine.WriteAsync("db", "autogen", points);

        var response = await new QueryExecutor().ExecuteAsync(
            engine, "db", "SELECT value FROM cpu WHERE time >= $start AND value > $min",
            default, Params("""{"start":3000,"min":5}"""));

        Assert.Null(response.Results[0].Error);
        var series = Assert.Single(response.Results[0].Series!);
        // time >= 3000 -> ts 3000..10000; value > 5 -> values 6..9 -> ts 7000..10000
        Assert.Equal(4, series.Values.Count);
    }

    [Fact]
    public async Task ExecuteAsync_UnboundPlaceholder_ReturnsMissingParamError()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1_000_000);
        await engine.WriteAsync("db", "autogen",
        [
            new Point { Measurement = "cpu", Tags = new() { ["host"] = "a" }, Fields = new() { ["value"] = FieldValue.FromDouble(1) }, TimestampNs = 1_000 },
        ]);

        var response = await new QueryExecutor().ExecuteAsync(engine, "db", "SELECT value FROM cpu WHERE host = $host");

        Assert.Contains("missing parameter", response.Results[0].Error);
    }

    [Fact]
    public async Task ExecuteAsync_InjectionAttemptParam_ReturnsNoRows()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1_000_000);
        await engine.WriteAsync("db", "autogen",
        [
            new Point { Measurement = "cpu", Tags = new() { ["host"] = "a" }, Fields = new() { ["value"] = FieldValue.FromDouble(1) }, TimestampNs = 1_000 },
        ]);

        var response = await new QueryExecutor().ExecuteAsync(
            engine, "db", "SELECT value FROM cpu WHERE host = $host", default, Params("""{"host":"a' OR '1'='1"}"""));

        Assert.Null(response.Results[0].Error);
        // the injected value binds as one literal tag value that matches no series:
        // no rows may be returned and no error may surface
        Assert.Equal(0, response.Results[0].Series?.Sum(s => s.Values.Count) ?? 0);
    }

    [Fact]
    public async Task ExecuteAsync_ParamsIgnored_WhenQueryHasNoPlaceholders()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1_000_000);
        await engine.WriteAsync("db", "autogen",
        [
            new Point { Measurement = "cpu", Tags = new() { ["host"] = "a" }, Fields = new() { ["value"] = FieldValue.FromDouble(1) }, TimestampNs = 1_000 },
        ]);

        var response = await new QueryExecutor().ExecuteAsync(
            engine, "db", "SELECT value FROM cpu WHERE host = 'a'", default, Params("""{"unused":1}"""));

        Assert.Null(response.Results[0].Error);
        Assert.Single(response.Results[0].Series!);
    }

    [Fact]
    public async Task ExecuteAsync_RepeatedTemplateWithDifferentParams_UsesCachedParse()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1_000_000);
        await engine.WriteAsync("db", "autogen",
        [
            new Point { Measurement = "cpu", Tags = new() { ["host"] = "a" }, Fields = new() { ["value"] = FieldValue.FromDouble(1) }, TimestampNs = 1_000 },
            new Point { Measurement = "cpu", Tags = new() { ["host"] = "b" }, Fields = new() { ["value"] = FieldValue.FromDouble(2) }, TimestampNs = 2_000 },
        ]);

        var executor = new QueryExecutor();
        const string template = "SELECT value FROM cpu WHERE host = $host";

        var first = await executor.ExecuteAsync(engine, "db", template, default, Params("""{"host":"a"}"""));
        var second = await executor.ExecuteAsync(engine, "db", template, default, Params("""{"host":"b"}"""));

        Assert.Null(first.Results[0].Error);
        Assert.Null(second.Results[0].Error);
        Assert.Single(first.Results[0].Series!.SelectMany(s => s.Values));
        Assert.Single(second.Results[0].Series!.SelectMany(s => s.Values));
        // same template text hits the parse cache for both executions
        Assert.Same(QueryParamBinder.ParseCached(template), QueryParamBinder.ParseCached(template));
    }

    [Fact]
    public async Task ExecuteWithReport_ChunkedPath_BindsParams()
    {
        using var engine = new TsdbEngine(_testDir, flushThreshold: 1_000_000);
        await engine.WriteAsync("db", "autogen",
        [
            new Point { Measurement = "cpu", Tags = new() { ["host"] = "a" }, Fields = new() { ["value"] = FieldValue.FromDouble(1) }, TimestampNs = 1_000 },
            new Point { Measurement = "cpu", Tags = new() { ["host"] = "a" }, Fields = new() { ["value"] = FieldValue.FromDouble(2) }, TimestampNs = 2_000 },
            new Point { Measurement = "cpu", Tags = new() { ["host"] = "b" }, Fields = new() { ["value"] = FieldValue.FromDouble(3) }, TimestampNs = 3_000 },
        ]);

        var outcome = new QueryExecutor().ExecuteChunkedWithReport(
            engine, "db", "SELECT value FROM cpu WHERE host = $host", 2, default, Params("""{"host":"a"}"""));

        var responses = outcome.Responses.ToList();
        Assert.True(responses.Count >= 1);
        Assert.Null(outcome.Report.Error);
        var totalRows = responses.SelectMany(r => r.Results).Where(r => r.Series != null).SelectMany(r => r.Series!).Sum(s => s.Values.Count);
        Assert.Equal(2, totalRows);
    }

    [Fact]
    public void TryParseParamsJson_ValidAndInvalidInputs()
    {
        Assert.True(QueryParamBinder.TryParseParamsJson("""{"host":"a"}""", out var map));
        Assert.Equal("a", map["host"].GetString());

        Assert.False(QueryParamBinder.TryParseParamsJson("", out _));
        Assert.False(QueryParamBinder.TryParseParamsJson("not json", out _));
        Assert.False(QueryParamBinder.TryParseParamsJson(null, out _));
    }
}
