using MiniInflux.Net10.Protocol;

namespace MiniInflux.Tests;

public class InfluxQlParserTests
{
    [Fact]
    public void Parse_CreateDatabase_ReturnsCreateDatabaseQuery()
    {
        var query = InfluxQlParser.Parse("CREATE DATABASE metrics");

        Assert.Equal(QueryKind.CreateDatabase, query.Kind);
        Assert.Equal("metrics", query.Database);
    }

    [Fact]
    public void Parse_ShowDatabases_ReturnsShowDatabasesQuery()
    {
        var query = InfluxQlParser.Parse("SHOW DATABASES");

        Assert.Equal(QueryKind.ShowDatabases, query.Kind);
    }

    [Fact]
    public void Parse_ShowMeasurements_ReturnsShowMeasurementsQuery()
    {
        var query = InfluxQlParser.Parse("SHOW MEASUREMENTS");

        Assert.Equal(QueryKind.ShowMeasurements, query.Kind);
    }

    [Fact]
    public void Parse_SelectWithTimeFilter_ParsesTimeRange()
    {
        var query = InfluxQlParser.Parse("SELECT value FROM cpu WHERE time >= 1000 AND time <= 2000");

        Assert.Equal(QueryKind.Select, query.Kind);
        Assert.Equal("cpu", query.Measurement);
        Assert.Equal(1000, query.MinTimeNs);
        Assert.Equal(2000, query.MaxTimeNs);
    }

    [Fact]
    public void Parse_SelectWithTagFilter_ParsesTagFilter()
    {
        var query = InfluxQlParser.Parse("SELECT value FROM cpu WHERE host = 'server01'");

        Assert.Equal(QueryKind.Select, query.Kind);
        Assert.Single(query.TagFilters);
        Assert.Equal("host", query.TagFilters[0].Key);
        Assert.Equal("server01", query.TagFilters[0].Value);
        Assert.Equal(TagOp.Eq, query.TagFilters[0].Op);
    }

    [Fact]
    public void Parse_SelectWithTagNotEqual_ParsesTagFilter()
    {
        var query = InfluxQlParser.Parse("SELECT value FROM cpu WHERE host != 'server01'");

        Assert.Single(query.TagFilters);
        Assert.Equal(TagOp.Neq, query.TagFilters[0].Op);
    }

    [Fact]
    public void Parse_SelectWithTagRegex_ParsesRegexFilter()
    {
        var query = InfluxQlParser.Parse("SELECT value FROM cpu WHERE host =~ /server.*/");

        Assert.Single(query.TagFilters);
        Assert.Equal(TagOp.Regex, query.TagFilters[0].Op);
        Assert.Equal("server.*", query.TagFilters[0].Value);
    }

    [Fact]
    public void Parse_SelectWithFieldFilter_ParsesFieldFilter()
    {
        var query = InfluxQlParser.Parse("SELECT value FROM cpu WHERE value > 80");

        Assert.Single(query.FieldFilters);
        Assert.Equal("value", query.FieldFilters[0].Field);
        Assert.Equal(80, query.FieldFilters[0].Value);
        Assert.Equal(FieldOp.Gt, query.FieldFilters[0].Op);
    }

    [Fact]
    public void Parse_SelectWithGroupByTime_ParsesGroupBy()
    {
        var query = InfluxQlParser.Parse("SELECT mean(value) FROM cpu GROUP BY time(1m)");

        Assert.Equal(QueryKind.Select, query.Kind);
        Assert.Equal(60_000_000_000, query.GroupByNs);
        Assert.Single(query.Select);
        Assert.Equal("mean", query.Select[0].Func);
        Assert.Equal("value", query.Select[0].Field);
    }

    [Fact]
    public void Parse_SelectWithLimit_ParsesLimit()
    {
        var query = InfluxQlParser.Parse("SELECT value FROM cpu LIMIT 10");

        Assert.Equal(10, query.Limit);
    }

    [Fact]
    public void Parse_SelectStar_ParsesWildcard()
    {
        var query = InfluxQlParser.Parse("SELECT * FROM cpu");

        Assert.Single(query.Select);
        Assert.Equal("*", query.Select[0].Field);
    }

    [Fact]
    public void Parse_SelectWithAggregation_ParsesAggregation()
    {
        var query = InfluxQlParser.Parse("SELECT mean(value), max(temp) FROM cpu GROUP BY time(1m)");

        Assert.Equal(2, query.Select.Count);
        Assert.Equal("mean", query.Select[0].Func);
        Assert.Equal("max", query.Select[1].Func);
    }
}
