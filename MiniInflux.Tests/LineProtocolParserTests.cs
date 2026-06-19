using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;

namespace MiniInflux.Tests;

public class LineProtocolParserTests
{
    [Fact]
    public void ParseOne_SimpleLine_ReturnsPoint()
    {
        var line = "cpu,host=server01 value=1.5 1000000000";
        var point = LineProtocolParser.ParseOne(line, TimestampPrecision.Parse("ns"));

        Assert.Equal("cpu", point.Measurement);
        Assert.Single(point.Tags);
        Assert.Equal("server01", point.Tags["host"]);
        Assert.Single(point.Fields);
        Assert.Equal(FieldKind.Float, point.Fields["value"].Kind);
        Assert.Equal(1.5, point.Fields["value"].Float);
        Assert.Equal(1000000000, point.TimestampNs);
    }

    [Fact]
    public void ParseOne_IntegerField_ReturnsIntegerPoint()
    {
        var line = "mem,host=server01 used=1024i 1000000000";
        var point = LineProtocolParser.ParseOne(line, TimestampPrecision.Parse("ns"));

        Assert.Equal(FieldKind.Integer, point.Fields["used"].Kind);
        Assert.Equal(1024, point.Fields["used"].Integer);
    }

    [Fact]
    public void ParseOne_BooleanField_ReturnsBooleanPoint()
    {
        var line = "cpu,host=server01 active=true 1000000000";
        var point = LineProtocolParser.ParseOne(line, TimestampPrecision.Parse("ns"));

        Assert.Equal(FieldKind.Boolean, point.Fields["active"].Kind);
        Assert.True(point.Fields["active"].Boolean);
    }

    [Fact]
    public void ParseOne_StringField_ReturnsStringPoint()
    {
        var line = "cpu,host=server01 status=\"running\" 1000000000";
        var point = LineProtocolParser.ParseOne(line, TimestampPrecision.Parse("ns"));

        Assert.Equal(FieldKind.String, point.Fields["status"].Kind);
        Assert.Equal("running", point.Fields["status"].String);
    }

    [Fact]
    public void ParseMany_MultipleLines_ReturnsMultiplePoints()
    {
        var text = "cpu,host=server01 value=1.5 1000000000\ncpu,host=server02 value=2.5 2000000000";
        var points = LineProtocolParser.ParseMany(text, TimestampPrecision.Parse("ns"));

        Assert.Equal(2, points.Count);
        Assert.Equal("server01", points[0].Tags["host"]);
        Assert.Equal("server02", points[1].Tags["host"]);
    }

    [Fact]
    public void ParseOne_MillisecondPrecision_ConvertsToNanoseconds()
    {
        var line = "cpu value=1.5 1000";
        var point = LineProtocolParser.ParseOne(line, TimestampPrecision.Parse("ms"));

        Assert.Equal(1000_000_000, point.TimestampNs);
    }

    [Fact]
    public void ParseOne_MultipleTags_ReturnsAllTags()
    {
        var line = "cpu,host=server01,region=us-west value=1.5 1000000000";
        var point = LineProtocolParser.ParseOne(line, TimestampPrecision.Parse("ns"));

        Assert.Equal(2, point.Tags.Count);
        Assert.Equal("server01", point.Tags["host"]);
        Assert.Equal("us-west", point.Tags["region"]);
    }

    [Fact]
    public void ParseOne_MultipleFields_ReturnsAllFields()
    {
        var line = "cpu,host=server01 value=1.5,temp=42i 1000000000";
        var point = LineProtocolParser.ParseOne(line, TimestampPrecision.Parse("ns"));

        Assert.Equal(2, point.Fields.Count);
        Assert.Equal(1.5, point.Fields["value"].Float);
        Assert.Equal(42, point.Fields["temp"].Integer);
    }
}
