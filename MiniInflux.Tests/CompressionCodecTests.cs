using MiniInflux.Net10.Model;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class CompressionCodecTests
{
    [Fact]
    public void Timestamps_Roundtrip_PreservesValues()
    {
        var timestamps = new List<long> { 1000, 2000, 3000, 4000, 5000 };
        var encoded = CompressionCodec.EncodeTimestamps(timestamps);
        var decoded = CompressionCodec.DecodeTimestamps(encoded);

        Assert.Equal(timestamps, decoded);
    }

    [Fact]
    public void Timestamps_IrregularIntervals_RoundtripPreservesValues()
    {
        var timestamps = new List<long> { 1000, 1500, 3000, 10000, 15000 };
        var encoded = CompressionCodec.EncodeTimestamps(timestamps);
        var decoded = CompressionCodec.DecodeTimestamps(encoded);

        Assert.Equal(timestamps, decoded);
    }

    [Fact]
    public void GorillaTimestamps_Roundtrip_PreservesValues()
    {
        var timestamps = new List<long> { 1_000, 2_000, 3_000, 4_000, 4_500, 5_000, 9_000, 9_500 };
        var encoded = CompressionCodec.EncodeTimestamps(TimestampCodecKind.Gorilla, timestamps);
        var decoded = CompressionCodec.DecodeTimestamps(TimestampCodecKind.Gorilla, encoded);

        Assert.Equal(timestamps, decoded);
    }

    [Fact]
    public void Doubles_Roundtrip_PreservesValues()
    {
        var values = new List<FieldValue>
        {
            FieldValue.FromDouble(1.5),
            FieldValue.FromDouble(2.5),
            FieldValue.FromDouble(3.5),
            FieldValue.FromDouble(1.5)
        };
        var encoded = CompressionCodec.EncodeValues(FieldKind.Float, values);
        var decoded = CompressionCodec.DecodeValues(FieldKind.Float, encoded);

        Assert.Equal(values.Count, decoded.Count);
        for (int i = 0; i < values.Count; i++)
            Assert.Equal(values[i].Float, decoded[i].Float, 10);
    }

    [Fact]
    public void GorillaDoubles_Roundtrip_PreservesValues()
    {
        var values = new List<FieldValue>
        {
            FieldValue.FromDouble(12.25),
            FieldValue.FromDouble(12.25),
            FieldValue.FromDouble(12.5),
            FieldValue.FromDouble(13.0),
            FieldValue.FromDouble(13.0),
            FieldValue.FromDouble(14.75)
        };

        var encoded = CompressionCodec.EncodeValues(FieldKind.Float, ValueCodecKind.Gorilla, values);
        var decoded = CompressionCodec.DecodeValues(FieldKind.Float, ValueCodecKind.Gorilla, encoded);

        Assert.Equal(values.Count, decoded.Count);
        for (int i = 0; i < values.Count; i++)
            Assert.Equal(values[i].Float, decoded[i].Float, 10);
    }

    [Fact]
    public void Integers_Roundtrip_PreservesValues()
    {
        var values = new List<FieldValue>
        {
            FieldValue.FromInteger(100),
            FieldValue.FromInteger(200),
            FieldValue.FromInteger(150),
            FieldValue.FromInteger(300)
        };
        var encoded = CompressionCodec.EncodeValues(FieldKind.Integer, values);
        var decoded = CompressionCodec.DecodeValues(FieldKind.Integer, encoded);

        Assert.Equal(values.Count, decoded.Count);
        for (int i = 0; i < values.Count; i++)
            Assert.Equal(values[i].Integer, decoded[i].Integer);
    }

    [Fact]
    public void Booleans_Roundtrip_PreservesValues()
    {
        var values = new List<FieldValue>
        {
            FieldValue.FromBoolean(true),
            FieldValue.FromBoolean(false),
            FieldValue.FromBoolean(true),
            FieldValue.FromBoolean(true),
            FieldValue.FromBoolean(false)
        };
        var encoded = CompressionCodec.EncodeValues(FieldKind.Boolean, values);
        var decoded = CompressionCodec.DecodeValues(FieldKind.Boolean, encoded);

        Assert.Equal(values.Count, decoded.Count);
        for (int i = 0; i < values.Count; i++)
            Assert.Equal(values[i].Boolean, decoded[i].Boolean);
    }

    [Fact]
    public void Strings_Roundtrip_PreservesValues()
    {
        var values = new List<FieldValue>
        {
            FieldValue.FromString("hello"),
            FieldValue.FromString("world"),
            FieldValue.FromString("hello"),
            FieldValue.FromString("test")
        };
        var encoded = CompressionCodec.EncodeValues(FieldKind.String, values);
        var decoded = CompressionCodec.DecodeValues(FieldKind.String, encoded);

        Assert.Equal(values.Count, decoded.Count);
        for (int i = 0; i < values.Count; i++)
            Assert.Equal(values[i].String, decoded[i].String);
    }

    [Fact]
    public void EmptyTimestamps_Roundtrip_ReturnsEmptyList()
    {
        var timestamps = new List<long>();
        var encoded = CompressionCodec.EncodeTimestamps(timestamps);
        var decoded = CompressionCodec.DecodeTimestamps(encoded);

        Assert.Empty(decoded);
    }

    [Fact]
    public void AdaptiveFloatEncoding_CanSelectGorilla()
    {
        var values = Enumerable.Range(0, 256)
            .Select(i => FieldValue.FromDouble(1000.0 + (i / 16) * 0.5))
            .ToList();

        var block = CompressionCodec.EncodeValuesAdaptive(FieldKind.Float, values);
        var decoded = CompressionCodec.DecodeValues(FieldKind.Float, block.Codec, block.Compression, block.Payload);

        Assert.Equal(values.Count, decoded.Count);
        Assert.Equal(ValueCodecKind.Gorilla, block.Codec);
        Assert.Equal(BlockCompressionKind.None, block.Compression);
    }

    [Fact]
    public void AdaptiveFloatEncoding_CanSelectLegacyRawForVolatileSeries()
    {
        var values = Enumerable.Range(0, 256)
            .Select(i => FieldValue.FromDouble(1000.0 + Math.Sin(i / 8.0) * 50.0 + ((i % 7) - 3) * 0.137))
            .ToList();

        var block = CompressionCodec.EncodeValuesAdaptive(FieldKind.Float, values);

        Assert.Equal(ValueCodecKind.Legacy, block.Codec);
        Assert.Equal(BlockCompressionKind.None, block.Compression);
    }

    [Fact]
    public void AdaptiveFloatEncoding_CanSelectLegacyBrotliForSmoothLinearSeries()
    {
        var values = Enumerable.Range(0, 256)
            .Select(i => FieldValue.FromDouble(1000.0 + i * 0.25))
            .ToList();

        var block = CompressionCodec.EncodeValuesAdaptive(FieldKind.Float, values);

        Assert.Equal(ValueCodecKind.Legacy, block.Codec);
        Assert.Equal(BlockCompressionKind.Brotli, block.Compression);
    }
}
