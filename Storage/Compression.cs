using System.IO.Compression;
using System.Numerics;
using MiniInflux.Net10.Model;

namespace MiniInflux.Net10.Storage;

public enum TimestampCodecKind : byte
{
    DeltaOfDeltaVarint = 0,
    Gorilla = 1
}

public enum ValueCodecKind : byte
{
    Legacy = 0,
    Gorilla = 1
}

public enum BlockCompressionKind : byte
{
    None = 0,
    Brotli = 1
}

public readonly record struct TimestampEncodedBlock(
    TimestampCodecKind Codec,
    BlockCompressionKind Compression,
    byte[] Payload);

public readonly record struct ValueEncodedBlock(
    ValueCodecKind Codec,
    BlockCompressionKind Compression,
    byte[] Payload);

public static class CompressionCodec
{
    public static byte[] EncodeTimestamps(IReadOnlyList<long> timestamps) => EncodeTimestampsLegacy(timestamps);

    public static List<long> DecodeTimestamps(byte[] data) => DecodeTimestampsLegacy(data);

    public static byte[] EncodeTimestamps(TimestampCodecKind codec, IReadOnlyList<long> timestamps) => codec switch
    {
        TimestampCodecKind.DeltaOfDeltaVarint => EncodeTimestampsLegacy(timestamps),
        TimestampCodecKind.Gorilla => EncodeTimestampsGorilla(timestamps),
        _ => throw new InvalidDataException($"unsupported timestamp codec: {codec}")
    };

    public static List<long> DecodeTimestamps(TimestampCodecKind codec, byte[] data) => codec switch
    {
        TimestampCodecKind.DeltaOfDeltaVarint => DecodeTimestampsLegacy(data),
        TimestampCodecKind.Gorilla => DecodeTimestampsGorilla(data),
        _ => throw new InvalidDataException($"unsupported timestamp codec: {codec}")
    };

    public static byte[] EncodeValues(FieldKind kind, IReadOnlyList<FieldValue> values) => EncodeValuesLegacy(kind, values);

    public static List<FieldValue> DecodeValues(FieldKind kind, byte[] data) => DecodeValuesLegacy(kind, data);

    public static byte[] EncodeValues(FieldKind kind, ValueCodecKind codec, IReadOnlyList<FieldValue> values) => codec switch
    {
        ValueCodecKind.Legacy => EncodeValuesLegacy(kind, values),
        ValueCodecKind.Gorilla when kind == FieldKind.Float => EncodeFloatValuesGorilla(values),
        ValueCodecKind.Gorilla => throw new InvalidDataException($"gorilla value codec only supports float fields, got {kind}"),
        _ => throw new InvalidDataException($"unsupported value codec: {codec}")
    };

    public static List<FieldValue> DecodeValues(FieldKind kind, ValueCodecKind codec, byte[] data) => codec switch
    {
        ValueCodecKind.Legacy => DecodeValuesLegacy(kind, data),
        ValueCodecKind.Gorilla when kind == FieldKind.Float => DecodeFloatValuesGorilla(data),
        ValueCodecKind.Gorilla => throw new InvalidDataException($"gorilla value codec only supports float fields, got {kind}"),
        _ => throw new InvalidDataException($"unsupported value codec: {codec}")
    };

    public static TimestampEncodedBlock EncodeTimestampsAdaptive(IReadOnlyList<long> timestamps)
    {
        return ChooseBestTimestampBlock(
            (TimestampCodecKind.DeltaOfDeltaVarint, EncodeTimestampsLegacy(timestamps)),
            (TimestampCodecKind.Gorilla, EncodeTimestampsGorilla(timestamps)));
    }

    public static TimestampEncodedBlock EncodeTimestampsBlock(TimestampCodecKind codec, IReadOnlyList<long> timestamps)
    {
        var payload = EncodeTimestamps(codec, timestamps);
        var compressed = MaybeCompress(payload);
        return new TimestampEncodedBlock(codec, compressed.Compression, compressed.Payload);
    }

    public static List<long> DecodeTimestamps(TimestampCodecKind codec, BlockCompressionKind compression, byte[] payload)
    {
        var decoded = Decompress(compression, payload);
        return codec switch
        {
            TimestampCodecKind.DeltaOfDeltaVarint => DecodeTimestampsLegacy(decoded),
            TimestampCodecKind.Gorilla => DecodeTimestampsGorilla(decoded),
            _ => throw new InvalidDataException($"unsupported timestamp codec: {codec}")
        };
    }

    public static ValueEncodedBlock EncodeValuesAdaptive(FieldKind kind, IReadOnlyList<FieldValue> values)
    {
        if (kind != FieldKind.Float)
            return ChooseBestValueBlock((ValueCodecKind.Legacy, EncodeValuesLegacy(kind, values)));

        return ChooseAdaptiveFloatBlock(values);
    }

    public static ValueEncodedBlock EncodeValuesBlock(FieldKind kind, ValueCodecKind codec, IReadOnlyList<FieldValue> values)
    {
        var payload = EncodeValues(kind, codec, values);
        var compressed = MaybeCompress(payload);
        return new ValueEncodedBlock(codec, compressed.Compression, compressed.Payload);
    }

    public static ValueEncodedBlock EncodeValuesBlock(FieldKind kind, ValueCodecKind codec, BlockCompressionKind compression, IReadOnlyList<FieldValue> values)
    {
        var payload = EncodeValues(kind, codec, values);
        var finalPayload = compression switch
        {
            BlockCompressionKind.None => payload,
            BlockCompressionKind.Brotli => CompressBrotli(payload),
            _ => throw new InvalidDataException($"unsupported block compression: {compression}")
        };

        return new ValueEncodedBlock(codec, compression, finalPayload);
    }

    public static List<FieldValue> DecodeValues(FieldKind kind, ValueCodecKind codec, BlockCompressionKind compression, byte[] payload)
    {
        var decoded = Decompress(compression, payload);
        return codec switch
        {
            ValueCodecKind.Legacy => DecodeValuesLegacy(kind, decoded),
            ValueCodecKind.Gorilla when kind == FieldKind.Float => DecodeFloatValuesGorilla(decoded),
            ValueCodecKind.Gorilla => throw new InvalidDataException($"gorilla value codec only supports float fields, got {kind}"),
            _ => throw new InvalidDataException($"unsupported value codec: {codec}")
        };
    }

    private static TimestampEncodedBlock ChooseBestTimestampBlock(params (TimestampCodecKind Codec, byte[] Payload)[] candidates)
    {
        if (candidates.Length == 0)
            throw new InvalidOperationException("no timestamp codec candidates available");

        var bestBase = candidates
            .OrderBy(candidate => candidate.Payload.Length)
            .First();

        var compressed = MaybeCompress(bestBase.Payload);
        return new TimestampEncodedBlock(bestBase.Codec, compressed.Compression, compressed.Payload);
    }

    private static ValueEncodedBlock ChooseBestValueBlock(params (ValueCodecKind Codec, byte[] Payload)[] candidates)
    {
        if (candidates.Length == 0)
            throw new InvalidOperationException("no value codec candidates available");

        var bestBase = candidates
            .OrderBy(candidate => candidate.Payload.Length)
            .First();

        var compressed = MaybeCompress(bestBase.Payload);
        return new ValueEncodedBlock(bestBase.Codec, compressed.Compression, compressed.Payload);
    }

    private static ValueEncodedBlock ChooseAdaptiveFloatBlock(IReadOnlyList<FieldValue> values)
    {
        var candidates = new[]
        {
            EncodeValuesBlock(FieldKind.Float, ValueCodecKind.Legacy, BlockCompressionKind.None, values),
            EncodeValuesBlock(FieldKind.Float, ValueCodecKind.Legacy, BlockCompressionKind.Brotli, values),
            EncodeValuesBlock(FieldKind.Float, ValueCodecKind.Gorilla, BlockCompressionKind.None, values)
        };

        var minBytes = candidates.Min(candidate => candidate.Payload.Length);
        return candidates
            .OrderBy(candidate => ScoreFloatCandidate(candidate, minBytes))
            .ThenBy(candidate => candidate.Payload.Length)
            .First();
    }

    private static (BlockCompressionKind Compression, byte[] Payload) MaybeCompress(byte[] payload)
    {
        var compressed = CompressBrotli(payload);
        return compressed.Length < payload.Length
            ? (BlockCompressionKind.Brotli, compressed)
            : (BlockCompressionKind.None, payload);
    }

    private static double ScoreFloatCandidate(ValueEncodedBlock candidate, int minBytes)
    {
        var normalizedBytes = candidate.Payload.Length / (double)Math.Max(1, minBytes);
        var speedCost = candidate switch
        {
            { Codec: ValueCodecKind.Legacy, Compression: BlockCompressionKind.None } => 1.00,
            { Codec: ValueCodecKind.Gorilla, Compression: BlockCompressionKind.None } => 1.15,
            { Codec: ValueCodecKind.Legacy, Compression: BlockCompressionKind.Brotli } => 5.00,
            { Codec: ValueCodecKind.Gorilla, Compression: BlockCompressionKind.Brotli } => 6.00,
            _ => 8.00
        };

        return normalizedBytes * 2.5 + speedCost;
    }

    private static byte[] Decompress(BlockCompressionKind compression, byte[] payload) => compression switch
    {
        BlockCompressionKind.None => payload,
        BlockCompressionKind.Brotli => DecompressBrotli(payload),
        _ => throw new InvalidDataException($"unsupported block compression: {compression}")
    };

    private static byte[] EncodeTimestampsLegacy(IReadOnlyList<long> timestamps)
    {
        using var ms = new MemoryStream();
        Varint.WriteUInt64(ms, (ulong)timestamps.Count);
        if (timestamps.Count == 0)
            return ms.ToArray();

        Varint.WriteUInt64(ms, Varint.ZigZag(timestamps[0]));
        long previous = timestamps[0];
        long previousDelta = 0;
        for (int i = 1; i < timestamps.Count; i++)
        {
            var delta = timestamps[i] - previous;
            Varint.WriteUInt64(ms, Varint.ZigZag(delta - previousDelta));
            previous = timestamps[i];
            previousDelta = delta;
        }

        return ms.ToArray();
    }

    private static List<long> DecodeTimestampsLegacy(byte[] data)
    {
        using var ms = new MemoryStream(data);
        var count = checked((int)Varint.ReadUInt64(ms));
        var result = new List<long>(count);
        if (count == 0)
            return result;

        var first = Varint.UnZigZag(Varint.ReadUInt64(ms));
        result.Add(first);
        long previous = first;
        long previousDelta = 0;
        for (int i = 1; i < count; i++)
        {
            var deltaOfDelta = Varint.UnZigZag(Varint.ReadUInt64(ms));
            var delta = previousDelta + deltaOfDelta;
            var timestamp = previous + delta;
            result.Add(timestamp);
            previous = timestamp;
            previousDelta = delta;
        }

        return result;
    }

    private static byte[] EncodeTimestampsGorilla(IReadOnlyList<long> timestamps)
    {
        using var ms = new MemoryStream();
        Varint.WriteUInt64(ms, (ulong)timestamps.Count);
        if (timestamps.Count == 0)
            return ms.ToArray();

        using var writer = new BitWriter(ms);
        writer.WriteBits(unchecked((ulong)timestamps[0]), 64);
        if (timestamps.Count == 1)
            return ms.ToArray();

        long previous = timestamps[0];
        long previousDelta = timestamps[1] - timestamps[0];
        writer.WriteBits(unchecked((ulong)previousDelta), 64);
        previous = timestamps[1];

        for (int i = 2; i < timestamps.Count; i++)
        {
            var delta = timestamps[i] - previous;
            var deltaOfDelta = delta - previousDelta;
            WriteTimestampDeltaOfDelta(writer, deltaOfDelta);
            previous = timestamps[i];
            previousDelta = delta;
        }

        writer.Flush();
        return ms.ToArray();
    }

    private static List<long> DecodeTimestampsGorilla(byte[] data)
    {
        using var ms = new MemoryStream(data);
        var count = checked((int)Varint.ReadUInt64(ms));
        var result = new List<long>(count);
        if (count == 0)
            return result;

        using var reader = new BitReader(ms);
        var first = unchecked((long)reader.ReadBits(64));
        result.Add(first);
        if (count == 1)
            return result;

        long previous = first;
        long previousDelta = unchecked((long)reader.ReadBits(64));
        var second = previous + previousDelta;
        result.Add(second);
        previous = second;

        for (int i = 2; i < count; i++)
        {
            var deltaOfDelta = ReadTimestampDeltaOfDelta(reader);
            var delta = previousDelta + deltaOfDelta;
            var timestamp = previous + delta;
            result.Add(timestamp);
            previous = timestamp;
            previousDelta = delta;
        }

        return result;
    }

    private static void WriteTimestampDeltaOfDelta(BitWriter writer, long deltaOfDelta)
    {
        if (deltaOfDelta == 0)
        {
            writer.WriteBit(false);
            return;
        }

        if (deltaOfDelta >= -63 && deltaOfDelta <= 64)
        {
            writer.WriteBits(0b10, 2);
            writer.WriteBits((ulong)(deltaOfDelta + 63), 7);
            return;
        }

        if (deltaOfDelta >= -255 && deltaOfDelta <= 256)
        {
            writer.WriteBits(0b110, 3);
            writer.WriteBits((ulong)(deltaOfDelta + 255), 9);
            return;
        }

        if (deltaOfDelta >= -2047 && deltaOfDelta <= 2048)
        {
            writer.WriteBits(0b1110, 4);
            writer.WriteBits((ulong)(deltaOfDelta + 2047), 12);
            return;
        }

        writer.WriteBits(0b1111, 4);
        writer.WriteBits(unchecked((ulong)deltaOfDelta), 64);
    }

    private static long ReadTimestampDeltaOfDelta(BitReader reader)
    {
        if (!reader.ReadBit())
            return 0;

        if (!reader.ReadBit())
            return (long)reader.ReadBits(7) - 63;

        if (!reader.ReadBit())
            return (long)reader.ReadBits(9) - 255;

        if (!reader.ReadBit())
            return (long)reader.ReadBits(12) - 2047;

        return unchecked((long)reader.ReadBits(64));
    }

    private static byte[] EncodeValuesLegacy(FieldKind kind, IReadOnlyList<FieldValue> values) => kind switch
    {
        FieldKind.Float => EncodeFloatValuesLegacy(values),
        FieldKind.Integer => EncodeIntValuesLegacy(values),
        FieldKind.Boolean => EncodeBoolValuesLegacy(values),
        FieldKind.String => EncodeStringValuesLegacy(values),
        _ => []
    };

    private static List<FieldValue> DecodeValuesLegacy(FieldKind kind, byte[] data) => kind switch
    {
        FieldKind.Float => DecodeFloatValuesLegacy(data),
        FieldKind.Integer => DecodeIntValuesLegacy(data),
        FieldKind.Boolean => DecodeBoolValuesLegacy(data),
        FieldKind.String => DecodeStringValuesLegacy(data),
        _ => []
    };

    private static byte[] EncodeFloatValuesLegacy(IReadOnlyList<FieldValue> values)
    {
        using var ms = new MemoryStream();
        Varint.WriteUInt64(ms, (ulong)values.Count);
        ulong previous = 0;
        foreach (var value in values)
        {
            var bits = unchecked((ulong)BitConverter.DoubleToInt64Bits(value.Float));
            Varint.WriteUInt64(ms, bits ^ previous);
            previous = bits;
        }
        return ms.ToArray();
    }

    private static List<FieldValue> DecodeFloatValuesLegacy(byte[] data)
    {
        using var ms = new MemoryStream(data);
        var count = checked((int)Varint.ReadUInt64(ms));
        var result = new List<FieldValue>(count);
        ulong previous = 0;
        for (int i = 0; i < count; i++)
        {
            var bits = Varint.ReadUInt64(ms) ^ previous;
            result.Add(FieldValue.FromDouble(BitConverter.Int64BitsToDouble(unchecked((long)bits))));
            previous = bits;
        }
        return result;
    }

    private static byte[] EncodeFloatValuesGorilla(IReadOnlyList<FieldValue> values)
    {
        using var ms = new MemoryStream();
        Varint.WriteUInt64(ms, (ulong)values.Count);
        if (values.Count == 0)
            return ms.ToArray();

        using var writer = new BitWriter(ms);
        var previousBits = unchecked((ulong)BitConverter.DoubleToInt64Bits(values[0].Float));
        writer.WriteBits(previousBits, 64);

        var previousLeading = 0;
        var previousTrailing = 0;
        var hasWindow = false;

        for (int i = 1; i < values.Count; i++)
        {
            var currentBits = unchecked((ulong)BitConverter.DoubleToInt64Bits(values[i].Float));
            var xor = previousBits ^ currentBits;
            if (xor == 0)
            {
                writer.WriteBit(false);
                previousBits = currentBits;
                continue;
            }

            writer.WriteBit(true);
            var leading = BitOperations.LeadingZeroCount(xor);
            var trailing = BitOperations.TrailingZeroCount(xor);
            var significantBits = 64 - leading - trailing;

            if (hasWindow && leading >= previousLeading && trailing >= previousTrailing)
            {
                writer.WriteBit(false);
                var reusableBits = 64 - previousLeading - previousTrailing;
                writer.WriteBits(xor >> previousTrailing, reusableBits);
            }
            else
            {
                writer.WriteBit(true);
                writer.WriteBits((ulong)leading, 6);
                writer.WriteBits((ulong)significantBits, 7);
                writer.WriteBits(xor >> trailing, significantBits);
                previousLeading = leading;
                previousTrailing = trailing;
                hasWindow = true;
            }

            previousBits = currentBits;
        }

        writer.Flush();
        return ms.ToArray();
    }

    private static List<FieldValue> DecodeFloatValuesGorilla(byte[] data)
    {
        using var ms = new MemoryStream(data);
        var count = checked((int)Varint.ReadUInt64(ms));
        var result = new List<FieldValue>(count);
        if (count == 0)
            return result;

        using var reader = new BitReader(ms);
        var previousBits = reader.ReadBits(64);
        result.Add(FieldValue.FromDouble(BitConverter.Int64BitsToDouble(unchecked((long)previousBits))));

        var previousLeading = 0;
        var previousTrailing = 0;
        var hasWindow = false;

        for (int i = 1; i < count; i++)
        {
            ulong currentBits;
            if (!reader.ReadBit())
            {
                currentBits = previousBits;
            }
            else if (!reader.ReadBit())
            {
                if (!hasWindow)
                    throw new InvalidDataException("gorilla float stream reused a missing window");

                var reusableBits = 64 - previousLeading - previousTrailing;
                var significant = reader.ReadBits(reusableBits);
                currentBits = previousBits ^ (significant << previousTrailing);
            }
            else
            {
                var leading = checked((int)reader.ReadBits(6));
                var significantBits = checked((int)reader.ReadBits(7));
                if (significantBits <= 0 || significantBits > 64 || leading + significantBits > 64)
                    throw new InvalidDataException("gorilla float stream contains an invalid window");

                var trailing = 64 - leading - significantBits;
                var significant = reader.ReadBits(significantBits);
                currentBits = previousBits ^ (significant << trailing);
                previousLeading = leading;
                previousTrailing = trailing;
                hasWindow = true;
            }

            result.Add(FieldValue.FromDouble(BitConverter.Int64BitsToDouble(unchecked((long)currentBits))));
            previousBits = currentBits;
        }

        return result;
    }

    private static byte[] EncodeIntValuesLegacy(IReadOnlyList<FieldValue> values)
    {
        using var ms = new MemoryStream();
        Varint.WriteUInt64(ms, (ulong)values.Count);
        long previous = 0;
        foreach (var value in values)
        {
            Varint.WriteUInt64(ms, Varint.ZigZag(value.Integer - previous));
            previous = value.Integer;
        }
        return ms.ToArray();
    }

    private static List<FieldValue> DecodeIntValuesLegacy(byte[] data)
    {
        using var ms = new MemoryStream(data);
        var count = checked((int)Varint.ReadUInt64(ms));
        var result = new List<FieldValue>(count);
        long previous = 0;
        for (int i = 0; i < count; i++)
        {
            previous += Varint.UnZigZag(Varint.ReadUInt64(ms));
            result.Add(FieldValue.FromInteger(previous));
        }
        return result;
    }

    private static byte[] EncodeBoolValuesLegacy(IReadOnlyList<FieldValue> values)
    {
        using var ms = new MemoryStream();
        Varint.WriteUInt64(ms, (ulong)values.Count);
        byte current = 0;
        var bitIndex = 0;
        foreach (var value in values)
        {
            if (value.Boolean)
                current |= (byte)(1 << bitIndex);

            bitIndex++;
            if (bitIndex == 8)
            {
                ms.WriteByte(current);
                current = 0;
                bitIndex = 0;
            }
        }

        if (bitIndex > 0)
            ms.WriteByte(current);

        return ms.ToArray();
    }

    private static List<FieldValue> DecodeBoolValuesLegacy(byte[] data)
    {
        using var ms = new MemoryStream(data);
        var count = checked((int)Varint.ReadUInt64(ms));
        var payload = new byte[ms.Length - ms.Position];
        _ = ms.Read(payload);
        var result = new List<FieldValue>(count);
        for (int i = 0; i < count; i++)
            result.Add(FieldValue.FromBoolean(((payload[i / 8] >> (i % 8)) & 1) == 1));
        return result;
    }

    private static byte[] EncodeStringValuesLegacy(IReadOnlyList<FieldValue> values)
    {
        using var ms = new MemoryStream();
        Varint.WriteUInt64(ms, (ulong)values.Count);

        var dictionary = new Dictionary<string, int>(StringComparer.Ordinal);
        var entries = new List<string>();
        foreach (var value in values)
        {
            var text = value.String ?? string.Empty;
            if (dictionary.ContainsKey(text))
                continue;

            dictionary[text] = entries.Count;
            entries.Add(text);
        }

        Varint.WriteUInt64(ms, (ulong)entries.Count);
        foreach (var entry in entries)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(entry);
            Varint.WriteUInt64(ms, (ulong)bytes.Length);
            ms.Write(bytes);
        }

        foreach (var value in values)
            Varint.WriteUInt64(ms, (ulong)dictionary[value.String ?? string.Empty]);

        return ms.ToArray();
    }

    private static List<FieldValue> DecodeStringValuesLegacy(byte[] data)
    {
        using var ms = new MemoryStream(data);
        var count = checked((int)Varint.ReadUInt64(ms));
        var dictionaryCount = checked((int)Varint.ReadUInt64(ms));
        var dictionary = new string[dictionaryCount];
        for (int i = 0; i < dictionaryCount; i++)
        {
            var length = checked((int)Varint.ReadUInt64(ms));
            var bytes = new byte[length];
            _ = ms.Read(bytes);
            dictionary[i] = System.Text.Encoding.UTF8.GetString(bytes);
        }

        var result = new List<FieldValue>(count);
        for (int i = 0; i < count; i++)
            result.Add(FieldValue.FromString(dictionary[checked((int)Varint.ReadUInt64(ms))]));
        return result;
    }

    private static byte[] CompressBrotli(byte[] input)
    {
        using var ms = new MemoryStream();
        using (var brotli = new BrotliStream(ms, CompressionLevel.Fastest, leaveOpen: true))
            brotli.Write(input);
        return ms.ToArray();
    }

    private static byte[] DecompressBrotli(byte[] input)
    {
        using var source = new MemoryStream(input);
        using var brotli = new BrotliStream(source, CompressionMode.Decompress);
        using var output = new MemoryStream();
        brotli.CopyTo(output);
        return output.ToArray();
    }

    private sealed class BitWriter : IDisposable
    {
        private readonly Stream _stream;
        private byte _currentByte;
        private int _bitIndex;

        public BitWriter(Stream stream) => _stream = stream;

        public void WriteBit(bool value)
        {
            if (value)
                _currentByte |= (byte)(1 << (7 - _bitIndex));

            _bitIndex++;
            if (_bitIndex == 8)
                FlushCurrentByte();
        }

        public void WriteBits(ulong value, int bitCount)
        {
            for (int i = bitCount - 1; i >= 0; i--)
                WriteBit(((value >> i) & 1UL) != 0);
        }

        public void Flush()
        {
            if (_bitIndex > 0)
                FlushCurrentByte();
        }

        private void FlushCurrentByte()
        {
            _stream.WriteByte(_currentByte);
            _currentByte = 0;
            _bitIndex = 0;
        }

        public void Dispose() => Flush();
    }

    private sealed class BitReader : IDisposable
    {
        private readonly Stream _stream;
        private int _currentByte = -1;
        private int _bitIndex = 8;

        public BitReader(Stream stream) => _stream = stream;

        public bool ReadBit()
        {
            if (_bitIndex == 8)
            {
                _currentByte = _stream.ReadByte();
                if (_currentByte < 0)
                    throw new EndOfStreamException();
                _bitIndex = 0;
            }

            var value = ((_currentByte >> (7 - _bitIndex)) & 1) == 1;
            _bitIndex++;
            return value;
        }

        public ulong ReadBits(int bitCount)
        {
            ulong value = 0;
            for (int i = 0; i < bitCount; i++)
            {
                value <<= 1;
                if (ReadBit())
                    value |= 1;
            }
            return value;
        }

        public void Dispose()
        {
        }
    }

}
