using System.Buffers.Binary;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// CRC32C (Castagnoli) implementation using slicing-by-8 lookup tables.
/// AOT-safe, no hardware intrinsics required; ~8x fewer table lookups than the
/// single-byte loop, which matters when every segment write/verify walks the whole file.
/// </summary>
public static class Crc32
{
    private const uint Polynomial = 0x82F63B78u; // CRC32C (Castagnoli) reversed polynomial
    internal static readonly uint[][] Tables = BuildTables();

    private static uint[][] BuildTables()
    {
        var tables = new uint[8][];
        var t0 = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint crc = i;
            for (int j = 0; j < 8; j++)
                crc = (crc & 1) != 0 ? (crc >> 1) ^ Polynomial : crc >> 1;
            t0[i] = crc;
        }
        tables[0] = t0;
        for (var t = 1; t < 8; t++)
        {
            var prev = tables[t - 1];
            var cur = new uint[256];
            for (uint i = 0; i < 256; i++)
                cur[i] = (prev[i] >> 8) ^ t0[prev[i] & 0xFF];
            tables[t] = cur;
        }
        return tables;
    }

    public static uint Compute(ReadOnlySpan<byte> data)
    {
        var crc = IncrementalCrc32.Create();
        crc.Append(data);
        return crc.GetResult();
    }

    public static uint Compute(byte[] data) => Compute(data.AsSpan());

    public static uint Compute(byte[] data, int offset, int length) => Compute(data.AsSpan(offset, length));
}

/// <summary>
/// Streaming CRC32C state sharing the slicing-by-8 tables with the one-shot Compute; lets a large
/// file be verified chunk by chunk without materializing it.
/// </summary>
public struct IncrementalCrc32
{
    private uint _crc;

    public static IncrementalCrc32 Create() => new() { _crc = 0xFFFFFFFFu };

    public void Append(ReadOnlySpan<byte> data)
    {
        var crc = _crc;
        var t = Crc32.Tables;
        var i = 0;
        var len = data.Length;
        for (; i + 8 <= len; i += 8)
        {
            var lo = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(i, 4)) ^ crc;
            var hi = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(i + 4, 4));
            crc = t[7][lo & 0xFF] ^ t[6][(lo >> 8) & 0xFF] ^ t[5][(lo >> 16) & 0xFF] ^ t[4][lo >> 24]
                ^ t[3][hi & 0xFF] ^ t[2][(hi >> 8) & 0xFF] ^ t[1][(hi >> 16) & 0xFF] ^ t[0][hi >> 24];
        }
        for (; i < len; i++)
            crc = t[0][(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
        _crc = crc;
    }

    public readonly uint GetResult() => _crc ^ 0xFFFFFFFFu;
}
