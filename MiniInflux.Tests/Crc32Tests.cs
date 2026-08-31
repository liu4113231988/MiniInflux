using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class Crc32Tests
{
    [Fact]
    public void Compute_EmptyArray_ReturnsZero()
    {
        var result = Crc32.Compute(Array.Empty<byte>());
        Assert.Equal(0u, result);
    }

    [Fact]
    public void Compute_SameInput_ReturnsSameOutput()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        var result1 = Crc32.Compute(data);
        var result2 = Crc32.Compute(data);

        Assert.Equal(result1, result2);
    }

    [Fact]
    public void Compute_DifferentInput_ReturnsDifferentOutput()
    {
        var data1 = new byte[] { 1, 2, 3, 4, 5 };
        var data2 = new byte[] { 5, 4, 3, 2, 1 };

        var result1 = Crc32.Compute(data1);
        var result2 = Crc32.Compute(data2);

        Assert.NotEqual(result1, result2);
    }

    [Fact]
    public void Compute_KnownValue_ReturnsExpectedCrc()
    {
        // "123456789" has a known CRC32C value
        var data = System.Text.Encoding.ASCII.GetBytes("123456789");
        var result = Crc32.Compute(data);
        Assert.Equal(0xE3069283u, result);
    }

    [Fact]
    public void Compute_LargeInput_MatchesSingleByteReference()
    {
        // Cross-check the slicing-by-8 implementation against a straightforward single-byte
        // table implementation on a large, non-8-aligned payload.
        var random = new Random(42);
        var data = new byte[1_000_003];
        random.NextBytes(data);
        Assert.Equal(ReferenceCrc32C(data), Crc32.Compute(data));
    }

    private static uint ReferenceCrc32C(byte[] data)
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            var crc = i;
            for (var j = 0; j < 8; j++)
                crc = (crc & 1) != 0 ? (crc >> 1) ^ 0x82F63B78u : crc >> 1;
            table[i] = crc;
        }

        uint value = 0xFFFFFFFFu;
        foreach (var b in data)
            value = table[(value ^ b) & 0xFF] ^ (value >> 8);
        return value ^ 0xFFFFFFFFu;
    }
}
