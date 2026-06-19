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
}
