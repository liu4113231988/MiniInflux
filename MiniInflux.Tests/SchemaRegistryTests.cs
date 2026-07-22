using MiniInflux.Net10.Model;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Tests;

public class SchemaRegistryTests : IDisposable
{
    private readonly string _testDir;
    private readonly SchemaRegistry _registry;

    public SchemaRegistryTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"miniinflux_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
        _registry = new SchemaRegistry(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, true);
    }

    [Fact]
    public void ValidateAndRegister_NewField_RegistersSuccessfully()
    {
        var points = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string> { { "host", "server01" } },
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1.5) } },
                TimestampNs = 1000
            }
        };

        _registry.ValidateAndRegister("testdb", "cpu", points);

        var fieldType = _registry.GetFieldType("testdb", "cpu", "value");
        Assert.Equal(FieldKind.Float, fieldType);
    }

    [Fact]
    public void ValidateAndRegister_SameType_NoConflict()
    {
        var points1 = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string>(),
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1.5) } },
                TimestampNs = 1000
            }
        };
        var points2 = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string>(),
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(2.5) } },
                TimestampNs = 2000
            }
        };

        _registry.ValidateAndRegister("testdb", "cpu", points1);
        _registry.ValidateAndRegister("testdb", "cpu", points2);
    }

    [Fact]
    public void ValidateAndRegister_TypeConflict_ThrowsException()
    {
        var points1 = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string>(),
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1.5) } },
                TimestampNs = 1000
            }
        };
        var points2 = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string>(),
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromString("hello") } },
                TimestampNs = 2000
            }
        };

        _registry.ValidateAndRegister("testdb", "cpu", points1);

        Assert.Throws<FieldConflictException>(() =>
            _registry.ValidateAndRegister("testdb", "cpu", points2));
    }

    [Fact]
    public void ValidateAndRegister_PersistsToDisk()
    {
        var points = new List<Point>
        {
            new Point
            {
                Measurement = "cpu",
                Tags = new Dictionary<string, string>(),
                Fields = new Dictionary<string, FieldValue> { { "value", FieldValue.FromDouble(1.5) } },
                TimestampNs = 1000
            }
        };

        _registry.ValidateAndRegister("testdb", "cpu", points);
        _registry.SaveIfDirty();

        // Create a new registry pointing to the same directory
        var registry2 = new SchemaRegistry(_testDir);
        var fieldType = registry2.GetFieldType("testdb", "cpu", "value");
        Assert.Equal(FieldKind.Float, fieldType);
    }
}
