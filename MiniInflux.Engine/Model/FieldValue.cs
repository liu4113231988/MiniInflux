namespace MiniInflux.Net10.Model;

public enum FieldKind : byte { Float = 1, Integer = 2, Boolean = 3, String = 4 }

public readonly record struct FieldValue(FieldKind Kind, double Float, long Integer, bool Boolean, string? String)
{
    public static FieldValue FromDouble(double v) => new(FieldKind.Float, v, 0, false, null);
    public static FieldValue FromInteger(long v) => new(FieldKind.Integer, 0, v, false, null);
    public static FieldValue FromBoolean(bool v) => new(FieldKind.Boolean, 0, 0, v, null);
    public static FieldValue FromString(string v) => new(FieldKind.String, 0, 0, false, v);
    public object? ToObject() => Kind switch { FieldKind.Float => Float, FieldKind.Integer => Integer, FieldKind.Boolean => Boolean, FieldKind.String => String, _ => null };
    public double? AsDouble() => Kind switch { FieldKind.Float => Float, FieldKind.Integer => Integer, FieldKind.Boolean => Boolean ? 1 : 0, _ => null };
}
