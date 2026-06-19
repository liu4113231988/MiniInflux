using System.Text.Json.Serialization;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

[JsonSerializable(typeof(QueryResponse))]
[JsonSerializable(typeof(QueryResult))]
[JsonSerializable(typeof(QuerySeries))]
[JsonSerializable(typeof(ErrorResponse))]
[JsonSerializable(typeof(RetentionPolicyInfo))]
[JsonSerializable(typeof(DebugStats))]
[JsonSerializable(typeof(Dictionary<string, int>))]
internal partial class AppJsonContext : JsonSerializerContext { }
