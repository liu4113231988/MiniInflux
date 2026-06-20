using System.Text.Json.Serialization;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

[JsonSerializable(typeof(QueryResponse))]
[JsonSerializable(typeof(QueryResult))]
[JsonSerializable(typeof(QuerySeries))]
[JsonSerializable(typeof(ErrorResponse))]
[JsonSerializable(typeof(AdminMessage))]
[JsonSerializable(typeof(BenchmarkSnapshot))]
[JsonSerializable(typeof(BenchmarkRunResult))]
[JsonSerializable(typeof(BackupMetadata))]
[JsonSerializable(typeof(BackupFileEntry))]
[JsonSerializable(typeof(RetentionPolicyInfo))]
[JsonSerializable(typeof(DebugStats))]
[JsonSerializable(typeof(Dictionary<string, int>))]
[JsonSerializable(typeof(Dictionary<string, long>))]
internal partial class AppJsonContext : JsonSerializerContext { }
