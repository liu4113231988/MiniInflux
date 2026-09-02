using System.Text.Json.Serialization;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Net10;

[JsonSerializable(typeof(BenchmarkRunResult))]
[JsonSerializable(typeof(BenchmarkPhaseTimings))]
[JsonSerializable(typeof(CodecComparisonBenchmark))]
[JsonSerializable(typeof(CodecBenchmarkResult))]
[JsonSerializable(typeof(FloatWorkloadBenchmark))]
[JsonSerializable(typeof(FloatStrategyBenchmarkResult))]
[JsonSerializable(typeof(BackupMetadata))]
[JsonSerializable(typeof(BackupFileEntry))]
[JsonSerializable(typeof(Dictionary<string, System.Text.Json.JsonElement>))]
public partial class EngineJsonContext : JsonSerializerContext { }
