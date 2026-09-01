using System.Text.Json.Serialization;

[JsonSerializable(typeof(CliInspectSegmentResult))]
[JsonSerializable(typeof(CliInspectWalResult))]
[JsonSerializable(typeof(CliInspectManifestResult))]
[JsonSerializable(typeof(CliInspectSchemaResult))]
[JsonSerializable(typeof(CliInspectTombstoneResult))]
[JsonSerializable(typeof(CliValidateDataDirResult))]
[JsonSerializable(typeof(CliRepairResult))]
[JsonSerializable(typeof(CliCompactResult))]
[JsonSerializable(typeof(CliBackupCreateResult))]
[JsonSerializable(typeof(CliBackupVerifyResult))]
[JsonSerializable(typeof(CliRestoreResult))]
internal partial class CliJsonContext : JsonSerializerContext { }
