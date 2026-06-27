public sealed class CliInspectSegmentResult
{
    public int SchemaVersion { get; set; } = 1;
    public string Command { get; set; } = "inspect-segment";
    public string Path { get; set; } = "";
    public long FileBytes { get; set; }
    public int Columns { get; set; }
    public List<string> Measurements { get; set; } = [];
    public long MinTimeNs { get; set; }
    public long MaxTimeNs { get; set; }
    public int TotalPoints { get; set; }
    public List<CliSegmentColumnSummary> ColumnEntries { get; set; } = [];
}

public sealed class CliSegmentColumnSummary
{
    public string Measurement { get; set; } = "";
    public string Tags { get; set; } = "";
    public string Field { get; set; } = "";
    public string Kind { get; set; } = "";
    public int Points { get; set; }
    public long MinTimeNs { get; set; }
    public long MaxTimeNs { get; set; }
    public CliNumericStatsSummary? Stats { get; set; }
}

public sealed class CliNumericStatsSummary
{
    public double Min { get; set; }
    public double Max { get; set; }
    public double Sum { get; set; }
    public int Count { get; set; }
}

public sealed class CliInspectWalResult
{
    public int SchemaVersion { get; set; } = 1;
    public string Command { get; set; } = "inspect-wal";
    public string Path { get; set; } = "";
    public string Checkpoint { get; set; } = "";
    public string CurrentPosition { get; set; } = "";
    public int WalFiles { get; set; }
    public int ReplayRecords { get; set; }
    public List<CliWalFileSummary> Files { get; set; } = [];
}

public sealed class CliWalFileSummary
{
    public string Name { get; set; } = "";
    public long Bytes { get; set; }
}

public sealed class CliInspectManifestResult
{
    public int SchemaVersion { get; set; } = 1;
    public string Command { get; set; } = "inspect-manifest";
    public string Path { get; set; } = "";
    public int Databases { get; set; }
    public int RetentionPolicies { get; set; }
    public int ShardGroups { get; set; }
    public int SegmentFiles { get; set; }
    public int Measurements { get; set; }
    public int Series { get; set; }
    public int ContinuousQueries { get; set; }
    public List<CliManifestDatabaseSummary> DatabaseEntries { get; set; } = [];
}

public sealed class CliManifestDatabaseSummary
{
    public string Name { get; set; } = "";
    public int RetentionPolicies { get; set; }
    public int ShardGroups { get; set; }
    public int SegmentFiles { get; set; }
    public int Measurements { get; set; }
    public int Series { get; set; }
    public int ContinuousQueries { get; set; }
    public List<CliManifestRetentionPolicySummary> RetentionPolicyEntries { get; set; } = [];
    public List<CliManifestMeasurementSummary> MeasurementEntries { get; set; } = [];
    public List<CliManifestContinuousQuerySummary> ContinuousQueryEntries { get; set; } = [];
}

public sealed class CliManifestRetentionPolicySummary
{
    public string Database { get; set; } = "";
    public string Name { get; set; } = "";
    public long DurationNs { get; set; }
    public int Replication { get; set; }
    public bool IsDefault { get; set; }
    public int ShardGroups { get; set; }
    public List<CliManifestShardSummary> ShardEntries { get; set; } = [];
}

public sealed class CliManifestShardSummary
{
    public string Database { get; set; } = "";
    public string RetentionPolicy { get; set; } = "";
    public int Id { get; set; }
    public long StartTimeNs { get; set; }
    public long EndTimeNs { get; set; }
    public int Segments { get; set; }
    public List<string> Files { get; set; } = [];
}

public sealed class CliManifestMeasurementSummary
{
    public string Database { get; set; } = "";
    public string Name { get; set; } = "";
    public int Series { get; set; }
    public List<string> TagKeys { get; set; } = [];
}

public sealed class CliManifestContinuousQuerySummary
{
    public string Database { get; set; } = "";
    public string Name { get; set; } = "";
    public long EveryNs { get; set; }
    public long ForNs { get; set; }
    public int RecomputeRecentBuckets { get; set; }
    public long? LastCompletedBucketStartNs { get; set; }
}

public sealed class CliInspectSchemaResult
{
    public int SchemaVersion { get; set; } = 1;
    public string Command { get; set; } = "inspect-schema";
    public string Path { get; set; } = "";
    public int Entries { get; set; }
    public int Databases { get; set; }
    public int Measurements { get; set; }
    public List<CliSchemaMeasurementSummary> MeasurementEntries { get; set; } = [];
    public List<CliSchemaFieldSummary> FieldEntries { get; set; } = [];
}

public sealed class CliSchemaMeasurementSummary
{
    public string Database { get; set; } = "";
    public string Name { get; set; } = "";
    public int Fields { get; set; }
}

public sealed class CliSchemaFieldSummary
{
    public string Database { get; set; } = "";
    public string Measurement { get; set; } = "";
    public string Name { get; set; } = "";
    public string Kind { get; set; } = "";
}

public sealed class CliInspectTombstoneResult
{
    public int SchemaVersion { get; set; } = 1;
    public string Command { get; set; } = "inspect-tombstone";
    public string Path { get; set; } = "";
    public int TombstoneFiles { get; set; }
    public int Tombstones { get; set; }
    public int Databases { get; set; }
    public List<CliTombstoneDatabaseSummary> DatabaseEntries { get; set; } = [];
    public List<CliTombstoneEntry> TombstoneEntries { get; set; } = [];
}

public sealed class CliTombstoneDatabaseSummary
{
    public string Name { get; set; } = "";
    public int Tombstones { get; set; }
}

public sealed class CliTombstoneEntry
{
    public string Database { get; set; } = "";
    public string Measurement { get; set; } = "";
    public string Tags { get; set; } = "";
    public long? MinTimeNs { get; set; }
    public long? MaxTimeNs { get; set; }
    public long CreatedAtNs { get; set; }
}

public sealed class CliValidateDataDirResult
{
    public int SchemaVersion { get; set; } = 1;
    public string Command { get; set; } = "validate-data-dir";
    public string DataPath { get; set; } = "";
    public bool ManifestPresent { get; set; }
    public bool SchemaPresent { get; set; }
    public int TombstoneFiles { get; set; }
    public int WalFiles { get; set; }
    public int SegmentsOnDisk { get; set; }
    public int ManifestDatabases { get; set; }
    public int ManifestRetentionPolicies { get; set; }
    public int ManifestShardGroups { get; set; }
    public int ManifestSegmentFiles { get; set; }
    public int Issues { get; set; }
    public int Warnings { get; set; }
    public List<CliValidationMessage> IssueEntries { get; set; } = [];
    public List<CliValidationMessage> WarningEntries { get; set; } = [];
}

public sealed class CliValidationMessage
{
    public string Code { get; set; } = "";
    public string Message { get; set; } = "";
}

public sealed class CliRepairResult
{
    public int SchemaVersion { get; set; } = 1;
    public string Command { get; set; } = "repair";
    public string DataPath { get; set; } = "";
    public bool DryRun { get; set; }
    public bool ChangesApplied { get; set; }
    public int WalRecordsReplayed { get; set; }
    public int SegmentsScanned { get; set; }
    public int SegmentsCorrupted { get; set; }
    public int SchemaConflictsSkipped { get; set; }
}

public sealed class CliCompactResult
{
    public int SchemaVersion { get; set; } = 1;
    public string Command { get; set; } = "compact";
    public string DataPath { get; set; } = "";
    public bool DryRun { get; set; }
    public bool ChangesApplied { get; set; }
    public int CompactionTasksMerged { get; set; }
    public long CompactionRunsTotal { get; set; }
    public long SegmentsMergedTotal { get; set; }
}

public sealed class CliBackupCreateResult
{
    public int SchemaVersion { get; set; } = 1;
    public string Command { get; set; } = "backup";
    public string BackupCreated { get; set; } = "";
}

public sealed class CliBackupVerifyResult
{
    public int SchemaVersion { get; set; } = 1;
    public string Command { get; set; } = "backup-verify";
    public string BackupPath { get; set; } = "";
    public int FormatVersion { get; set; }
    public DateTimeOffset CreatedAtUtc { get; set; }
    public string SourceRoot { get; set; } = "";
    public int Files { get; set; }
    public bool Verified { get; set; }
}

public sealed class CliRestoreResult
{
    public int SchemaVersion { get; set; } = 1;
    public string Command { get; set; } = "restore";
    public string BackupPath { get; set; } = "";
    public string RestoreTarget { get; set; } = "";
    public string PendingRestorePath { get; set; } = "";
    public bool DryRun { get; set; }
    public bool ValidatedBackup { get; set; }
    public bool ChangesApplied { get; set; }
    public bool RestartRequired { get; set; }
}
