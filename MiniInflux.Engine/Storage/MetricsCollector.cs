using System.Text;
using Microsoft.Extensions.Logging;
using MiniInflux.Net10.Query;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Lightweight metrics collector for observability endpoints (/health, /debug/stats, /metrics).
/// Uses simple atomic counters to avoid external dependencies.
/// </summary>
public sealed class MetricsCollector
{
    private readonly object _continuousQueryMetricsLock = new();
    private long _writePointsTotal;
    private long _queryTotal;
    private long _queryErrorTotal;
    private long _queryTimeoutTotal;
    private long _queryRowsReturnedTotal;
    private long _queryScannedPointsTotal;
    private long _queryEstimatedInputBytesTotal;
    private long _queryEstimatedResultBytesTotal;
    private long _lastQueryEstimatedPeakBytes;
    private long _continuousQueryRunsTotal;
    private long _continuousQueryErrorsTotal;
    private long _continuousQueryBucketsTotal;
    private long _continuousQueryRecomputeBucketsTotal;
    private long _lastContinuousQueryDurationMs;
    private long _compactionCount;
    private int _compactionRunning;
    private readonly long[] _queryDurationBuckets = new long[8];
    private static readonly int[] QueryDurationBoundsMs = [1, 5, 10, 50, 100, 500, 1000, 5000];
    private readonly TsdbEngine _engine;
    private readonly WriteQueue? _writeQueue;
    private readonly ILogger? _logger;
    private volatile bool _segmentEnumerationErrorLogged;
    private readonly Dictionary<string, ContinuousQueryMetrics> _continuousQueryMetrics = new(StringComparer.Ordinal);

    public MetricsCollector(TsdbEngine engine, WriteQueue? writeQueue = null, ILogger? logger = null) { _engine = engine; _writeQueue = writeQueue; _logger = logger; }

    public void RecordWrite(int pointCount) => Interlocked.Add(ref _writePointsTotal, pointCount);
    public void RecordQuery() => Interlocked.Increment(ref _queryTotal);
    public void RecordQuery(QueryExecutionReport report)
    {
        Interlocked.Increment(ref _queryTotal);
        if (!string.IsNullOrWhiteSpace(report.Error)) Interlocked.Increment(ref _queryErrorTotal);
        if (report.TimedOut) Interlocked.Increment(ref _queryTimeoutTotal);
        Interlocked.Add(ref _queryRowsReturnedTotal, report.RowsReturned);
        Interlocked.Add(ref _queryScannedPointsTotal, report.ScannedPoints);
        Interlocked.Add(ref _queryEstimatedInputBytesTotal, report.EstimatedInputBytes);
        Interlocked.Add(ref _queryEstimatedResultBytesTotal, report.EstimatedResultBytes);
        Interlocked.Exchange(ref _lastQueryEstimatedPeakBytes, report.PeakEstimatedMemoryBytes);
        for (int i = 0; i < QueryDurationBoundsMs.Length; i++)
        {
            if (report.DurationMs <= QueryDurationBoundsMs[i])
            {
                Interlocked.Increment(ref _queryDurationBuckets[i]);
                break;
            }
        }
    }
    public void SetCompactionRunning(bool v) => Interlocked.Exchange(ref _compactionRunning, v ? 1 : 0);
    public void RecordCompaction() => Interlocked.Increment(ref _compactionCount);
    public void RecordContinuousQueryRun(string database, string name, int bucketsProcessed, bool hadError, bool wasRecompute, long durationMs, long? lastBucketStartNs = null)
    {
        if (bucketsProcessed <= 0 && !hadError)
            return;

        Interlocked.Increment(ref _continuousQueryRunsTotal);
        if (hadError) Interlocked.Increment(ref _continuousQueryErrorsTotal);
        if (bucketsProcessed > 0) Interlocked.Add(ref _continuousQueryBucketsTotal, bucketsProcessed);
        if (wasRecompute && bucketsProcessed > 0) Interlocked.Add(ref _continuousQueryRecomputeBucketsTotal, bucketsProcessed);
        Interlocked.Exchange(ref _lastContinuousQueryDurationMs, durationMs);

        lock (_continuousQueryMetricsLock)
        {
            var key = $"{database}/{name}";
            if (!_continuousQueryMetrics.TryGetValue(key, out var metric))
            {
                metric = new ContinuousQueryMetrics { Database = database, Name = name };
                _continuousQueryMetrics[key] = metric;
            }

            metric.RunsTotal++;
            if (hadError) metric.ErrorsTotal++;
            metric.BucketsTotal += Math.Max(0, bucketsProcessed);
            if (wasRecompute) metric.RecomputeBucketsTotal += Math.Max(0, bucketsProcessed);
            metric.LastDurationMs = durationMs;
            if (lastBucketStartNs.HasValue) metric.LastBucketStartNs = lastBucketStartNs.Value;
        }
    }

    public long WritePointsTotal => Interlocked.Read(ref _writePointsTotal);
    public long QueryTotal => Interlocked.Read(ref _queryTotal);
    public long QueryErrorTotal => Interlocked.Read(ref _queryErrorTotal);
    public long QueryTimeoutTotal => Interlocked.Read(ref _queryTimeoutTotal);
    public long QueryRowsReturnedTotal => Interlocked.Read(ref _queryRowsReturnedTotal);
    public long QueryScannedPointsTotal => Interlocked.Read(ref _queryScannedPointsTotal);
    public long QueryEstimatedInputBytesTotal => Interlocked.Read(ref _queryEstimatedInputBytesTotal);
    public long QueryEstimatedResultBytesTotal => Interlocked.Read(ref _queryEstimatedResultBytesTotal);
    public long LastQueryEstimatedPeakBytes => Interlocked.Read(ref _lastQueryEstimatedPeakBytes);
    public long ContinuousQueryRunsTotal => Interlocked.Read(ref _continuousQueryRunsTotal);
    public long ContinuousQueryErrorsTotal => Interlocked.Read(ref _continuousQueryErrorsTotal);
    public long ContinuousQueryBucketsTotal => Interlocked.Read(ref _continuousQueryBucketsTotal);
    public long ContinuousQueryRecomputeBucketsTotal => Interlocked.Read(ref _continuousQueryRecomputeBucketsTotal);
    public long LastContinuousQueryDurationMs => Interlocked.Read(ref _lastContinuousQueryDurationMs);
    public bool CompactionRunning => Interlocked.CompareExchange(ref _compactionRunning, 0, 0) == 1;
    public long CompactionCount => Interlocked.Read(ref _compactionCount);

    /// <summary>
    /// Flat counters/gauges for the /debug/vars endpoint (expvar-style).
    /// Kept numeric so the payload stays AOT-friendly (Dictionary&lt;string, long&gt;).
    /// </summary>
    public Dictionary<string, long> CollectVars()
    {
        var stats = CollectStats();
        return new Dictionary<string, long>(StringComparer.Ordinal)
        {
            ["write_points_total"] = stats.WritePointsTotal,
            ["query_total"] = stats.QueryTotal,
            ["query_errors_total"] = stats.QueryErrorTotal,
            ["query_timeouts_total"] = stats.QueryTimeoutTotal,
            ["query_rows_returned_total"] = stats.QueryRowsReturnedTotal,
            ["query_scanned_points_total"] = stats.QueryScannedPointsTotal,
            ["query_estimated_input_bytes_total"] = stats.QueryEstimatedInputBytesTotal,
            ["query_estimated_result_bytes_total"] = stats.QueryEstimatedResultBytesTotal,
            ["last_query_estimated_peak_bytes"] = stats.LastQueryEstimatedPeakBytes,
            ["continuous_query_runs_total"] = stats.ContinuousQueryRunsTotal,
            ["continuous_query_errors_total"] = stats.ContinuousQueryErrorsTotal,
            ["continuous_query_buckets_total"] = stats.ContinuousQueryBucketsTotal,
            ["continuous_query_recompute_buckets_total"] = stats.ContinuousQueryRecomputeBucketsTotal,
            ["last_continuous_query_duration_ms"] = stats.LastContinuousQueryDurationMs,
            ["segment_files"] = stats.SegmentCount,
            ["compaction_running"] = stats.CompactionRunning ? 1 : 0,
            ["compaction_total"] = stats.CompactionCount,
            ["compaction_tasks_total"] = stats.CompactionTasksTotal,
            ["compaction_segments_merged_total"] = stats.CompactionSegmentsMergedTotal,
            ["compaction_queued_tasks"] = stats.CompactionQueuedTasks,
            ["compaction_backlog_tasks"] = stats.CompactionBacklogTasks,
            ["compaction_backlog_bytes"] = stats.CompactionBacklogBytes,
            ["compaction_input_bytes_total"] = stats.CompactionInputBytesTotal,
            ["compaction_output_bytes_total"] = stats.CompactionOutputBytesTotal,
            ["compaction_duration_ms_total"] = stats.CompactionDurationMsTotal,
            ["memory_buffer_points"] = stats.MemoryBufferPoints,
            ["memory_buffer_bytes"] = stats.MemoryBufferBytes,
            ["flush_snapshot_points"] = stats.FlushSnapshotPoints,
            ["flush_snapshot_bytes"] = stats.FlushSnapshotBytes,
            ["write_queue_pending_requests"] = stats.WriteQueuePendingRequests,
            ["write_queue_queued_points"] = stats.WriteQueueQueuedPoints,
            ["write_queue_queued_bytes"] = stats.WriteQueueQueuedBytes,
            ["quarantined_segments"] = stats.QuarantinedSegments,
            ["gc_collection_count_gen0"] = stats.Gc?.CollectionCountGen0 ?? 0,
            ["gc_collection_count_gen1"] = stats.Gc?.CollectionCountGen1 ?? 0,
            ["gc_collection_count_gen2"] = stats.Gc?.CollectionCountGen2 ?? 0,
            ["gc_heap_size_bytes"] = stats.Gc?.HeapSizeBytes ?? 0,
            ["gc_committed_bytes"] = stats.Gc?.TotalCommittedBytes ?? 0,
            ["gc_working_set_bytes"] = stats.Gc?.WorkingSetBytes ?? 0,
            ["gc_pause_duration_ms_total"] = (long)(stats.Gc?.PauseDurationMsTotal ?? 0)
        };
    }

    /// <summary>
    /// Collect all stats for /debug/stats endpoint.
    /// </summary>
    public DebugStats CollectStats()
    {
        var cardinality = new Dictionary<string, int>();
        foreach (var db in _engine.ListDatabases())
            cardinality[db] = _engine.GetSeriesCardinality(db);
        var compaction = _engine.GetCompactionStats();
        var durationBuckets = new Dictionary<string, long>(StringComparer.Ordinal);
        for (int i = 0; i < QueryDurationBoundsMs.Length; i++)
            durationBuckets[$"le_{QueryDurationBoundsMs[i]}"] = Interlocked.Read(ref _queryDurationBuckets[i]);

        return new DebugStats
        {
            WriteDiagnosticsEnabled = WriteDiagnostics.Enabled,
            WritePhaseMilliseconds = WriteDiagnostics.Enabled ? WriteDiagnostics.Milliseconds() : null,
            WritePhaseCounts = WriteDiagnostics.Enabled ? WriteDiagnostics.Counts() : null,
            WritePointsTotal = WritePointsTotal,
            QueryTotal = QueryTotal,
            QueryErrorTotal = QueryErrorTotal,
            QueryTimeoutTotal = QueryTimeoutTotal,
            QueryRowsReturnedTotal = QueryRowsReturnedTotal,
            QueryScannedPointsTotal = QueryScannedPointsTotal,
            QueryEstimatedInputBytesTotal = QueryEstimatedInputBytesTotal,
            QueryEstimatedResultBytesTotal = QueryEstimatedResultBytesTotal,
            LastQueryEstimatedPeakBytes = LastQueryEstimatedPeakBytes,
            ContinuousQueryRunsTotal = ContinuousQueryRunsTotal,
            ContinuousQueryErrorsTotal = ContinuousQueryErrorsTotal,
            ContinuousQueryBucketsTotal = ContinuousQueryBucketsTotal,
            ContinuousQueryRecomputeBucketsTotal = ContinuousQueryRecomputeBucketsTotal,
            LastContinuousQueryDurationMs = LastContinuousQueryDurationMs,
            SegmentCount = CountSegmentFiles(),
            QuarantinedSegments = _engine.QuarantinedSegments.Count,
            CompactionRunning = compaction.Running || CompactionRunning,
            CompactionCount = compaction.TotalRuns > 0 ? compaction.TotalRuns : CompactionCount,
            CompactionQueuedTasks = compaction.QueuedTasks,
            CompactionBacklogTasks = compaction.BacklogTasks,
            CompactionBacklogBytes = compaction.BacklogBytes,
            CompactionInputBytesTotal = compaction.TotalInputBytes,
            CompactionOutputBytesTotal = compaction.TotalOutputBytes,
            CompactionDurationMsTotal = compaction.TotalDurationMs,
            CompactionTasksTotal = compaction.TotalTasks,
            CompactionSegmentsMergedTotal = compaction.TotalSegmentsMerged,
            SeriesCardinality = cardinality,
            MemoryBufferPoints = _engine.GetBufferedPointCount(),
            MemoryBufferBytes = _engine.GetBufferedByteCount(),
            FlushSnapshotPoints = _engine.GetFlushSnapshotPointCount(),
            FlushSnapshotBytes = _engine.GetFlushSnapshotByteCount(),
            WriteQueuePendingRequests = _writeQueue?.PendingRequests ?? 0,
            WriteQueueQueuedPoints = _writeQueue?.QueuedPoints ?? 0,
            WriteQueueQueuedBytes = _writeQueue?.QueuedBytes ?? 0,
            QueryDurationBuckets = durationBuckets,
            ContinuousQueryMetrics = GetContinuousQueryMetricsSnapshot(),
            Gc = CollectGcStats()
        };
    }

    /// <summary>Escapes a Prometheus label value (backslash, double quote, newline).</summary>
    private static string EscapeLabelValue(string value) =>
        value.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n");

    private List<ContinuousQueryMetrics> GetContinuousQueryMetricsSnapshot()
    {
        lock (_continuousQueryMetricsLock)
        {
            return _continuousQueryMetrics.Values
                .OrderBy(x => x.Database, StringComparer.Ordinal)
                .ThenBy(x => x.Name, StringComparer.Ordinal)
                .Select(x => new ContinuousQueryMetrics
                {
                    Database = x.Database,
                    Name = x.Name,
                    RunsTotal = x.RunsTotal,
                    ErrorsTotal = x.ErrorsTotal,
                    BucketsTotal = x.BucketsTotal,
                    RecomputeBucketsTotal = x.RecomputeBucketsTotal,
                    LastDurationMs = x.LastDurationMs,
                    LastBucketStartNs = x.LastBucketStartNs
                })
                .ToList();
        }
    }

    private int CountSegmentFiles()
    {
        int count = 0;
        var dbDir = Path.Combine(_engine.RootPath, "db");
        if (!Directory.Exists(dbDir)) return 0;
        try
        {
            foreach (var f in Directory.EnumerateFiles(dbDir, "*.seg", SearchOption.AllDirectories))
                count++;
        }
        catch (Exception ex)
        {
            // Directory enumeration can fail with IOException/UnauthorizedAccessException when a
            // segment is being concurrently deleted; report once to avoid spamming on every
            // subsequent /metrics poll, but keep returning 0 so the endpoint stays usable.
            if (!_segmentEnumerationErrorLogged)
            {
                _segmentEnumerationErrorLogged = true;
                _logger?.LogWarning(ex, "metrics: failed to enumerate segment files in {Dir}", dbDir);
            }
        }
        return count;
    }

    /// <summary>
    /// Format Prometheus text exposition for /metrics endpoint.
    /// </summary>
    public string FormatPrometheus()
    {
        var sb = new StringBuilder();
        var stats = CollectStats();

        sb.AppendLine("# HELP mini_influx_write_points_total Total number of points written.");
        sb.AppendLine("# TYPE mini_influx_write_points_total counter");
        sb.AppendLine($"mini_influx_write_points_total {stats.WritePointsTotal}");

        sb.AppendLine("# HELP mini_influx_query_total Total number of queries executed.");
        sb.AppendLine("# TYPE mini_influx_query_total counter");
        sb.AppendLine($"mini_influx_query_total {stats.QueryTotal}");

        sb.AppendLine("# HELP mini_influx_query_errors_total Total number of queries failed.");
        sb.AppendLine("# TYPE mini_influx_query_errors_total counter");
        sb.AppendLine($"mini_influx_query_errors_total {stats.QueryErrorTotal}");

        sb.AppendLine("# HELP mini_influx_query_timeouts_total Total number of query timeouts.");
        sb.AppendLine("# TYPE mini_influx_query_timeouts_total counter");
        sb.AppendLine($"mini_influx_query_timeouts_total {stats.QueryTimeoutTotal}");

        sb.AppendLine("# HELP mini_influx_query_rows_returned_total Total number of rows returned by queries.");
        sb.AppendLine("# TYPE mini_influx_query_rows_returned_total counter");
        sb.AppendLine($"mini_influx_query_rows_returned_total {stats.QueryRowsReturnedTotal}");

        sb.AppendLine("# HELP mini_influx_query_scanned_points_total Total number of points scanned by queries.");
        sb.AppendLine("# TYPE mini_influx_query_scanned_points_total counter");
        sb.AppendLine($"mini_influx_query_scanned_points_total {stats.QueryScannedPointsTotal}");

        sb.AppendLine("# HELP mini_influx_query_estimated_input_bytes_total Estimated bytes loaded into query execution.");
        sb.AppendLine("# TYPE mini_influx_query_estimated_input_bytes_total counter");
        sb.AppendLine($"mini_influx_query_estimated_input_bytes_total {stats.QueryEstimatedInputBytesTotal}");

        sb.AppendLine("# HELP mini_influx_query_estimated_result_bytes_total Estimated bytes materialized in query results.");
        sb.AppendLine("# TYPE mini_influx_query_estimated_result_bytes_total counter");
        sb.AppendLine($"mini_influx_query_estimated_result_bytes_total {stats.QueryEstimatedResultBytesTotal}");

        sb.AppendLine("# HELP mini_influx_last_query_estimated_peak_bytes Estimated peak memory footprint of the latest query.");
        sb.AppendLine("# TYPE mini_influx_last_query_estimated_peak_bytes gauge");
        sb.AppendLine($"mini_influx_last_query_estimated_peak_bytes {stats.LastQueryEstimatedPeakBytes}");

        sb.AppendLine("# HELP mini_influx_continuous_query_runs_total Total number of continuous query executions.");
        sb.AppendLine("# TYPE mini_influx_continuous_query_runs_total counter");
        sb.AppendLine($"mini_influx_continuous_query_runs_total {stats.ContinuousQueryRunsTotal}");

        sb.AppendLine("# HELP mini_influx_continuous_query_errors_total Total number of failed continuous query executions.");
        sb.AppendLine("# TYPE mini_influx_continuous_query_errors_total counter");
        sb.AppendLine($"mini_influx_continuous_query_errors_total {stats.ContinuousQueryErrorsTotal}");

        sb.AppendLine("# HELP mini_influx_continuous_query_buckets_total Total number of continuous query buckets processed.");
        sb.AppendLine("# TYPE mini_influx_continuous_query_buckets_total counter");
        sb.AppendLine($"mini_influx_continuous_query_buckets_total {stats.ContinuousQueryBucketsTotal}");

        sb.AppendLine("# HELP mini_influx_continuous_query_recompute_buckets_total Total number of continuous query buckets recomputed.");
        sb.AppendLine("# TYPE mini_influx_continuous_query_recompute_buckets_total counter");
        sb.AppendLine($"mini_influx_continuous_query_recompute_buckets_total {stats.ContinuousQueryRecomputeBucketsTotal}");

        sb.AppendLine("# HELP mini_influx_last_continuous_query_duration_ms Duration of the latest continuous query execution.");
        sb.AppendLine("# TYPE mini_influx_last_continuous_query_duration_ms gauge");
        sb.AppendLine($"mini_influx_last_continuous_query_duration_ms {stats.LastContinuousQueryDurationMs}");

        sb.AppendLine("# HELP mini_influx_continuous_query_run_total Total number of continuous query executions by query.");
        sb.AppendLine("# TYPE mini_influx_continuous_query_run_total counter");
        foreach (var metric in stats.ContinuousQueryMetrics)
            sb.AppendLine($"mini_influx_continuous_query_run_total{{db=\"{EscapeLabelValue(metric.Database)}\",name=\"{EscapeLabelValue(metric.Name)}\"}} {metric.RunsTotal}");

        sb.AppendLine("# HELP mini_influx_continuous_query_error_total Total number of failed continuous query executions by query.");
        sb.AppendLine("# TYPE mini_influx_continuous_query_error_total counter");
        foreach (var metric in stats.ContinuousQueryMetrics)
            sb.AppendLine($"mini_influx_continuous_query_error_total{{db=\"{EscapeLabelValue(metric.Database)}\",name=\"{EscapeLabelValue(metric.Name)}\"}} {metric.ErrorsTotal}");

        sb.AppendLine("# HELP mini_influx_continuous_query_bucket_total Total number of processed continuous query buckets by query.");
        sb.AppendLine("# TYPE mini_influx_continuous_query_bucket_total counter");
        foreach (var metric in stats.ContinuousQueryMetrics)
            sb.AppendLine($"mini_influx_continuous_query_bucket_total{{db=\"{EscapeLabelValue(metric.Database)}\",name=\"{EscapeLabelValue(metric.Name)}\"}} {metric.BucketsTotal}");

        sb.AppendLine("# HELP mini_influx_continuous_query_recompute_bucket_total Total number of recomputed continuous query buckets by query.");
        sb.AppendLine("# TYPE mini_influx_continuous_query_recompute_bucket_total counter");
        foreach (var metric in stats.ContinuousQueryMetrics)
            sb.AppendLine($"mini_influx_continuous_query_recompute_bucket_total{{db=\"{EscapeLabelValue(metric.Database)}\",name=\"{EscapeLabelValue(metric.Name)}\"}} {metric.RecomputeBucketsTotal}");

        sb.AppendLine("# HELP mini_influx_query_duration_ms Query duration bucket counters.");
        sb.AppendLine("# TYPE mini_influx_query_duration_ms histogram");
        long cumulative = 0;
        foreach (var bound in QueryDurationBoundsMs)
        {
            cumulative += stats.QueryDurationBuckets.TryGetValue($"le_{bound}", out var bucketCount) ? bucketCount : 0;
            sb.AppendLine($"mini_influx_query_duration_ms_bucket{{le=\"{bound}\"}} {cumulative}");
        }
        sb.AppendLine($"mini_influx_query_duration_ms_bucket{{le=\"+Inf\"}} {stats.QueryTotal}");
        sb.AppendLine($"mini_influx_query_duration_ms_count {stats.QueryTotal}");

        sb.AppendLine("# HELP mini_influx_segment_files Total number of segment files.");
        sb.AppendLine("# TYPE mini_influx_segment_files gauge");
        sb.AppendLine($"mini_influx_segment_files {stats.SegmentCount}");

        sb.AppendLine("# HELP mini_influx_compaction_running Whether compaction is currently running.");
        sb.AppendLine("# TYPE mini_influx_compaction_running gauge");
        sb.AppendLine($"mini_influx_compaction_running {(stats.CompactionRunning ? 1 : 0)}");

        sb.AppendLine("# HELP mini_influx_compaction_total Total number of compaction runs.");
        sb.AppendLine("# TYPE mini_influx_compaction_total counter");
        sb.AppendLine($"mini_influx_compaction_total {stats.CompactionCount}");

        sb.AppendLine("# HELP mini_influx_compaction_tasks_total Total number of compaction tasks executed.");
        sb.AppendLine("# TYPE mini_influx_compaction_tasks_total counter");
        sb.AppendLine($"mini_influx_compaction_tasks_total {stats.CompactionTasksTotal}");

        sb.AppendLine("# HELP mini_influx_compaction_segments_merged_total Total number of segments merged by compaction.");
        sb.AppendLine("# TYPE mini_influx_compaction_segments_merged_total counter");
        sb.AppendLine($"mini_influx_compaction_segments_merged_total {stats.CompactionSegmentsMergedTotal}");

        sb.AppendLine("# HELP mini_influx_compaction_queued_tasks Current compaction queue size.");
        sb.AppendLine("# TYPE mini_influx_compaction_queued_tasks gauge");
        sb.AppendLine($"mini_influx_compaction_queued_tasks {stats.CompactionQueuedTasks}");
        sb.AppendLine("# HELP mini_influx_compaction_backlog_tasks Estimated compaction tasks waiting to run.");
        sb.AppendLine("# TYPE mini_influx_compaction_backlog_tasks gauge");
        sb.AppendLine($"mini_influx_compaction_backlog_tasks {stats.CompactionBacklogTasks}");

        sb.AppendLine("# HELP mini_influx_buffer_points Current number of points in memory buffer.");
        sb.AppendLine("# TYPE mini_influx_buffer_points gauge");
        sb.AppendLine($"mini_influx_buffer_points {stats.MemoryBufferPoints}");

        sb.AppendLine("# HELP mini_influx_buffer_bytes Estimated number of bytes in memory buffer.");
        sb.AppendLine("# TYPE mini_influx_buffer_bytes gauge");
        sb.AppendLine($"mini_influx_buffer_bytes {stats.MemoryBufferBytes}");
        sb.AppendLine("# HELP mini_influx_flush_snapshot_points Points held by in-flight async flush snapshots.");
        sb.AppendLine("# TYPE mini_influx_flush_snapshot_points gauge");
        sb.AppendLine($"mini_influx_flush_snapshot_points {stats.FlushSnapshotPoints}");
        sb.AppendLine("# HELP mini_influx_flush_snapshot_bytes Estimated bytes held by in-flight async flush snapshots.");
        sb.AppendLine("# TYPE mini_influx_flush_snapshot_bytes gauge");
        sb.AppendLine($"mini_influx_flush_snapshot_bytes {stats.FlushSnapshotBytes}");
        sb.AppendLine("# HELP mini_influx_write_queue_pending_requests Writes waiting for or running in the queue.");
        sb.AppendLine("# TYPE mini_influx_write_queue_pending_requests gauge");
        sb.AppendLine($"mini_influx_write_queue_pending_requests {stats.WriteQueuePendingRequests}");
        sb.AppendLine("# HELP mini_influx_write_queue_queued_points Points currently queued (not yet processed).");
        sb.AppendLine("# TYPE mini_influx_write_queue_queued_points gauge");
        sb.AppendLine($"mini_influx_write_queue_queued_points {stats.WriteQueueQueuedPoints}");
        sb.AppendLine("# HELP mini_influx_write_queue_queued_bytes Estimated bytes currently queued (not yet processed).");
        sb.AppendLine("# TYPE mini_influx_write_queue_queued_bytes gauge");
        sb.AppendLine($"mini_influx_write_queue_queued_bytes {stats.WriteQueueQueuedBytes}");

        sb.AppendLine("# HELP mini_influx_compaction_input_bytes_total Total bytes read by compaction from source segments.");
        sb.AppendLine("# TYPE mini_influx_compaction_input_bytes_total counter");
        sb.AppendLine($"mini_influx_compaction_input_bytes_total {stats.CompactionInputBytesTotal}");
        sb.AppendLine("# HELP mini_influx_compaction_output_bytes_total Total bytes written by compaction to merged segments.");
        sb.AppendLine("# TYPE mini_influx_compaction_output_bytes_total counter");
        sb.AppendLine($"mini_influx_compaction_output_bytes_total {stats.CompactionOutputBytesTotal}");
        sb.AppendLine("# HELP mini_influx_compaction_duration_ms_total Total time spent in compaction runs.");
        sb.AppendLine("# TYPE mini_influx_compaction_duration_ms_total counter");
        sb.AppendLine($"mini_influx_compaction_duration_ms_total {stats.CompactionDurationMsTotal}");
        sb.AppendLine("# HELP mini_influx_compaction_backlog_bytes Estimated bytes of segments awaiting compaction.");
        sb.AppendLine("# TYPE mini_influx_compaction_backlog_bytes gauge");
        sb.AppendLine($"mini_influx_compaction_backlog_bytes {stats.CompactionBacklogBytes}");

        if (stats.Gc is { } gc)
        {
            sb.AppendLine("# HELP mini_influx_gc_collection_count_gen0 GC gen-0 collection count.");
            sb.AppendLine("# TYPE mini_influx_gc_collection_count_gen0 counter");
            sb.AppendLine($"mini_influx_gc_collection_count_gen0 {gc.CollectionCountGen0}");
            sb.AppendLine("# HELP mini_influx_gc_collection_count_gen1 GC gen-1 collection count.");
            sb.AppendLine("# TYPE mini_influx_gc_collection_count_gen1 counter");
            sb.AppendLine($"mini_influx_gc_collection_count_gen1 {gc.CollectionCountGen1}");
            sb.AppendLine("# HELP mini_influx_gc_collection_count_gen2 GC gen-2 collection count.");
            sb.AppendLine("# TYPE mini_influx_gc_collection_count_gen2 counter");
            sb.AppendLine($"mini_influx_gc_collection_count_gen2 {gc.CollectionCountGen2}");
            sb.AppendLine("# HELP mini_influx_gc_heap_size_bytes GC managed heap size in bytes.");
            sb.AppendLine("# TYPE mini_influx_gc_heap_size_bytes gauge");
            sb.AppendLine($"mini_influx_gc_heap_size_bytes {gc.HeapSizeBytes}");
            sb.AppendLine("# HELP mini_influx_gc_committed_bytes GC committed memory in bytes.");
            sb.AppendLine("# TYPE mini_influx_gc_committed_bytes gauge");
            sb.AppendLine($"mini_influx_gc_committed_bytes {gc.TotalCommittedBytes}");
            sb.AppendLine("# HELP mini_influx_gc_pause_duration_ms_total Total GC pause duration in milliseconds.");
            sb.AppendLine("# TYPE mini_influx_gc_pause_duration_ms_total counter");
            sb.AppendLine($"mini_influx_gc_pause_duration_ms_total {gc.PauseDurationMsTotal:0.###}");
            sb.AppendLine("# HELP mini_influx_process_working_set_bytes Process working set in bytes.");
            sb.AppendLine("# TYPE mini_influx_process_working_set_bytes gauge");
            sb.AppendLine($"mini_influx_process_working_set_bytes {gc.WorkingSetBytes}");
        }

        var health = _engine.Health;
        sb.AppendLine("# HELP mini_influx_storage_write_available Whether the WAL-backed write path is available.");
        sb.AppendLine("# TYPE mini_influx_storage_write_available gauge");
        sb.AppendLine($"mini_influx_storage_write_available {(health.WriteAvailable ? 1 : 0)}");
        sb.AppendLine("# HELP mini_influx_storage_failures_total Storage background and durability failures.");
        sb.AppendLine("# TYPE mini_influx_storage_failures_total counter");
        sb.AppendLine($"mini_influx_storage_failures_total {health.FailureCount}");
        sb.AppendLine("# HELP mini_influx_data_disk_free_bytes Free bytes on the data volume.");
        sb.AppendLine("# TYPE mini_influx_data_disk_free_bytes gauge");
        sb.AppendLine($"mini_influx_data_disk_free_bytes {GetAvailableDiskBytes()}");

        sb.AppendLine("# HELP mini_influx_series_cardinality Number of unique series per database.");
        sb.AppendLine("# TYPE mini_influx_series_cardinality gauge");
        foreach (var (db, count) in stats.SeriesCardinality)
            sb.AppendLine($"mini_influx_series_cardinality{{db=\"{db}\"}} {count}");

        return sb.ToString();
    }

    private long GetAvailableDiskBytes()
    {
        var root = Path.GetPathRoot(Path.GetFullPath(_engine.RootPath));
        return string.IsNullOrWhiteSpace(root) ? 0 : new DriveInfo(root).AvailableFreeSpace;
    }

    private static GcStats CollectGcStats()
    {
        var mem = GC.GetGCMemoryInfo();
        double pauseMs = 0;
        foreach (var t in mem.PauseDurations) pauseMs += t.TotalMilliseconds;
        var proc = System.Diagnostics.Process.GetCurrentProcess();
        return new GcStats
        {
            CollectionCountGen0 = GC.CollectionCount(0),
            CollectionCountGen1 = GC.CollectionCount(1),
            CollectionCountGen2 = GC.CollectionCount(2),
            TotalCommittedBytes = mem.TotalCommittedBytes,
            HeapSizeBytes = mem.HeapSizeBytes,
            PauseDurationMsTotal = pauseMs,
            WorkingSetBytes = proc.WorkingSet64,
            PagedMemoryBytes = proc.PagedMemorySize64
        };
    }
}

public sealed class DebugStats
{
    public bool WriteDiagnosticsEnabled { get; set; }
    public Dictionary<string, double>? WritePhaseMilliseconds { get; set; }
    public Dictionary<string, long>? WritePhaseCounts { get; set; }
    public long WritePointsTotal { get; set; }
    public long QueryTotal { get; set; }
    public long QueryErrorTotal { get; set; }
    public long QueryTimeoutTotal { get; set; }
    public long QueryRowsReturnedTotal { get; set; }
    public long QueryScannedPointsTotal { get; set; }
    public long QueryEstimatedInputBytesTotal { get; set; }
    public long QueryEstimatedResultBytesTotal { get; set; }
    public long LastQueryEstimatedPeakBytes { get; set; }
    public long ContinuousQueryRunsTotal { get; set; }
    public long ContinuousQueryErrorsTotal { get; set; }
    public long ContinuousQueryBucketsTotal { get; set; }
    public long ContinuousQueryRecomputeBucketsTotal { get; set; }
    public long LastContinuousQueryDurationMs { get; set; }
    public int SegmentCount { get; set; }
    public bool CompactionRunning { get; set; }
    public long CompactionCount { get; set; }
    public long CompactionTasksTotal { get; set; }
    public long CompactionSegmentsMergedTotal { get; set; }
    public int CompactionQueuedTasks { get; set; }
    public int CompactionBacklogTasks { get; set; }
    public Dictionary<string, int> SeriesCardinality { get; set; } = new();
    public long MemoryBufferPoints { get; set; }
    public long MemoryBufferBytes { get; set; }
    public long FlushSnapshotPoints { get; set; }
    public long FlushSnapshotBytes { get; set; }
    public long WriteQueuePendingRequests { get; set; }
    public long WriteQueueQueuedPoints { get; set; }
    public long WriteQueueQueuedBytes { get; set; }
    public long CompactionBacklogBytes { get; set; }
    public long CompactionInputBytesTotal { get; set; }
    public long CompactionOutputBytesTotal { get; set; }
    public long CompactionDurationMsTotal { get; set; }
    public Dictionary<string, long> QueryDurationBuckets { get; set; } = new();
    public List<ContinuousQueryMetrics> ContinuousQueryMetrics { get; set; } = [];
    public int QuarantinedSegments { get; set; }
    public GcStats? Gc { get; set; }
}

public sealed class GcStats
{
    public long CollectionCountGen0 { get; set; }
    public long CollectionCountGen1 { get; set; }
    public long CollectionCountGen2 { get; set; }
    public long TotalCommittedBytes { get; set; }
    public long HeapSizeBytes { get; set; }
    public double PauseDurationMsTotal { get; set; }
    public long WorkingSetBytes { get; set; }
    public long PagedMemoryBytes { get; set; }
}

public sealed class ContinuousQueryMetrics
{
    public string Database { get; set; } = "";
    public string Name { get; set; } = "";
    public long RunsTotal { get; set; }
    public long ErrorsTotal { get; set; }
    public long BucketsTotal { get; set; }
    public long RecomputeBucketsTotal { get; set; }
    public long LastDurationMs { get; set; }
    public long? LastBucketStartNs { get; set; }
}
