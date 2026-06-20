using System.Text;
using MiniInflux.Net10.Query;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Lightweight metrics collector for observability endpoints (/health, /debug/stats, /metrics).
/// Uses simple atomic counters to avoid external dependencies.
/// </summary>
public sealed class MetricsCollector
{
    private long _writePointsTotal;
    private long _queryTotal;
    private long _queryErrorTotal;
    private long _queryTimeoutTotal;
    private long _queryRowsReturnedTotal;
    private long _queryScannedPointsTotal;
    private long _queryEstimatedInputBytesTotal;
    private long _queryEstimatedResultBytesTotal;
    private long _lastQueryEstimatedPeakBytes;
    private long _compactionCount;
    private int _compactionRunning;
    private readonly long[] _queryDurationBuckets = new long[8];
    private static readonly int[] QueryDurationBoundsMs = [1, 5, 10, 50, 100, 500, 1000, 5000];
    private readonly TsdbEngine _engine;

    public MetricsCollector(TsdbEngine engine) { _engine = engine; }

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

    public long WritePointsTotal => Interlocked.Read(ref _writePointsTotal);
    public long QueryTotal => Interlocked.Read(ref _queryTotal);
    public long QueryErrorTotal => Interlocked.Read(ref _queryErrorTotal);
    public long QueryTimeoutTotal => Interlocked.Read(ref _queryTimeoutTotal);
    public long QueryRowsReturnedTotal => Interlocked.Read(ref _queryRowsReturnedTotal);
    public long QueryScannedPointsTotal => Interlocked.Read(ref _queryScannedPointsTotal);
    public long QueryEstimatedInputBytesTotal => Interlocked.Read(ref _queryEstimatedInputBytesTotal);
    public long QueryEstimatedResultBytesTotal => Interlocked.Read(ref _queryEstimatedResultBytesTotal);
    public long LastQueryEstimatedPeakBytes => Interlocked.Read(ref _lastQueryEstimatedPeakBytes);
    public bool CompactionRunning => Interlocked.CompareExchange(ref _compactionRunning, 0, 0) == 1;
    public long CompactionCount => Interlocked.Read(ref _compactionCount);

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
            WritePointsTotal = WritePointsTotal,
            QueryTotal = QueryTotal,
            QueryErrorTotal = QueryErrorTotal,
            QueryTimeoutTotal = QueryTimeoutTotal,
            QueryRowsReturnedTotal = QueryRowsReturnedTotal,
            QueryScannedPointsTotal = QueryScannedPointsTotal,
            QueryEstimatedInputBytesTotal = QueryEstimatedInputBytesTotal,
            QueryEstimatedResultBytesTotal = QueryEstimatedResultBytesTotal,
            LastQueryEstimatedPeakBytes = LastQueryEstimatedPeakBytes,
            SegmentCount = CountSegmentFiles(),
            CompactionRunning = compaction.Running || CompactionRunning,
            CompactionCount = compaction.TotalRuns > 0 ? compaction.TotalRuns : CompactionCount,
            CompactionQueuedTasks = compaction.QueuedTasks,
            CompactionTasksTotal = compaction.TotalTasks,
            CompactionSegmentsMergedTotal = compaction.TotalSegmentsMerged,
            SeriesCardinality = cardinality,
            MemoryBufferPoints = _engine.GetBufferedPointCount(),
            MemoryBufferBytes = _engine.GetBufferedByteCount(),
            QueryDurationBuckets = durationBuckets
        };
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
        catch { }
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

        sb.AppendLine("# HELP mini_influx_buffer_points Current number of points in memory buffer.");
        sb.AppendLine("# TYPE mini_influx_buffer_points gauge");
        sb.AppendLine($"mini_influx_buffer_points {stats.MemoryBufferPoints}");

        sb.AppendLine("# HELP mini_influx_buffer_bytes Estimated number of bytes in memory buffer.");
        sb.AppendLine("# TYPE mini_influx_buffer_bytes gauge");
        sb.AppendLine($"mini_influx_buffer_bytes {stats.MemoryBufferBytes}");

        sb.AppendLine("# HELP mini_influx_series_cardinality Number of unique series per database.");
        sb.AppendLine("# TYPE mini_influx_series_cardinality gauge");
        foreach (var (db, count) in stats.SeriesCardinality)
            sb.AppendLine($"mini_influx_series_cardinality{{db=\"{db}\"}} {count}");

        return sb.ToString();
    }
}

public sealed class DebugStats
{
    public long WritePointsTotal { get; set; }
    public long QueryTotal { get; set; }
    public long QueryErrorTotal { get; set; }
    public long QueryTimeoutTotal { get; set; }
    public long QueryRowsReturnedTotal { get; set; }
    public long QueryScannedPointsTotal { get; set; }
    public long QueryEstimatedInputBytesTotal { get; set; }
    public long QueryEstimatedResultBytesTotal { get; set; }
    public long LastQueryEstimatedPeakBytes { get; set; }
    public int SegmentCount { get; set; }
    public bool CompactionRunning { get; set; }
    public long CompactionCount { get; set; }
    public long CompactionTasksTotal { get; set; }
    public long CompactionSegmentsMergedTotal { get; set; }
    public int CompactionQueuedTasks { get; set; }
    public Dictionary<string, int> SeriesCardinality { get; set; } = new();
    public long MemoryBufferPoints { get; set; }
    public long MemoryBufferBytes { get; set; }
    public Dictionary<string, long> QueryDurationBuckets { get; set; } = new();
}
