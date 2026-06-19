using System.Text;

namespace MiniInflux.Net10.Storage;

/// <summary>
/// Lightweight metrics collector for observability endpoints (/health, /debug/stats, /metrics).
/// Uses simple atomic counters to avoid external dependencies.
/// </summary>
public sealed class MetricsCollector
{
    private long _writePointsTotal;
    private long _queryTotal;
    private long _compactionCount;
    private int _compactionRunning;
    private readonly TsdbEngine _engine;

    public MetricsCollector(TsdbEngine engine) { _engine = engine; }

    public void RecordWrite(int pointCount) => Interlocked.Add(ref _writePointsTotal, pointCount);
    public void RecordQuery() => Interlocked.Increment(ref _queryTotal);
    public void SetCompactionRunning(bool v) => Interlocked.Exchange(ref _compactionRunning, v ? 1 : 0);
    public void RecordCompaction() => Interlocked.Increment(ref _compactionCount);

    public long WritePointsTotal => Interlocked.Read(ref _writePointsTotal);
    public long QueryTotal => Interlocked.Read(ref _queryTotal);
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

        return new DebugStats
        {
            WritePointsTotal = WritePointsTotal,
            QueryTotal = QueryTotal,
            SegmentCount = CountSegmentFiles(),
            CompactionRunning = CompactionRunning,
            CompactionCount = CompactionCount,
            SeriesCardinality = cardinality,
            MemoryBufferPoints = _engine.GetBufferedPointCount()
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

        sb.AppendLine("# HELP mini_influx_segment_files Total number of segment files.");
        sb.AppendLine("# TYPE mini_influx_segment_files gauge");
        sb.AppendLine($"mini_influx_segment_files {stats.SegmentCount}");

        sb.AppendLine("# HELP mini_influx_compaction_running Whether compaction is currently running.");
        sb.AppendLine("# TYPE mini_influx_compaction_running gauge");
        sb.AppendLine($"mini_influx_compaction_running {(stats.CompactionRunning ? 1 : 0)}");

        sb.AppendLine("# HELP mini_influx_compaction_total Total number of compaction runs.");
        sb.AppendLine("# TYPE mini_influx_compaction_total counter");
        sb.AppendLine($"mini_influx_compaction_total {stats.CompactionCount}");

        sb.AppendLine("# HELP mini_influx_buffer_points Current number of points in memory buffer.");
        sb.AppendLine("# TYPE mini_influx_buffer_points gauge");
        sb.AppendLine($"mini_influx_buffer_points {stats.MemoryBufferPoints}");

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
    public int SegmentCount { get; set; }
    public bool CompactionRunning { get; set; }
    public long CompactionCount { get; set; }
    public Dictionary<string, int> SeriesCardinality { get; set; } = new();
    public long MemoryBufferPoints { get; set; }
}
