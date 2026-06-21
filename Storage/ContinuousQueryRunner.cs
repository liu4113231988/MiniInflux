using System.Globalization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;

namespace MiniInflux.Net10.Storage;

public sealed class ContinuousQueryRunner
{
    private readonly TsdbEngine _engine;
    private readonly QueryExecutor _executor;
    private readonly MiniInfluxOptions _options;
    private readonly MetricsCollector _metrics;
    private readonly ILogger<ContinuousQueryRunner> _logger;

    public ContinuousQueryRunner(
        TsdbEngine engine,
        QueryExecutor executor,
        MiniInfluxOptions options,
        MetricsCollector metrics,
        ILogger<ContinuousQueryRunner> logger)
    {
        _engine = engine;
        _executor = executor;
        _options = options;
        _metrics = metrics;
        _logger = logger;
    }

    public Task<int> ExecuteDueQueriesAsync(CancellationToken cancellationToken = default)
    {
        if (!_options.ContinuousQuery.Enabled)
            return Task.FromResult(0);

        var nowNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        int executed = 0;
        foreach (var cq in _engine.Meta.ListContinuousQueries())
            executed += ExecuteDueQuery(cq, nowNs, cancellationToken);
        return Task.FromResult(executed);
    }

    private int ExecuteDueQuery(ContinuousQueryInfo cq, long nowNs, CancellationToken cancellationToken)
    {
        ParsedQuery parsed;
        try
        {
            parsed = InfluxQlParser.Parse(cq.QueryText);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "continuous query parse failed name={Name} db={Database}", cq.Name, cq.Database);
            return 0;
        }

        if (parsed.Kind != QueryKind.Select || !parsed.GroupByNs.HasValue || string.IsNullOrWhiteSpace(parsed.IntoTarget))
        {
            _logger.LogWarning("continuous query skipped due to invalid shape name={Name} db={Database}", cq.Name, cq.Database);
            return 0;
        }

        var everyNs = cq.EveryNs > 0 ? cq.EveryNs : parsed.GroupByNs.Value;
        var effectiveForNs = cq.ForNs > 0 ? cq.ForNs : _options.ContinuousQuery.InitialBackfillDurationNs;
        var latestClosedBucketStart = (nowNs / everyNs) * everyNs - everyNs;
        if (latestClosedBucketStart < 0)
            return 0;

        var earliestAllowedBucketStart = ResolveEarliestAllowedBucketStart(latestClosedBucketStart, everyNs, effectiveForNs);
        long lastTargetBucketStart;
        long nextBucketStart;
        if (cq.LastCompletedBucketStartNs == long.MinValue)
        {
            var initialRange = ResolveInitialExecutionRange(cq, parsed, everyNs, earliestAllowedBucketStart, latestClosedBucketStart, cancellationToken);
            nextBucketStart = initialRange.StartBucketStartNs;
            lastTargetBucketStart = initialRange.EndBucketStartNs;
        }
        else
        {
            nextBucketStart = cq.LastCompletedBucketStartNs + everyNs;
            nextBucketStart = Math.Max(nextBucketStart, earliestAllowedBucketStart);
            lastTargetBucketStart = latestClosedBucketStart;
        }

        var pendingBuckets = new List<(long BucketStartNs, bool Recompute)>();
        for (var bucket = nextBucketStart; bucket <= lastTargetBucketStart; bucket += everyNs)
            pendingBuckets.Add((bucket, false));

        var recomputeRecentBuckets = Math.Max(0, _options.ContinuousQuery.RecomputeRecentBuckets);
        if (recomputeRecentBuckets > 0)
        {
            var recomputeStart = latestClosedBucketStart - (recomputeRecentBuckets - 1L) * everyNs;
            for (var bucket = Math.Max(recomputeStart, earliestAllowedBucketStart); bucket <= latestClosedBucketStart; bucket += everyNs)
                pendingBuckets.Add((bucket, true));
        }

        pendingBuckets = pendingBuckets
            .GroupBy(x => x.BucketStartNs)
            .Select(g => (BucketStartNs: g.Key, Recompute: g.Any(x => x.Recompute)))
            .OrderBy(x => x.BucketStartNs)
            .Take(Math.Max(1, _options.ContinuousQuery.MaxCatchUpRunsPerCycle))
            .ToList();

        int executed = 0;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        foreach (var pending in pendingBuckets)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var boundedQuery = InjectTimeWindow(cq.QueryText, pending.BucketStartNs, pending.BucketStartNs + everyNs);
            var outcome = _executor.ExecuteWithReport(_engine, cq.Database, boundedQuery, cancellationToken);
            var error = outcome.Response.Results.FirstOrDefault()?.Error;
            if (!string.IsNullOrWhiteSpace(error))
            {
                _logger.LogWarning("continuous query execution failed name={Name} db={Database} bucket_start={BucketStart} error={Error}",
                    cq.Name, cq.Database, pending.BucketStartNs, error);
                sw.Stop();
                _metrics.RecordContinuousQueryRun(cq.Database, cq.Name, executed, hadError: true, wasRecompute: pending.Recompute, durationMs: sw.ElapsedMilliseconds, lastBucketStartNs: pending.BucketStartNs);
                break;
            }

            if (!pending.Recompute)
                _engine.Meta.UpdateContinuousQueryProgress(cq.Database, cq.Name, pending.BucketStartNs);
            executed++;
            _logger.LogDebug("continuous query executed name={Name} db={Database} bucket_start={BucketStart}",
                cq.Name, cq.Database, pending.BucketStartNs);
            _metrics.RecordContinuousQueryRun(cq.Database, cq.Name, 1, hadError: false, wasRecompute: pending.Recompute, durationMs: outcome.Report.DurationMs, lastBucketStartNs: pending.BucketStartNs);
        }

        sw.Stop();
        return executed;
    }

    private (long StartBucketStartNs, long EndBucketStartNs) ResolveInitialExecutionRange(
        ContinuousQueryInfo cq,
        ParsedQuery parsed,
        long everyNs,
        long earliestAllowedBucketStart,
        long latestClosedBucketStart,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(parsed.Measurement))
            return (earliestAllowedBucketStart, latestClosedBucketStart);

        var sourceDb = parsed.SourceDatabase ?? cq.Database;
        var sourceRp = parsed.SourceRpName ?? _engine.GetDefaultRpName(sourceDb);
        var points = _engine.ReadAllPoints(sourceDb, sourceRp, parsed.Measurement, parsed.MinTimeNs, parsed.MaxTimeNs, cancellationToken: cancellationToken);
        if (points.Count == 0)
            return (earliestAllowedBucketStart, latestClosedBucketStart);

        var latestDataBucketStart = (points.Max(p => p.TimestampNs) / everyNs) * everyNs;
        var targetBucketStart = Math.Min(latestDataBucketStart, latestClosedBucketStart);
        return (earliestAllowedBucketStart, targetBucketStart);
    }

    private static long ResolveEarliestAllowedBucketStart(long latestClosedBucketStart, long everyNs, long forNs)
    {
        if (forNs <= 0)
            return latestClosedBucketStart;

        var backfillSpan = Math.Max(0, forNs - everyNs);
        return latestClosedBucketStart - backfillSpan;
    }

    internal static string InjectTimeWindow(string queryText, long bucketStartNs, long bucketEndExclusiveNs)
    {
        var trimmed = queryText.Trim().TrimEnd(';');
        var insertAt = FindFirstClauseIndex(trimmed);
        var head = insertAt >= 0 ? trimmed[..insertAt] : trimmed;
        var tail = insertAt >= 0 ? trimmed[insertAt..] : string.Empty;
        var condition = string.Format(CultureInfo.InvariantCulture, "time >= {0} AND time < {1}", bucketStartNs, bucketEndExclusiveNs);

        if (ContainsTopLevelWhere(head))
            return $"{head} AND {condition}{tail}";

        return $"{head} WHERE {condition}{tail}";
    }

    private static bool ContainsTopLevelWhere(string text) =>
        IndexOfTopLevelKeyword(text, " WHERE ") >= 0;

    private static int FindFirstClauseIndex(string text)
    {
        var candidates = new[]
        {
            IndexOfTopLevelKeyword(text, " GROUP BY "),
            IndexOfTopLevelKeyword(text, " ORDER BY "),
            IndexOfTopLevelKeyword(text, " LIMIT "),
            IndexOfTopLevelKeyword(text, " OFFSET "),
            IndexOfTopLevelKeyword(text, " SLIMIT "),
            IndexOfTopLevelKeyword(text, " SOFFSET "),
            IndexOfTopLevelKeyword(text, " FILL(")
        };
        return candidates.Where(i => i >= 0).DefaultIfEmpty(-1).Min();
    }

    private static int IndexOfTopLevelKeyword(string text, string keyword)
    {
        int depth = 0;
        bool inSingleQuote = false;
        bool inDoubleQuote = false;
        for (int i = 0; i <= text.Length - keyword.Length; i++)
        {
            var ch = text[i];
            if (ch == '\'' && !inDoubleQuote) inSingleQuote = !inSingleQuote;
            else if (ch == '"' && !inSingleQuote) inDoubleQuote = !inDoubleQuote;
            else if (!inSingleQuote && !inDoubleQuote)
            {
                if (ch == '(') depth++;
                else if (ch == ')') depth--;
            }

            if (depth == 0 && !inSingleQuote && !inDoubleQuote &&
                text.AsSpan(i, keyword.Length).Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;
    }
}

public sealed class ContinuousQueryHostedService : BackgroundService
{
    private readonly ContinuousQueryRunner _runner;
    private readonly MiniInfluxOptions _options;
    private readonly ILogger<ContinuousQueryHostedService> _logger;

    public ContinuousQueryHostedService(
        ContinuousQueryRunner runner,
        MiniInfluxOptions options,
        ILogger<ContinuousQueryHostedService> logger)
    {
        _runner = runner;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.ContinuousQuery.Enabled)
        {
            _logger.LogInformation("continuous query scheduler disabled by configuration");
            return;
        }

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(Math.Max(250, _options.ContinuousQuery.CheckIntervalMs)));
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _runner.ExecuteDueQueriesAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "continuous query scheduler cycle failed");
            }

            if (!await timer.WaitForNextTickAsync(stoppingToken))
                break;
        }
    }
}
