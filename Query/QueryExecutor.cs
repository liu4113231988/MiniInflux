using System.Text.RegularExpressions;
using System.Text.Json.Serialization;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Storage;

namespace MiniInflux.Net10.Query;

public sealed class QueryResponse
{
    public List<QueryResult> Results { get; set; } = [];
}

public sealed class QueryResult
{
    public int StatementId { get; set; }
    public List<QuerySeries>? Series { get; set; }
    public string? Error { get; set; }
}

public sealed class QuerySeries
{
    public string Name { get; set; } = "";
    public Dictionary<string, string>? Tags { get; set; }
    public List<string> Columns { get; set; } = [];
    public List<List<object?>> Values { get; set; } = [];
    [JsonIgnore]
    public HashSet<string>? TagColumns { get; set; }
}

public sealed class QueryExecutionReport
{
    public int ScannedPoints { get; set; }
    public int RowsReturned { get; set; }
    public long DurationMs { get; set; }
    public bool TimedOut { get; set; }
    public bool Canceled { get; set; }
    public bool UsedAggregatePushdown { get; set; }
    public bool UsedRegexPushdown { get; set; }
    public bool UsedSeriesIndexPushdown { get; set; }
    public long EstimatedInputBytes { get; set; }
    public long EstimatedResultBytes { get; set; }
    public long PeakEstimatedMemoryBytes { get; set; }
    public string? Error { get; set; }
}

public sealed class QueryExecutionOutcome
{
    public required QueryResponse Response { get; init; }
    public required QueryExecutionReport Report { get; init; }
}

public sealed class QueryChunkedExecutionOutcome
{
    public required IEnumerable<QueryResponse> Responses { get; init; }
    public required QueryExecutionReport Report { get; init; }
    public bool UsedStreamingRawSelect { get; init; }
}

public sealed class QueryExecutor
{
    private readonly int _maxResponseRows;
    private readonly int _maxQueryPoints;
    private readonly int _maxQueryDurationMs;
    private readonly long _maxQueryMemoryBytes;
    private readonly AuthStore? _authStore;

    public QueryExecutor(
        int maxResponseRows = 100_000,
        int maxQueryPoints = 1_000_000,
        int maxQueryDurationMs = 0,
        long maxQueryMemoryBytes = 0,
        AuthStore? authStore = null)
    {
        _maxResponseRows = maxResponseRows;
        _maxQueryPoints = maxQueryPoints;
        _maxQueryDurationMs = maxQueryDurationMs;
        _maxQueryMemoryBytes = maxQueryMemoryBytes;
        _authStore = authStore;
    }

    public Task<QueryResponse> ExecuteAsync(TsdbEngine e, string? db, string q, CancellationToken cancellationToken = default)
        => Task.FromResult(ExecuteWithReport(e, db, q, cancellationToken).Response);

    public QueryChunkedExecutionOutcome ExecuteChunkedWithReport(TsdbEngine e, string? db, string q, int chunkSize, CancellationToken cancellationToken = default)
    {
        var report = new QueryExecutionReport();
        var safeChunkSize = Math.Max(1, chunkSize);
        ParsedQuery? parsed = null;
        try { parsed = InfluxQlParser.Parse(q); } catch { }

        if (parsed != null && CanStreamRawSelect(parsed))
        {
            return new QueryChunkedExecutionOutcome
            {
                Responses = ExecuteStreamingRawSelect(e, db, parsed, safeChunkSize, report, cancellationToken),
                Report = report,
                UsedStreamingRawSelect = true
            };
        }

        return new QueryChunkedExecutionOutcome
        {
            Responses = ExecuteBufferedChunks(e, db, q, safeChunkSize, report, cancellationToken),
            Report = report,
            UsedStreamingRawSelect = false
        };
    }

    public QueryExecutionOutcome ExecuteWithReport(TsdbEngine e, string? db, string q, CancellationToken cancellationToken = default)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var report = new QueryExecutionReport();
        try
        {
            using var timeoutCts = _maxQueryDurationMs > 0 ? new CancellationTokenSource(_maxQueryDurationMs) : null;
            using var linkedCts = timeoutCts != null
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token)
                : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var token = linkedCts.Token;

            var response = new QueryResponse
            {
                Results = [new QueryResult { StatementId = 0, Series = Run(e, db, InfluxQlParser.Parse(q), token, report) }]
            };
            report.RowsReturned = CountRows(response);
            sw.Stop();
            report.DurationMs = sw.ElapsedMilliseconds;
            return new QueryExecutionOutcome { Response = response, Report = report };
        }
        catch (OperationCanceledException) when (_maxQueryDurationMs > 0 && !cancellationToken.IsCancellationRequested)
        {
            sw.Stop();
            report.DurationMs = sw.ElapsedMilliseconds;
            report.TimedOut = true;
            report.Error = $"query timed out after {_maxQueryDurationMs} ms";
            return new QueryExecutionOutcome
            {
                Response = new QueryResponse
                {
                    Results = [new QueryResult { StatementId = 0, Error = report.Error }]
                },
                Report = report
            };
        }
        catch (OperationCanceledException)
        {
            sw.Stop();
            report.DurationMs = sw.ElapsedMilliseconds;
            report.Canceled = true;
            report.Error = "query canceled";
            return new QueryExecutionOutcome
            {
                Response = new QueryResponse
                {
                    Results = [new QueryResult { StatementId = 0, Error = report.Error }]
                },
                Report = report
            };
        }
        catch (Exception ex)
        {
            sw.Stop();
            report.DurationMs = sw.ElapsedMilliseconds;
            report.Error = ex.Message;
            return new QueryExecutionOutcome
            {
                Response = new QueryResponse
                {
                    Results = [new QueryResult { StatementId = 0, Error = ex.Message }]
                },
                Report = report
            };
        }
    }

    IEnumerable<QueryResponse> ExecuteBufferedChunks(TsdbEngine e, string? db, string q, int chunkSize, QueryExecutionReport report, CancellationToken cancellationToken)
    {
        var outcome = ExecuteWithReport(e, db, q, cancellationToken);
        CopyReport(outcome.Report, report);
        foreach (var chunk in ChunkResponse(outcome.Response, chunkSize))
            yield return chunk;
    }

    IEnumerable<QueryResponse> ExecuteStreamingRawSelect(TsdbEngine e, string? db, ParsedQuery q, int chunkSize, QueryExecutionReport report, CancellationToken cancellationToken)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var timeoutCts = _maxQueryDurationMs > 0 ? new CancellationTokenSource(_maxQueryDurationMs) : null;
        using var linkedCts = timeoutCts != null
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token)
            : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var enumerator = StreamRawSelectChunks(e, db, q, chunkSize, report, linkedCts.Token).GetEnumerator();

        try
        {
            while (true)
            {
                QueryResponse? current = null;
                QueryResponse? error = null;
                var done = false;
                try
                {
                    if (enumerator.MoveNext())
                        current = enumerator.Current;
                    else
                        done = true;
                }
                catch (OperationCanceledException) when (_maxQueryDurationMs > 0 && !cancellationToken.IsCancellationRequested)
                {
                    report.TimedOut = true;
                    report.Error = $"query timed out after {_maxQueryDurationMs} ms";
                    error = ErrorChunk(report.Error);
                }
                catch (OperationCanceledException)
                {
                    report.Canceled = true;
                    report.Error = "query canceled";
                    error = ErrorChunk(report.Error);
                }
                catch (Exception ex)
                {
                    report.Error = ex.Message;
                    error = ErrorChunk(ex.Message);
                }

                if (error != null)
                {
                    yield return error;
                    yield break;
                }

                if (done)
                    yield break;

                yield return current!;
            }
        }
        finally
        {
            sw.Stop();
            report.DurationMs = sw.ElapsedMilliseconds;
        }
    }

    IEnumerable<QueryResponse> StreamRawSelectChunks(TsdbEngine e, string? db, ParsedQuery q, int chunkSize, QueryExecutionReport report, CancellationToken cancellationToken)
    {
        Req(q.SourceDatabase ?? db);
        var sourceDb = q.SourceDatabase ?? db!;
        var sourceRp = q.SourceRpName ?? e.GetDefaultRpName(sourceDb);
        var resultMeasurement = ResolveResultMeasurementName(q);
        var requestedFields = BuildRequestedFields(q);
        var seriesFilter = BuildSeriesFilter(e, sourceDb, q, report);
        var fields = ResolveRawFields(e, sourceDb, q, requestedFields);
        var tags = ResolveRawTags(e, sourceDb, q.Measurement);
        var cols = new List<string> { "time" };
        cols.AddRange(tags);
        cols.AddRange(fields);

        var offset = Math.Max(0, q.Offset ?? 0);
        var rowLimit = EffectiveRowLimit(q);
        var skipped = 0;
        var emitted = 0;
        var scanned = 0;
        var chunkRows = new List<List<object?>>(chunkSize);
        long chunkBytes = EstimateRawSeriesShellBytes(resultMeasurement, cols);

        foreach (var point in e.EnumeratePoints(sourceDb, sourceRp, q.Measurement, q.MinTimeNs, q.MaxTimeNs, requestedFields, seriesFilter, q.FieldFilters, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            scanned++;
            EnsureQueryPointLimit(scanned);
            report.ScannedPoints = scanned;
            report.EstimatedInputBytes += EstimatePointBytes(point);

            if (!MatchesTagFilters(point, q.TagFilters) || !MatchesFieldFilters(point, q.FieldFilters))
                continue;

            if (skipped < offset)
            {
                skipped++;
                continue;
            }

            if (rowLimit.HasValue && emitted >= rowLimit.Value)
                break;

            var row = BuildRawRow(point, tags, fields);
            chunkRows.Add(row);
            emitted++;
            report.RowsReturned = emitted;
            var rowBytes = 32 + row.Sum(EstimateObjectBytes);
            report.EstimatedResultBytes += rowBytes;
            chunkBytes += rowBytes;
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, EstimatePointBytes(point) + chunkBytes);
            EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);

            if (chunkRows.Count >= chunkSize)
            {
                yield return RawChunk(resultMeasurement, cols, chunkRows);
                chunkRows = new List<List<object?>>(chunkSize);
                chunkBytes = EstimateRawSeriesShellBytes(resultMeasurement, cols);
            }
        }

        if (chunkRows.Count > 0 || emitted == 0)
            yield return RawChunk(resultMeasurement, cols, chunkRows);
    }

    List<QuerySeries>? Run(TsdbEngine e, string? db, ParsedQuery q, CancellationToken cancellationToken, QueryExecutionReport report)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return q.Kind switch
        {
            QueryKind.CreateDatabase => CreateDatabase(e, q),
            QueryKind.ShowDatabases => [new() { Name = "databases", Columns = ["name"], Values = e.ListDatabases().Select(x => new List<object?> { x }).ToList() }],
            QueryKind.ShowMeasurements => ShowMeasurements(e, db),
            QueryKind.ShowFieldKeys => ShowFieldKeys(e, db, q),
            QueryKind.ShowTagKeys => ShowTagKeys(e, db, q),
            QueryKind.ShowTagValues => ShowTagValues(e, db, q),
            QueryKind.ShowSeries => ShowSeries(e, db, q),
            QueryKind.ShowSeriesCardinality => ShowSeriesCardinality(e, db, q),
            QueryKind.ShowMeasurementCardinality => ShowMeasurementCardinality(e, db),
            QueryKind.ShowTagValuesCardinality => ShowTagValuesCardinality(e, db, q),
            QueryKind.CreateRetentionPolicy => CreateRetentionPolicy(e, q),
            QueryKind.AlterRetentionPolicy => AlterRetentionPolicy(e, q),
            QueryKind.DropRetentionPolicy => DropRetentionPolicy(e, q),
            QueryKind.ShowRetentionPolicies => ShowRetentionPolicies(e, db, q),
            QueryKind.CreateContinuousQuery => CreateContinuousQuery(e, q),
            QueryKind.ShowContinuousQueries => ShowContinuousQueries(e, q),
            QueryKind.DropContinuousQuery => DropContinuousQuery(e, q),
            QueryKind.DropDatabase => DropDatabase(e, q),
            QueryKind.DropMeasurement => DropMeasurement(e, db, q),
            QueryKind.DropSeries => DropSeriesResult(e, db, q),
            QueryKind.DropShard => DropShard(e, q),
            QueryKind.Delete => DeleteResult(e, db, q),
            QueryKind.Select => Select(e, db, q, cancellationToken, report),
            QueryKind.CreateUser => CreateUser(q),
            QueryKind.GrantPrivilege => GrantPrivilege(q),
            QueryKind.RevokePrivilege => RevokePrivilege(q),
            QueryKind.ShowUsers => ShowUsers(),
            QueryKind.ShowGrants => ShowGrants(q),
            QueryKind.DropUser => DropUser(q),
            _ => throw new NotSupportedException()
        };
    }

    List<QuerySeries>? CreateDatabase(TsdbEngine e, ParsedQuery q)
    {
        e.CreateDatabase(q.Database!);
        return null;
    }

    List<QuerySeries>? ShowMeasurements(TsdbEngine e, string? db)
    {
        Req(db);
        return [new() { Name = "measurements", Columns = ["name"], Values = e.ListMeasurements(db!).Select(x => new List<object?> { x }).ToList() }];
    }

    List<QuerySeries>? ShowFieldKeys(TsdbEngine e, string? db, ParsedQuery q)
    {
        Req(db);
        return [new()
        {
            Name = q.Measurement ?? "fieldKeys",
            Columns = ["fieldKey", "fieldType"],
            Values = e.ListFieldKeys(db!, q.Measurement).Select(x => new List<object?> { x.Field, x.Kind.ToString().ToLowerInvariant() }).ToList()
        }];
    }

    List<QuerySeries>? ShowTagKeys(TsdbEngine e, string? db, ParsedQuery q)
    {
        Req(db);
        return [new() { Name = q.Measurement ?? "tagKeys", Columns = ["tagKey"], Values = e.ListTagKeys(db!, q.Measurement).Select(x => new List<object?> { x }).ToList() }];
    }

    List<QuerySeries>? ShowTagValues(TsdbEngine e, string? db, ParsedQuery q)
    {
        Req(db);
        return [new()
        {
            Name = q.Measurement ?? "tagValues",
            Columns = ["key", "value"],
            Values = e.ListTagValues(db!, q.Measurement, q.TagKey ?? "").Select(x => new List<object?> { x.Key, x.Value }).ToList()
        }];
    }

    List<QuerySeries>? ShowSeries(TsdbEngine e, string? db, ParsedQuery q)
    {
        Req(db);
        return [new()
        {
            Name = "series",
            Columns = ["key"],
            Values = e.ListSeries(db!, q.Measurement).Select(x => new List<object?> { FormatSeriesKey(q.Measurement, x) }).ToList()
        }];
    }

    List<QuerySeries>? ShowSeriesCardinality(TsdbEngine e, string? db, ParsedQuery q)
    {
        Req(db);
        return [new() { Name = "series cardinality", Columns = ["count"], Values = [new List<object?> { e.ListSeries(db!, q.Measurement).Count }] }];
    }

    List<QuerySeries>? ShowMeasurementCardinality(TsdbEngine e, string? db)
    {
        Req(db);
        return [new() { Name = "measurement cardinality", Columns = ["count"], Values = [new List<object?> { e.GetMeasurementCardinality(db!) }] }];
    }

    List<QuerySeries>? ShowTagValuesCardinality(TsdbEngine e, string? db, ParsedQuery q)
    {
        Req(db);
        return [new() { Name = "tag values cardinality", Columns = ["count"], Values = [new List<object?> { e.GetTagValueCardinality(db!, q.Measurement, q.TagKey) }] }];
    }

    List<QuerySeries>? CreateRetentionPolicy(TsdbEngine e, ParsedQuery q)
    {
        Req(q.Database);
        e.CreateDatabase(q.Database!);
        e.Meta.CreateRetentionPolicy(q.Database!, q.RpName!, q.RpDurationNs ?? 0, q.RpDefault ?? false);
        return null;
    }

    List<QuerySeries>? AlterRetentionPolicy(TsdbEngine e, ParsedQuery q)
    {
        Req(q.Database);
        e.Meta.AlterRetentionPolicy(q.Database!, q.RpName!, q.RpDurationNs, q.RpDefault);
        return null;
    }

    List<QuerySeries>? DropRetentionPolicy(TsdbEngine e, ParsedQuery q)
    {
        Req(q.Database);
        e.Meta.DropRetentionPolicy(q.Database!, q.RpName!);
        return null;
    }

    List<QuerySeries>? ShowRetentionPolicies(TsdbEngine e, string? db, ParsedQuery q)
    {
        Req(q.Database ?? db);
        var rpList = e.Meta.ListRetentionPolicies(q.Database ?? db!);
        return [new()
        {
            Name = "retention policies",
            Columns = ["name", "duration", "replicaN", "default"],
            Values = rpList.Select(r => new List<object?> { r.Name, FormatDuration(r.DurationNs), r.Replication, r.IsDefault }).ToList()
        }];
    }

    List<QuerySeries>? CreateContinuousQuery(TsdbEngine e, ParsedQuery q)
    {
        Req(q.Database);
        Req(q.ContinuousQueryName);
        Req(q.ContinuousQueryText);

        var parsedSelect = InfluxQlParser.Parse(q.ContinuousQueryText!);
        if (parsedSelect.Kind != QueryKind.Select)
            throw new NotSupportedException("continuous query body must be a SELECT statement");
        if (string.IsNullOrWhiteSpace(parsedSelect.IntoTarget))
            throw new NotSupportedException("continuous query requires SELECT ... INTO ...");
        if (!parsedSelect.GroupByNs.HasValue)
            throw new NotSupportedException("continuous query requires GROUP BY time(...)");

        var cadence = q.ContinuousQueryEveryNs ?? parsedSelect.GroupByNs.Value;
        if (cadence <= 0)
            throw new NotSupportedException("continuous query cadence must be positive");

        e.CreateDatabase(q.Database!);
        e.Meta.SaveContinuousQuery(q.Database!, new ContinuousQueryInfo
        {
            Name = q.ContinuousQueryName!,
            Database = q.Database!,
            QueryText = q.ContinuousQueryText!,
            EveryNs = cadence,
            ForNs = q.ContinuousQueryForNs ?? 0,
            RecomputeRecentBuckets = q.ContinuousQueryRecomputeRecentBuckets ?? 0
        });
        return null;
    }

    List<QuerySeries>? ShowContinuousQueries(TsdbEngine e, ParsedQuery q)
    {
        var rows = e.Meta.ListContinuousQueries(q.Database)
            .Select(cq => new List<object?>
            {
                cq.Database,
                cq.Name,
                cq.QueryText,
                FormatDuration(cq.EveryNs),
                cq.ForNs > 0 ? FormatDuration(cq.ForNs) : null,
                cq.RecomputeRecentBuckets > 0 ? cq.RecomputeRecentBuckets : null,
                cq.LastCompletedBucketStartNs == long.MinValue ? null : Time(cq.LastCompletedBucketStartNs)
            })
            .ToList();
        return [new()
        {
            Name = "continuous queries",
            Columns = ["database", "name", "query", "every", "for", "recomputeRecentBuckets", "lastRun"],
            Values = rows
        }];
    }

    List<QuerySeries>? DropContinuousQuery(TsdbEngine e, ParsedQuery q)
    {
        Req(q.Database);
        Req(q.ContinuousQueryName);
        e.Meta.RemoveContinuousQuery(q.Database!, q.ContinuousQueryName!);
        return null;
    }

    List<QuerySeries>? DropDatabase(TsdbEngine e, ParsedQuery q)
    {
        Req(q.Database);
        e.DropDatabase(q.Database!);
        return null;
    }

    List<QuerySeries>? DropMeasurement(TsdbEngine e, string? db, ParsedQuery q)
    {
        Req(db);
        e.DropMeasurement(db!, q.Measurement!);
        return null;
    }

    List<QuerySeries>? DropSeriesResult(TsdbEngine e, string? db, ParsedQuery q)
    {
        Req(db);
        DropSeries(e, db!, q);
        return null;
    }

    List<QuerySeries>? DropShard(TsdbEngine e, ParsedQuery q)
    {
        e.DropShard(q.Limit ?? 0);
        return null;
    }

    List<QuerySeries>? DeleteResult(TsdbEngine e, string? db, ParsedQuery q)
    {
        Req(db);
        Delete(e, db!, q);
        return null;
    }

    List<QuerySeries>? CreateUser(ParsedQuery q)
    {
        RequireAuthStore().CreateUser(q.UserName!, q.Password!, q.IsAdmin ?? false);
        return null;
    }

    List<QuerySeries>? GrantPrivilege(ParsedQuery q)
    {
        var grant = q.Grant ?? throw new InvalidOperationException("missing grant");
        RequireAuthStore().Grant(grant.UserName, grant.Database, grant.Privilege);
        return null;
    }

    List<QuerySeries>? RevokePrivilege(ParsedQuery q)
    {
        var grant = q.Grant ?? throw new InvalidOperationException("missing revoke");
        RequireAuthStore().Revoke(grant.UserName, grant.Database, grant.Privilege);
        return null;
    }

    List<QuerySeries>? ShowUsers()
    {
        var users = RequireAuthStore().ListUsers();
        return [new()
        {
            Name = "users",
            Columns = ["user", "admin", "grants"],
            Values = users.Select(u => new List<object?> { u.UserName, u.IsAdmin, FormatGrants(u.Grants) }).ToList()
        }];
    }

    List<QuerySeries>? ShowGrants(ParsedQuery q)
    {
        var user = RequireAuthStore().Find(q.UserName!) ?? throw new InvalidOperationException($"user not found: {q.UserName}");
        return [new()
        {
            Name = "grants",
            Columns = ["user", "scope", "privilege"],
            Values = user.Grants.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => new List<object?> { user.UserName, kv.Key, kv.Value })
                .ToList()
        }];
    }

    List<QuerySeries>? DropUser(ParsedQuery q)
    {
        RequireAuthStore().DropUser(q.UserName!);
        return null;
    }

    List<QuerySeries> Select(TsdbEngine e, string? db, ParsedQuery q, CancellationToken cancellationToken, QueryExecutionReport report)
    {
        Req(q.SourceDatabase ?? db);
        var sourceDb = q.SourceDatabase ?? db!;
        var sourceRp = q.SourceRpName ?? e.GetDefaultRpName(sourceDb);
        var resultMeasurement = ResolveResultMeasurementName(q);
        HashSet<string>? requestedFields = null;
        if (q.Select.Count > 0 && !(q.Select.Count == 1 && q.Select[0].Field == "*"))
            requestedFields = new HashSet<string>(q.Select.Select(x => x.Field), StringComparer.Ordinal);
        if (requestedFields != null)
            foreach (var filter in q.FieldFilters)
                requestedFields.Add(filter.Field);

        List<Point> pts;
        if (q.Subquery != null)
        {
            var innerSeries = Run(e, db, q.Subquery, cancellationToken, report) ?? [];
            pts = MaterializeSubqueryPoints(innerSeries);
            EnsureQueryPointLimit(pts.Count);
            var subqueryBytes = EstimatePointsBytes(pts);
            report.EstimatedInputBytes = Math.Max(report.EstimatedInputBytes, subqueryBytes);
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, subqueryBytes);
            EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
        }
        else
        {
            var seriesFilter = BuildSeriesFilter(e, sourceDb, q, report);
            if (!q.GroupByNs.HasValue && q.Select.Any(s => s.Func != ""))
            {
                var pushedDown = q.GroupByTags.Count == 0
                    ? TrySelectFunctionsWithPushdown(e, sourceDb, sourceRp, q, requestedFields, seriesFilter, cancellationToken, report)
                    : TryGroupByTagFunctionsWithPushdown(e, sourceDb, sourceRp, q, requestedFields, seriesFilter, cancellationToken, report, resultMeasurement);
                if (pushedDown != null)
                {
                    var resultBytes = EstimateQuerySeriesBytes(pushedDown);
                    report.EstimatedResultBytes = resultBytes;
                    report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, resultBytes);
                    EnsureQueryMemoryLimit(resultBytes);
                    if (!string.IsNullOrWhiteSpace(q.IntoTarget))
                        ExecuteSelectInto(e, sourceDb, q, pushedDown);
                    return pushedDown;
                }
            }

            pts = e.ReadAllPoints(sourceDb, sourceRp, q.Measurement, q.MinTimeNs, q.MaxTimeNs, requestedFields, seriesFilter, q.FieldFilters, cancellationToken);
            EnsureQueryPointLimit(pts.Count);
            report.ScannedPoints += pts.Count;
            report.EstimatedInputBytes = EstimatePointsBytes(pts);
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, report.EstimatedInputBytes);
            EnsureQueryMemoryLimit(report.EstimatedInputBytes);
        }

        cancellationToken.ThrowIfCancellationRequested();
        pts = ApplyTagFilters(pts, q.TagFilters);
        cancellationToken.ThrowIfCancellationRequested();
        pts = ApplyFieldFilters(pts, q.FieldFilters);
        if (q.Desc) pts = pts.OrderByDescending(x => x.TimestampNs).ToList();

        if (q.GroupByNs.HasValue || q.GroupByTags.Count > 0)
        {
            var aggregateResult = AggGroupBy(pts, q, cancellationToken, resultMeasurement);
            report.EstimatedResultBytes = EstimateQuerySeriesBytes(aggregateResult);
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, report.EstimatedInputBytes + report.EstimatedResultBytes);
            EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
            if (!string.IsNullOrWhiteSpace(q.IntoTarget))
                ExecuteSelectInto(e, sourceDb, q, aggregateResult);
            return aggregateResult;
        }

        if (q.Select.Any(s => s.Func != ""))
        {
            var functionResult = SelectFunctions(pts, q, cancellationToken, resultMeasurement);
            report.EstimatedResultBytes = EstimateQuerySeriesBytes(functionResult);
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, report.EstimatedInputBytes + report.EstimatedResultBytes);
            EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
            if (!string.IsNullOrWhiteSpace(q.IntoTarget))
                ExecuteSelectInto(e, sourceDb, q, functionResult);
            return functionResult;
        }

        var rowLimit = Math.Min(q.Limit ?? _maxResponseRows, _maxResponseRows);
        pts = pts.Skip(q.Offset ?? 0).Take(rowLimit).ToList();

        var fields = q.Select.Count == 1 && q.Select[0].Field == "*"
            ? pts.SelectMany(p => p.Fields.Keys).Distinct().Order().ToList()
            : q.Select.Select(x => x.Field).ToList();
        var tags = pts.SelectMany(p => p.Tags.Keys).Distinct().Order().ToList();
        var cols = new List<string> { "time" };
        cols.AddRange(tags);
        cols.AddRange(fields);

        var vals = new List<List<object?>>();
        foreach (var p in pts)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var row = new List<object?> { Time(p.TimestampNs) };
            foreach (var t in tags) row.Add(p.Tags.TryGetValue(t, out var v) ? v : null);
            foreach (var f in fields) row.Add(p.Fields.TryGetValue(f, out var v) ? v.ToObject() : null);
            vals.Add(row);
        }

        EnsureWithinLimit(vals.Count);
        List<QuerySeries> rawResult =
        [
            new()
            {
                Name = resultMeasurement,
                Columns = cols,
                Values = vals,
                TagColumns = new HashSet<string>(tags, StringComparer.Ordinal)
            }
        ];
        report.EstimatedResultBytes = EstimateQuerySeriesBytes(rawResult);
        report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, report.EstimatedInputBytes + report.EstimatedResultBytes);
        EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
        if (!string.IsNullOrWhiteSpace(q.IntoTarget))
            ExecuteSelectInto(e, sourceDb, q, rawResult);
        return rawResult;
    }

    List<QuerySeries> AggGroupBy(List<Point> pts, ParsedQuery q, CancellationToken cancellationToken, string resultMeasurement)
    {
        long? step = q.GroupByNs;
        var tagNames = q.GroupByTags;
        var items = q.Select.Where(x => x.Func != "").ToList();
        if (items.Count == 0) items = [new("count", "*", "count")];
        var hasRowExpandingFunctions = items.Any(IsRowExpandingGroupFunction);

        var groups = new Dictionary<(string TagKey, long? BucketTime), List<Point>>();
        foreach (var p in pts)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var tagVals = tagNames.Select(tn => p.Tags.TryGetValue(tn, out var v) ? v : "").ToArray();
            var tagKey = string.Join("|", tagVals);
            long? bucketTime = step.HasValue ? p.TimestampNs / step.Value * step.Value : null;
            var key = (tagKey, bucketTime);
            if (!groups.TryGetValue(key, out var list))
            {
                list = [];
                groups[key] = list;
            }
            list.Add(p);
        }

        var seriesMap = new Dictionary<string, QuerySeries>();
        foreach (var (key, groupPts) in groups.OrderBy(g => g.Key.BucketTime ?? 0))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var tagVals = key.TagKey.Split('|');
            var tagsDict = tagNames.Count > 0
                ? tagNames.Zip(tagVals, (n, v) => (n, v)).ToDictionary(x => x.n, x => x.v)
                : null;
            var seriesKey = key.TagKey;
            if (!seriesMap.TryGetValue(seriesKey, out var series))
            {
                var cols = new List<string> { "time" };
                cols.AddRange(items.Select(x => x.Alias));
                series = new QuerySeries { Name = resultMeasurement, Tags = tagsDict, Columns = cols, Values = [] };
                seriesMap[seriesKey] = series;
            }

            foreach (var row in BuildGroupedRows(groupPts, items, key.BucketTime, cancellationToken))
                series.Values.Add(row);
        }

        if (!hasRowExpandingFunctions && step.HasValue && q.Fill != FillMode.None && q.MinTimeNs.HasValue && q.MaxTimeNs.HasValue)
            ApplyFill(seriesMap, q, step.Value, items);

        var rowLimit = Math.Min(q.Limit ?? _maxResponseRows, _maxResponseRows);
        foreach (var s in seriesMap.Values)
        {
            s.Values = OrderRowsByTime(s.Values, q.Desc);
            s.Values = s.Values.Skip(q.Offset ?? 0).Take(rowLimit).ToList();
        }

        var seriesList = ApplySeriesWindow(seriesMap.Values, q.SeriesOffset, q.SeriesLimit);
        EnsureWithinLimit(seriesList.Sum(s => s.Values.Count));
        return seriesList;
    }

    static List<List<object?>> BuildGroupedRows(List<Point> groupPts, List<SelectItem> items, long? bucketTimeNs, CancellationToken cancellationToken)
    {
        if (!items.Any(IsRowExpandingGroupFunction))
        {
            var row = new List<object?> { Time(bucketTimeNs ?? 0) };
            foreach (var it in items)
            {
                var pairs = BuildValuePairs(groupPts, it.Field);
                var groupValues = pairs.Select(p => p.Value).ToList();
                var groupTimestamps = pairs.Select(p => p.TimestampNs).ToList();
                row.Add(Calc(groupValues, it.Func, it.Param, groupTimestamps, it.UnitNs));
            }
            return [row];
        }

        var timelines = new List<SortedDictionary<long, object?>>(items.Count);
        foreach (var item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (IsRowExpandingGroupFunction(item))
            {
                timelines.Add(BuildFunctionSeries(groupPts, item));
                continue;
            }

            var pairs = BuildValuePairs(groupPts, item.Field);
            var values = pairs.Select(p => p.Value).ToList();
            var timestamps = pairs.Select(p => p.TimestampNs).ToList();
            var scalar = Calc(values, item.Func, item.Param, timestamps, item.UnitNs);
            var timeline = new SortedDictionary<long, object?>();
            if (bucketTimeNs.HasValue)
                timeline[bucketTimeNs.Value] = scalar;
            timelines.Add(timeline);
        }

        var allTimes = timelines.SelectMany(t => t.Keys).Distinct().Order().ToList();
        if (allTimes.Count == 0)
        {
            var fallbackTime = bucketTimeNs ?? 0;
            var row = new List<object?> { Time(fallbackTime) };
            foreach (var timeline in timelines)
                row.Add(timeline.TryGetValue(fallbackTime, out var value) ? value : null);
            return [row];
        }

        var rows = new List<List<object?>>();
        foreach (var time in allTimes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var row = new List<object?> { Time(time) };
            foreach (var timeline in timelines)
                row.Add(timeline.TryGetValue(time, out var value) ? value : null);
            rows.Add(row);
        }
        return rows;
    }

    static bool IsRowExpandingGroupFunction(SelectItem item) => item.Func is "top" or "bottom" or "sample";

    HashSet<string>? BuildSeriesFilter(TsdbEngine e, string db, ParsedQuery q, QueryExecutionReport report)
    {
        if (string.IsNullOrWhiteSpace(q.Measurement) || q.TagFilters.Count == 0)
            return null;

        HashSet<string>? candidates = null;
        foreach (var filter in q.TagFilters)
        {
            IReadOnlyList<string>? matches = filter.Op switch
            {
                TagOp.Eq => e.GetSeriesForTagValue(db, q.Measurement, filter.Key, filter.Value),
                TagOp.Neq => e.GetSeriesForTagKey(db, q.Measurement, filter.Key)
                    .Except(e.GetSeriesForTagValue(db, q.Measurement, filter.Key, filter.Value), StringComparer.Ordinal)
                    .ToArray(),
                TagOp.Regex => e.GetSeriesForTagRegex(db, q.Measurement, filter.Key, filter.Value),
                TagOp.NotRegex => e.GetSeriesForTagRegex(db, q.Measurement, filter.Key, filter.Value, negate: true),
                _ => null
            };

            if (matches == null) continue;
            report.UsedSeriesIndexPushdown = true;
            if (filter.Op is TagOp.Regex or TagOp.NotRegex) report.UsedRegexPushdown = true;

            var set = new HashSet<string>(matches, StringComparer.Ordinal);
            candidates = candidates == null
                ? set
                : new HashSet<string>(candidates.Intersect(set, StringComparer.Ordinal), StringComparer.Ordinal);
        }

        return candidates;
    }

    void ExecuteSelectInto(TsdbEngine e, string defaultDb, ParsedQuery q, List<QuerySeries> seriesList)
    {
        var target = ParseIntoTarget(defaultDb, q.IntoTarget!, q.Measurement ?? "result");
        var knownFields = new HashSet<string>(e.ListFieldKeys(q.SourceDatabase ?? defaultDb, q.Measurement).Select(f => f.Field), StringComparer.OrdinalIgnoreCase);
        var points = ConvertSeriesToPoints(target.Measurement, seriesList, q, knownFields);
        if (points.Count == 0)
            return;
        e.WriteAsync(target.Database, target.RetentionPolicy, points).GetAwaiter().GetResult();
    }

    static IntoTarget ParseIntoTarget(string defaultDb, string rawTarget, string fallbackMeasurement)
    {
        var parts = InfluxQlParser.SplitQualifiedIdentifier(rawTarget);
        return parts.Length switch
        {
            1 => new IntoTarget(defaultDb, "autogen", parts[0]),
            2 => new IntoTarget(parts[0], "autogen", parts[1]),
            3 => new IntoTarget(parts[0], parts[1], parts[2]),
            _ => new IntoTarget(defaultDb, "autogen", string.IsNullOrWhiteSpace(rawTarget) ? fallbackMeasurement : UnqTarget(rawTarget))
        };
    }

    static string UnqTarget(string value)
    {
        value = value.Trim();
        return value.Length >= 2 && value[0] == '"' && value[^1] == '"' ? value[1..^1] : value;
    }

    static List<Point> ConvertSeriesToPoints(string measurement, List<QuerySeries> seriesList, ParsedQuery query, HashSet<string> knownFields)
    {
        var selectedColumns = new HashSet<string>(
            query.Select.Select(s => s.Func == "" ? s.Field : s.Alias),
            StringComparer.OrdinalIgnoreCase);

        var points = new List<Point>();
        foreach (var series in seriesList)
        {
            var tagKeys = new HashSet<string>(series.Tags?.Keys ?? Enumerable.Empty<string>(), StringComparer.Ordinal);
            if (series.TagColumns != null)
                tagKeys.UnionWith(series.TagColumns);
            foreach (var row in series.Values)
            {
                if (row.Count == 0)
                    continue;
                var timeIndex = IndexOfColumn(series.Columns, "time");
                var timestampNs = timeIndex >= 0 ? ParseTimeNs(row[timeIndex]) : 0;
                var tags = series.Tags == null
                    ? new Dictionary<string, string>(StringComparer.Ordinal)
                    : new Dictionary<string, string>(series.Tags, StringComparer.Ordinal);
                var fields = new Dictionary<string, FieldValue>(StringComparer.Ordinal);
                for (int i = 0; i < series.Columns.Count && i < row.Count; i++)
                {
                    var column = series.Columns[i];
                    if (string.Equals(column, "time", StringComparison.OrdinalIgnoreCase))
                        continue;

                    var value = row[i];
                    if (value == null)
                        continue;

                    if (tagKeys.Contains(column))
                    {
                        tags[column] = value.ToString() ?? "";
                        continue;
                    }

                    if (ShouldTreatAsTag(query, selectedColumns, knownFields, column, value))
                    {
                        tags[column] = value.ToString() ?? "";
                        continue;
                    }

                    fields[column] = ToFieldValue(value);
                }

                if (fields.Count == 0)
                    continue;

                points.Add(new Point
                {
                    Measurement = measurement,
                    Tags = tags,
                    Fields = fields,
                    TimestampNs = timestampNs
                });
            }
        }
        return points;
    }

    static List<Point> MaterializeSubqueryPoints(List<QuerySeries> seriesList)
    {
        var points = new List<Point>();
        foreach (var series in seriesList)
        {
            var timeIndex = IndexOfColumn(series.Columns, "time");
            var seriesTags = series.Tags == null
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                : new Dictionary<string, string>(series.Tags, StringComparer.Ordinal);
            var tagColumns = new HashSet<string>(seriesTags.Keys, StringComparer.Ordinal);
            if (series.TagColumns != null)
                tagColumns.UnionWith(series.TagColumns);

            foreach (var row in series.Values)
            {
                var timestampNs = timeIndex >= 0 && timeIndex < row.Count ? ParseTimeNs(row[timeIndex]) : 0;
                var tags = new Dictionary<string, string>(seriesTags, StringComparer.Ordinal);
                var fields = new Dictionary<string, FieldValue>(StringComparer.Ordinal);
                for (int i = 0; i < series.Columns.Count && i < row.Count; i++)
                {
                    var column = series.Columns[i];
                    if (string.Equals(column, "time", StringComparison.OrdinalIgnoreCase))
                        continue;

                    var value = row[i];
                    if (value == null)
                        continue;

                    if (tagColumns.Contains(column))
                    {
                        tags[column] = value.ToString() ?? "";
                        continue;
                    }

                    fields[column] = ToFieldValue(value);
                }

                if (fields.Count == 0)
                    continue;

                points.Add(new Point
                {
                    Measurement = series.Name,
                    Tags = tags,
                    Fields = fields,
                    TimestampNs = timestampNs
                });
            }
        }

        return points.OrderBy(p => p.TimestampNs).ToList();
    }

    static bool ShouldTreatAsTag(ParsedQuery query, HashSet<string> selectedColumns, HashSet<string> knownFields, string column, object value)
    {
        if (query.Select.Any(s => s.Func != ""))
            return false;
        if (knownFields.Contains(column))
            return false;
        if (selectedColumns.Contains(column) && value is not string)
            return false;
        if (query.Select.Count == 1 && query.Select[0].Field == "*" && value is not string)
            return false;
        return value is string;
    }

    static int IndexOfColumn(List<string> columns, string name)
    {
        for (int i = 0; i < columns.Count; i++)
            if (string.Equals(columns[i], name, StringComparison.OrdinalIgnoreCase))
                return i;
        return -1;
    }

    static FieldValue ToFieldValue(object value) => value switch
    {
        FieldValue fv => fv,
        string s => FieldValue.FromString(s),
        bool b => FieldValue.FromBoolean(b),
        byte or sbyte or short or ushort or int or uint or long => FieldValue.FromInteger(Convert.ToInt64(value)),
        float or double or decimal => FieldValue.FromDouble(Convert.ToDouble(value)),
        _ => FieldValue.FromString(value.ToString() ?? "")
    };

    void EnsureWithinLimit(int rowCount)
    {
        if (_maxResponseRows > 0 && rowCount > _maxResponseRows)
            throw new InvalidOperationException($"query response row limit exceeded: {rowCount} > {_maxResponseRows}");
    }

    void EnsureQueryPointLimit(int pointCount)
    {
        if (_maxQueryPoints > 0 && pointCount > _maxQueryPoints)
            throw new InvalidOperationException($"query point limit exceeded: {pointCount} > {_maxQueryPoints}");
    }

    void EnsureQueryMemoryLimit(long bytes)
    {
        if (_maxQueryMemoryBytes > 0 && bytes > _maxQueryMemoryBytes)
            throw new InvalidOperationException($"query memory limit exceeded: {bytes} > {_maxQueryMemoryBytes}");
    }

    static long EstimatePointsBytes(List<Point> points) => points.Sum(EstimatePointBytes);

    static long EstimatePointBytes(Point point)
    {
        long size = 96 + EstimateStringBytes(point.Measurement) + 8;
        foreach (var tag in point.Tags)
            size += 32 + EstimateStringBytes(tag.Key) + EstimateStringBytes(tag.Value);
        foreach (var field in point.Fields)
            size += 48 + EstimateStringBytes(field.Key) + EstimateFieldValueBytes(field.Value);
        return size;
    }

    static long EstimateQuerySeriesBytes(List<QuerySeries> seriesList)
    {
        long size = 0;
        foreach (var series in seriesList)
        {
            size += 96 + EstimateStringBytes(series.Name);
            if (series.Tags != null)
                foreach (var tag in series.Tags)
                    size += 32 + EstimateStringBytes(tag.Key) + EstimateStringBytes(tag.Value);
            foreach (var col in series.Columns)
                size += 16 + EstimateStringBytes(col);
            foreach (var row in series.Values)
                size += 32 + row.Sum(EstimateObjectBytes);
        }
        return size;
    }

    static long EstimateFieldValueBytes(FieldValue value) => value.Kind switch
    {
        FieldKind.String => 24 + EstimateStringBytes(value.String),
        _ => 16
    };

    static long EstimateObjectBytes(object? value) => value switch
    {
        null => 0,
        string s => 24 + EstimateStringBytes(s),
        bool => 8,
        byte or sbyte or short or ushort or int or uint or long or ulong => 8,
        float or double or decimal => 8,
        _ => 24 + EstimateStringBytes(value.ToString())
    };

    static long EstimateStringBytes(string? value) => string.IsNullOrEmpty(value) ? 0 : 24 + value.Length * 2L;

    static void ApplyFill(Dictionary<string, QuerySeries> seriesMap, ParsedQuery q, long step, List<SelectItem> items)
    {
        var minBucket = q.MinTimeNs!.Value / step * step;
        var maxBucket = q.MaxTimeNs!.Value / step * step;
        foreach (var series in seriesMap.Values)
        {
            var timeSet = new HashSet<long>();
            foreach (var row in series.Values)
            {
                if (row[0] is string ts && DateTimeOffset.TryParse(ts, out var dto))
                    timeSet.Add(dto.ToUnixTimeMilliseconds() * 1_000_000);
            }
            var filledRows = new List<List<object?>>();
            var prevValues = new object?[items.Count];
            var original = series.Values.ToDictionary(row => ParseTimeNs(row[0]), row => row);
            for (long t = minBucket; t <= maxBucket; t += step)
            {
                if (timeSet.Contains(t))
                {
                    if (original.TryGetValue(t, out var existing))
                    {
                        filledRows.Add(existing);
                        for (int i = 0; i < items.Count; i++)
                            if (existing.Count > i + 1) prevValues[i] = existing[i + 1];
                        continue;
                    }
                }
                var row = new List<object?> { Time(t) };
                for (int i = 0; i < items.Count; i++)
                {
                    row.Add(q.Fill switch
                    {
                        FillMode.Zero => 0,
                        FillMode.Previous => prevValues[i],
                        FillMode.Linear => LinearFill(original, t, i + 1),
                        _ => null
                    });
                }
                filledRows.Add(row);
            }
            series.Values = filledRows.OrderBy(r => r[0]?.ToString()).ToList();
        }
    }

    static List<Point> ApplyTagFilters(List<Point> pts, List<TagFilter> filters)
    {
        if (filters.Count == 0) return pts;
        return pts.Where(p =>
        {
            foreach (var f in filters)
            {
                var tagVal = p.Tags.TryGetValue(f.Key, out var v) ? v : null;
                switch (f.Op)
                {
                    case TagOp.Eq: if (tagVal != f.Value) return false; break;
                    case TagOp.Neq: if (tagVal == f.Value) return false; break;
                    case TagOp.Regex: if (tagVal == null || !Regex.IsMatch(tagVal, f.Value)) return false; break;
                    case TagOp.NotRegex: if (tagVal != null && Regex.IsMatch(tagVal, f.Value)) return false; break;
                }
            }
            return true;
        }).ToList();
    }

    static List<Point> ApplyFieldFilters(List<Point> pts, List<FieldFilter> filters)
    {
        if (filters.Count == 0) return pts;
        return pts.Where(p =>
        {
            foreach (var f in filters)
            {
                if (!p.Fields.TryGetValue(f.Field, out var fv)) return false;
                var numVal = fv.AsDouble();
                if (!numVal.HasValue) return false;
                var ok = f.Op switch
                {
                    FieldOp.Eq => Math.Abs(numVal.Value - f.Value) < 1e-9,
                    FieldOp.Neq => Math.Abs(numVal.Value - f.Value) >= 1e-9,
                    FieldOp.Gt => numVal.Value > f.Value,
                    FieldOp.Gte => numVal.Value >= f.Value,
                    FieldOp.Lt => numVal.Value < f.Value,
                    FieldOp.Lte => numVal.Value <= f.Value,
                    _ => false
                };
                if (!ok) return false;
            }
            return true;
        }).ToList();
    }

    static void Delete(TsdbEngine e, string db, ParsedQuery q)
    {
        if (q.TagFilters.Count == 0 && q.FieldFilters.Count == 0)
        {
            e.DeleteFromMeasurement(db, q.Measurement!, q.MinTimeNs, q.MaxTimeNs);
            return;
        }

        e.DeleteFromMeasurement(db, q.Measurement!, q.MinTimeNs, q.MaxTimeNs, p =>
            MatchesTagFilters(p, q.TagFilters) && MatchesFieldFilters(p, q.FieldFilters));
    }

    static void DropSeries(TsdbEngine e, string db, ParsedQuery q)
    {
        var measurements = q.Measurement != null ? new[] { q.Measurement } : e.ListMeasurements(db);
        foreach (var measurement in measurements)
        {
            var points = e.Meta.ListRetentionPolicies(db)
                .Select(r => r.Name)
                .DefaultIfEmpty(e.GetDefaultRpName(db))
                .SelectMany(rp => e.ReadAllPoints(db, rp, measurement, q.MinTimeNs, q.MaxTimeNs, fieldFilters: q.FieldFilters))
                .ToList();
            points = ApplyTagFilters(points, q.TagFilters);
            points = ApplyFieldFilters(points, q.FieldFilters);
            var series = points.Select(p => SeriesKey.From(p).TagsCanonical).Distinct(StringComparer.Ordinal).ToList();
            if (q.TagFilters.Count == 0 && q.FieldFilters.Count == 0 && series.Count == 0)
                series = e.ListSeries(db, measurement).ToList();
            e.DropSeries(db, measurement, series);
        }
    }

    List<QuerySeries> SelectFunctions(List<Point> pts, ParsedQuery q, CancellationToken cancellationToken, string resultMeasurement)
    {
        var series = new QuerySeries
        {
            Name = resultMeasurement,
            Columns = ["time"]
        };
        series.Columns.AddRange(q.Select.Select(s => s.Alias));

        var rows = BuildFunctionRows(pts, q.Select.Where(s => s.Func != "").ToList(), cancellationToken);
        rows = OrderRowsByTime(rows, q.Desc);
        rows = rows.Skip(q.Offset ?? 0).Take(Math.Min(q.Limit ?? _maxResponseRows, _maxResponseRows)).ToList();
        EnsureWithinLimit(rows.Count);
        series.Values = rows;
        return [series];
    }

    static List<List<object?>> BuildFunctionRows(List<Point> pts, List<SelectItem> items, CancellationToken cancellationToken)
    {
        if (items.Count == 0) return [];
        var timelines = items.Select(item => BuildFunctionSeries(pts, item)).ToList();
        var allTimes = timelines.SelectMany(t => t.Keys).Distinct().Order().ToList();
        var rows = new List<List<object?>>();
        foreach (var time in allTimes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var row = new List<object?> { Time(time) };
            foreach (var timeline in timelines)
                row.Add(timeline.TryGetValue(time, out var value) ? value : null);
            rows.Add(row);
        }
        return rows;
    }

    static SortedDictionary<long, object?> BuildFunctionSeries(List<Point> pts, SelectItem item)
    {
        var values = pts
            .Where(p => p.Fields.TryGetValue(item.Field, out var v) && v.AsDouble().HasValue)
            .Select(p => (p.TimestampNs, Value: p.Fields[item.Field].AsDouble()!.Value))
            .OrderBy(p => p.TimestampNs)
            .ToList();

        var result = new SortedDictionary<long, object?>();
        if (values.Count == 0) return result;

        switch (item.Func)
        {
            case "difference":
                for (int i = 1; i < values.Count; i++) result[values[i].TimestampNs] = values[i].Value - values[i - 1].Value;
                break;
            case "derivative":
            case "non_negative_derivative":
                var unitNs = item.UnitNs ?? 1_000_000_000L;
                for (int i = 1; i < values.Count; i++)
                {
                    var deltaNs = values[i].TimestampNs - values[i - 1].TimestampNs;
                    if (deltaNs <= 0) continue;
                    var rate = (values[i].Value - values[i - 1].Value) * unitNs / deltaNs;
                    result[values[i].TimestampNs] = item.Func == "non_negative_derivative" && rate < 0 ? 0 : rate;
                }
                break;
            case "moving_average":
                var window = Math.Max(1, (int)item.Param);
                for (int i = window - 1; i < values.Count; i++)
                    result[values[i].TimestampNs] = values.Skip(i - window + 1).Take(window).Average(v => v.Value);
                break;
            case "cumulative_sum":
                double sum = 0;
                foreach (var value in values) { sum += value.Value; result[value.TimestampNs] = sum; }
                break;
            case "integral":
                var integralUnitNs = item.UnitNs ?? 1_000_000_000L;
                double area = 0;
                for (int i = 1; i < values.Count; i++)
                {
                    var deltaNs = values[i].TimestampNs - values[i - 1].TimestampNs;
                    if (deltaNs <= 0) continue;
                    area += ((values[i - 1].Value + values[i].Value) / 2.0) * deltaNs / integralUnitNs;
                    result[values[i].TimestampNs] = area;
                }
                break;
            case "top":
                AddRankedValues(result, values.OrderByDescending(v => v.Value).ThenBy(v => v.TimestampNs), item.Param);
                break;
            case "bottom":
                AddRankedValues(result, values.OrderBy(v => v.Value).ThenBy(v => v.TimestampNs), item.Param);
                break;
            case "sample":
                AddSampledValues(result, values, item.Param);
                break;
            case "elapsed":
                for (int i = 1; i < values.Count; i++) result[values[i].TimestampNs] = (values[i].TimestampNs - values[i - 1].TimestampNs) / 1_000_000_000.0;
                break;
            default:
                var pairs = BuildValuePairs(pts, item.Field);
                result[values[^1].TimestampNs] = Calc(pairs.Select(p => p.Value).ToList(), item.Func, item.Param, pairs.Select(p => p.TimestampNs).ToList(), item.UnitNs);
                break;
        }
        return result;
    }

    static object? Calc(List<FieldValue> vs, string fn, double param = 0, List<long>? timestamps = null, long? unitNs = null)
    {
        if (fn == "count") return vs.Count;
        if (vs.Count == 0) return null;
        var nums = vs.Select(v => v.AsDouble()).Where(x => x.HasValue).Select(x => x!.Value).ToList();
        var effectiveUnitNs = unitNs ?? 1_000_000_000L;
        return fn switch
        {
            "sum" => nums.Count == 0 ? null : nums.Sum(),
            "mean" => nums.Count == 0 ? null : nums.Average(),
            "min" => nums.Count == 0 ? null : nums.Min(),
            "max" => nums.Count == 0 ? null : nums.Max(),
            "first" => vs[0].ToObject(),
            "last" => vs[^1].ToObject(),
            "spread" => nums.Count == 0 ? null : nums.Max() - nums.Min(),
            "stddev" => nums.Count < 2 ? null : StdDev(nums),
            "median" => nums.Count == 0 ? null : Percentile(nums, 50),
            "percentile" => nums.Count == 0 ? null : Percentile(nums, param),
            "derivative" => CalcDerivative(timestamps, nums, false, effectiveUnitNs),
            "non_negative_derivative" => CalcDerivative(timestamps, nums, true, effectiveUnitNs),
            "difference" => nums.Count < 2 ? null : nums[^1] - nums[0],
            "cumulative_sum" => nums.Count == 0 ? null : nums.Sum(),
            "moving_average" => nums.Count == 0 ? null : nums.Average(),
            "integral" => timestamps == null ? null : CalcIntegral(timestamps, nums, effectiveUnitNs),
            _ => null
        };
    }

    static double StdDev(List<double> nums)
    {
        if (nums.Count < 2) return 0;
        var mean = nums.Average();
        var sumSq = nums.Sum(x => (x - mean) * (x - mean));
        return Math.Sqrt(sumSq / (nums.Count - 1));
    }

    static double Percentile(List<double> nums, double p)
    {
        if (nums.Count == 0) return 0;
        var sorted = nums.Order().ToList();
        var rank = (p / 100.0) * (sorted.Count - 1);
        var lower = (int)Math.Floor(rank);
        var upper = (int)Math.Ceiling(rank);
        if (lower == upper) return sorted[lower];
        return sorted[lower] + (rank - lower) * (sorted[upper] - sorted[lower]);
    }

    static double? CalcDerivative(List<long>? timestamps, List<double> nums, bool nonNegative, long unitNs)
    {
        if (timestamps == null || timestamps.Count < 2 || nums.Count < 2) return null;
        var valDiff = nums[^1] - nums[0];
        var timeDiffNs = timestamps[^1] - timestamps[0];
        if (timeDiffNs <= 0) return null;
        var rate = valDiff * unitNs / timeDiffNs;
        if (nonNegative && rate < 0) return 0;
        return rate;
    }

    static double? CalcIntegral(List<long> timestamps, List<double> nums, long unitNs)
    {
        if (timestamps.Count < 2 || nums.Count < 2) return null;
        double area = 0;
        for (int i = 1; i < Math.Min(timestamps.Count, nums.Count); i++)
        {
            var deltaNs = timestamps[i] - timestamps[i - 1];
            if (deltaNs <= 0) continue;
            area += ((nums[i - 1] + nums[i]) / 2.0) * deltaNs / unitNs;
        }
        return area;
    }

    static List<(long TimestampNs, FieldValue Value)> BuildValuePairs(List<Point> points, string field)
    {
        if (field == "*")
            return points.OrderBy(p => p.TimestampNs)
                .Select(p => (p.TimestampNs, FieldValue.FromInteger(p.Fields.Count)))
                .ToList();

        return points
            .Where(p => p.Fields.TryGetValue(field, out var v) && v.AsDouble().HasValue)
            .OrderBy(p => p.TimestampNs)
            .Select(p => (p.TimestampNs, p.Fields[field]))
            .ToList();
    }

    static void AddRankedValues(SortedDictionary<long, object?> result, IEnumerable<(long TimestampNs, double Value)> ordered, double param)
    {
        var take = Math.Max(1, (int)param);
        foreach (var value in ordered.Take(take))
            result[value.TimestampNs] = value.Value;
    }

    static void AddSampledValues(SortedDictionary<long, object?> result, List<(long TimestampNs, double Value)> values, double param)
    {
        var take = Math.Max(1, (int)param);
        if (values.Count <= take)
        {
            foreach (var value in values) result[value.TimestampNs] = value.Value;
            return;
        }

        var step = (double)(values.Count - 1) / Math.Max(1, take - 1);
        var used = new HashSet<int>();
        for (int i = 0; i < take; i++)
        {
            var index = (int)Math.Round(i * step, MidpointRounding.AwayFromZero);
            if (!used.Add(index)) continue;
            var value = values[index];
            result[value.TimestampNs] = value.Value;
        }
    }

    List<QuerySeries>? TrySelectFunctionsWithPushdown(
        TsdbEngine e,
        string db,
        string rp,
        ParsedQuery q,
        HashSet<string>? requestedFields,
        HashSet<string>? seriesFilter,
        CancellationToken cancellationToken,
        QueryExecutionReport report)
    {
        var items = q.Select.Where(s => s.Func != "").ToList();
        if (items.Count == 0) return null;
        if (items.Any(i => i.Func is "difference" or "derivative" or "non_negative_derivative" or "moving_average" or "cumulative_sum" or "elapsed" or "top" or "bottom" or "sample" or "integral" or "percentile" or "median" or "stddev" or "spread" or "first" or "last"))
            return null;
        if (q.FieldFilters.Count > 0) return null;

        var bufferPoints = e.ReadBufferedPoints(db, rp, q.Measurement, q.MinTimeNs, q.MaxTimeNs, requestedFields, seriesFilter);
        report.ScannedPoints += bufferPoints.Count;
        var metas = e.ReadSegmentMetadata(db, rp, q.Measurement, q.MinTimeNs, q.MaxTimeNs, requestedFields, seriesFilter, cancellationToken);
        if (metas.Count == 0) return null;
        if (metas.Any(m => !IsFullCoverage(m, q.MinTimeNs, q.MaxTimeNs) || m.Stats == null)) return null;
        report.ScannedPoints += metas.Sum(m => m.PointCount);

        var row = new List<object?> { Time(MaxTime(metas, bufferPoints)) };
        foreach (var item in items)
        {
            var relevantMetas = metas.Where(m => m.Field == item.Field).ToList();
            if (relevantMetas.Count == 0 && !bufferPoints.Any(p => p.Fields.ContainsKey(item.Field))) return null;
            row.Add(CalcPushdownValue(item.Func, item.Field, relevantMetas, bufferPoints));
        }

        report.UsedAggregatePushdown = true;
        return [new QuerySeries
        {
            Name = q.Measurement ?? "",
            Columns = ["time", .. items.Select(i => i.Alias)],
            Values = [row]
        }];
    }

    List<QuerySeries>? TryGroupByTagFunctionsWithPushdown(
        TsdbEngine e,
        string db,
        string rp,
        ParsedQuery q,
        HashSet<string>? requestedFields,
        HashSet<string>? seriesFilter,
        CancellationToken cancellationToken,
        QueryExecutionReport report,
        string resultMeasurement)
    {
        if (q.GroupByTags.Count == 0 || q.GroupByNs.HasValue)
            return null;

        var items = q.Select.Where(s => s.Func != "").ToList();
        if (items.Count == 0)
            return null;
        if (items.Any(i => i.Func is "difference" or "derivative" or "non_negative_derivative" or "moving_average" or "cumulative_sum" or "elapsed" or "top" or "bottom" or "sample" or "integral" or "percentile" or "median" or "stddev" or "spread" or "first" or "last"))
            return null;
        if (q.FieldFilters.Count > 0)
            return null;
        if (items.Any(i => i.Field == "*"))
            return null;

        var bufferPoints = e.ReadBufferedPoints(db, rp, q.Measurement, q.MinTimeNs, q.MaxTimeNs, requestedFields, seriesFilter);
        report.ScannedPoints += bufferPoints.Count;

        var metas = e.ReadSegmentMetadata(db, rp, q.Measurement, q.MinTimeNs, q.MaxTimeNs, requestedFields, seriesFilter, cancellationToken);
        if (metas.Count == 0)
            return null;
        if (metas.Any(m => !IsFullCoverage(m, q.MinTimeNs, q.MaxTimeNs) || m.Stats == null))
            return null;
        report.ScannedPoints += metas.Sum(m => m.PointCount);

        var groupedMetas = metas.GroupBy(meta =>
        {
            var tags = ParseTagsCanonical(meta.TagsCanonical);
            return BuildGroupByTagKey(tags, q.GroupByTags);
        }).ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);

        var groupedBufferPoints = bufferPoints.GroupBy(point => BuildGroupByTagKey(point.Tags, q.GroupByTags))
            .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);

        var groupKeys = groupedMetas.Keys.Concat(groupedBufferPoints.Keys).Distinct(StringComparer.Ordinal).OrderBy(x => x, StringComparer.Ordinal).ToList();
        if (groupKeys.Count == 0)
            return null;

        var result = new List<QuerySeries>();
        foreach (var groupKey in groupKeys)
        {
            groupedMetas.TryGetValue(groupKey, out var groupMetas);
            groupedBufferPoints.TryGetValue(groupKey, out var groupBufferPoints);
            groupMetas ??= [];
            groupBufferPoints ??= [];

            var row = new List<object?> { Time(MaxTime(groupMetas, groupBufferPoints)) };
            foreach (var item in items)
            {
                var relevantMetas = groupMetas.Where(m => m.Field == item.Field).ToList();
                if (relevantMetas.Count == 0 && !groupBufferPoints.Any(p => p.Fields.ContainsKey(item.Field)))
                    return null;
                row.Add(CalcPushdownValue(item.Func, item.Field, relevantMetas, groupBufferPoints));
            }

            result.Add(new QuerySeries
            {
                Name = resultMeasurement,
                Tags = BuildGroupByTags(groupKey, q.GroupByTags),
                Columns = ["time", .. items.Select(i => i.Alias)],
                Values = [row]
            });
        }

        report.UsedAggregatePushdown = true;
        return ApplySeriesWindow(result, q.SeriesOffset, q.SeriesLimit);
    }

    static bool IsFullCoverage(SegmentColumnMeta meta, long? minTimeNs, long? maxTimeNs) =>
        (!minTimeNs.HasValue || minTimeNs.Value <= meta.MinTime) && (!maxTimeNs.HasValue || maxTimeNs.Value >= meta.MaxTime);

    static long MaxTime(List<SegmentColumnMeta> metas, List<Point> bufferPoints)
    {
        var metaMax = metas.Count == 0 ? 0 : metas.Max(m => m.MaxTime);
        var bufferMax = bufferPoints.Count == 0 ? 0 : bufferPoints.Max(p => p.TimestampNs);
        return Math.Max(metaMax, bufferMax);
    }

    static object? CalcPushdownValue(string func, string field, List<SegmentColumnMeta> metas, List<Point> bufferPoints)
    {
        var bufferValues = bufferPoints
            .Where(p => p.Fields.TryGetValue(field, out var v) && v.AsDouble().HasValue)
            .Select(p => p.Fields[field].AsDouble()!.Value)
            .ToList();
        var metaStats = metas.Select(m => m.Stats!).ToList();
        return func switch
        {
            "count" => metas.Sum(m => m.PointCount) + bufferValues.Count,
            "sum" => metaStats.Sum(s => s.Sum) + bufferValues.Sum(),
            "min" => CombineMin(metaStats, bufferValues),
            "max" => CombineMax(metaStats, bufferValues),
            "mean" => CombineMean(metaStats, bufferValues),
            _ => null
        };
    }

    static string BuildGroupByTagKey(IReadOnlyDictionary<string, string> tags, List<string> groupByTags)
    {
        if (groupByTags.Count == 0)
            return string.Empty;
        return string.Join("|", groupByTags.Select(tag => tags.TryGetValue(tag, out var value) ? value : ""));
    }

    static Dictionary<string, string>? BuildGroupByTags(string groupKey, List<string> groupByTags)
    {
        if (groupByTags.Count == 0)
            return null;

        var values = groupKey.Split('|');
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        for (int i = 0; i < groupByTags.Count; i++)
        {
            var value = i < values.Length ? values[i] : "";
            result[groupByTags[i]] = value;
        }
        return result;
    }

    static Dictionary<string, string> ParseTagsCanonical(string tagsCanonical)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (string.IsNullOrEmpty(tagsCanonical))
            return result;

        foreach (var pair in tagsCanonical.Split(','))
        {
            var separator = pair.IndexOf('=');
            if (separator > 0)
                result[pair[..separator]] = pair[(separator + 1)..];
        }

        return result;
    }

    static object? CombineMin(List<BlockStats> stats, List<double> bufferValues)
    {
        var mins = stats.Select(s => s.Min).Concat(bufferValues).ToList();
        return mins.Count == 0 ? null : mins.Min();
    }

    static object? CombineMax(List<BlockStats> stats, List<double> bufferValues)
    {
        var maxes = stats.Select(s => s.Max).Concat(bufferValues).ToList();
        return maxes.Count == 0 ? null : maxes.Max();
    }

    static object? CombineMean(List<BlockStats> stats, List<double> bufferValues)
    {
        var sum = stats.Sum(s => s.Sum) + bufferValues.Sum();
        var count = stats.Sum(s => s.Count) + bufferValues.Count;
        return count == 0 ? null : sum / count;
    }

    AuthStore RequireAuthStore() => _authStore ?? throw new InvalidOperationException("auth store is not configured");

    static string FormatGrants(Dictionary<string, string> grants) =>
        grants.Count == 0
            ? ""
            : string.Join(",", grants.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => $"{kv.Key}:{kv.Value}"));

    static int CountRows(QueryResponse response) =>
        response.Results.SelectMany(r => r.Series ?? []).Sum(s => s.Values.Count);

    static bool CanStreamRawSelect(ParsedQuery q) =>
        q.Kind == QueryKind.Select
        && q.Subquery == null
        && q.GroupByNs == null
        && q.GroupByTags.Count == 0
        && q.Select.All(s => string.IsNullOrEmpty(s.Func))
        && string.IsNullOrWhiteSpace(q.IntoTarget)
        && !q.Desc
        && !string.IsNullOrWhiteSpace(q.Measurement);

    static HashSet<string>? BuildRequestedFields(ParsedQuery q)
    {
        HashSet<string>? requestedFields = null;
        if (q.Select.Count > 0 && !(q.Select.Count == 1 && q.Select[0].Field == "*"))
            requestedFields = new HashSet<string>(q.Select.Select(x => x.Field), StringComparer.Ordinal);
        if (requestedFields != null)
            foreach (var filter in q.FieldFilters)
                requestedFields.Add(filter.Field);
        return requestedFields;
    }

    static List<string> ResolveRawFields(TsdbEngine e, string db, ParsedQuery q, HashSet<string>? requestedFields)
    {
        if (requestedFields != null)
            return q.Select.Select(x => x.Field).Distinct(StringComparer.Ordinal).ToList();

        return e.Schema.GetFields(db, q.Measurement)
            .Select(x => x.FieldKey)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToList();
    }

    static List<string> ResolveRawTags(TsdbEngine e, string db, string? measurement) =>
        e.ListSeries(db, measurement)
            .SelectMany(ParseTagsCanonical)
            .Select(kv => kv.Key)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToList();

    int? EffectiveRowLimit(ParsedQuery q)
    {
        if (_maxResponseRows <= 0)
            return q.Limit;
        return Math.Min(q.Limit ?? _maxResponseRows, _maxResponseRows);
    }

    static bool MatchesTagFilters(Point point, List<TagFilter> filters)
    {
        if (filters.Count == 0) return true;
        foreach (var f in filters)
        {
            var tagVal = point.Tags.TryGetValue(f.Key, out var v) ? v : null;
            switch (f.Op)
            {
                case TagOp.Eq: if (tagVal != f.Value) return false; break;
                case TagOp.Neq: if (tagVal == f.Value) return false; break;
                case TagOp.Regex: if (tagVal == null || !Regex.IsMatch(tagVal, f.Value)) return false; break;
                case TagOp.NotRegex: if (tagVal != null && Regex.IsMatch(tagVal, f.Value)) return false; break;
            }
        }
        return true;
    }

    static bool MatchesFieldFilters(Point point, List<FieldFilter> filters)
    {
        if (filters.Count == 0) return true;
        foreach (var f in filters)
        {
            if (!point.Fields.TryGetValue(f.Field, out var fv)) return false;
            var numVal = fv.AsDouble();
            if (!numVal.HasValue) return false;
            var ok = f.Op switch
            {
                FieldOp.Eq => Math.Abs(numVal.Value - f.Value) < 1e-9,
                FieldOp.Neq => Math.Abs(numVal.Value - f.Value) >= 1e-9,
                FieldOp.Gt => numVal.Value > f.Value,
                FieldOp.Gte => numVal.Value >= f.Value,
                FieldOp.Lt => numVal.Value < f.Value,
                FieldOp.Lte => numVal.Value <= f.Value,
                _ => false
            };
            if (!ok) return false;
        }
        return true;
    }

    static List<object?> BuildRawRow(Point point, List<string> tags, List<string> fields)
    {
        var row = new List<object?> { Time(point.TimestampNs) };
        foreach (var tag in tags)
            row.Add(point.Tags.TryGetValue(tag, out var value) ? value : null);
        foreach (var field in fields)
            row.Add(point.Fields.TryGetValue(field, out var value) ? value.ToObject() : null);
        return row;
    }

    static QueryResponse RawChunk(string measurement, List<string> columns, List<List<object?>> rows) => new()
    {
        Results =
        [
            new QueryResult
            {
                StatementId = 0,
                Series = [new QuerySeries { Name = measurement, Columns = [.. columns], Values = rows }]
            }
        ]
    };

    static QueryResponse ErrorChunk(string error) => new()
    {
        Results = [new QueryResult { StatementId = 0, Error = error }]
    };

    static IEnumerable<QueryResponse> ChunkResponse(QueryResponse result, int chunkSize)
    {
        foreach (var queryResult in result.Results)
        {
            if (queryResult.Series == null || queryResult.Series.Count == 0)
            {
                yield return new QueryResponse { Results = [new QueryResult { StatementId = queryResult.StatementId, Error = queryResult.Error }] };
                continue;
            }

            foreach (var series in queryResult.Series)
            {
                if (series.Values.Count == 0)
                {
                    yield return new QueryResponse { Results = [new QueryResult { StatementId = queryResult.StatementId, Series = [CloneSeries(series, [])], Error = queryResult.Error }] };
                    continue;
                }

                for (int i = 0; i < series.Values.Count; i += chunkSize)
                {
                    yield return new QueryResponse
                    {
                        Results =
                        [
                            new QueryResult
                            {
                                StatementId = queryResult.StatementId,
                                Error = queryResult.Error,
                                Series = [CloneSeries(series, series.Values.Skip(i).Take(chunkSize).ToList())]
                            }
                        ]
                    };
                }
            }
        }
    }

    static QuerySeries CloneSeries(QuerySeries series, List<List<object?>> values) => new()
    {
        Name = series.Name,
        Tags = series.Tags == null ? null : new Dictionary<string, string>(series.Tags, StringComparer.Ordinal),
        Columns = [.. series.Columns],
        Values = values,
        TagColumns = series.TagColumns == null ? null : new HashSet<string>(series.TagColumns, StringComparer.Ordinal)
    };

    static long EstimateRawSeriesShellBytes(string measurement, List<string> columns) =>
        96 + EstimateStringBytes(measurement) + columns.Sum(col => 16 + EstimateStringBytes(col));

    static void CopyReport(QueryExecutionReport source, QueryExecutionReport target)
    {
        target.ScannedPoints = source.ScannedPoints;
        target.RowsReturned = source.RowsReturned;
        target.DurationMs = source.DurationMs;
        target.TimedOut = source.TimedOut;
        target.Canceled = source.Canceled;
        target.UsedAggregatePushdown = source.UsedAggregatePushdown;
        target.UsedRegexPushdown = source.UsedRegexPushdown;
        target.UsedSeriesIndexPushdown = source.UsedSeriesIndexPushdown;
        target.EstimatedInputBytes = source.EstimatedInputBytes;
        target.EstimatedResultBytes = source.EstimatedResultBytes;
        target.PeakEstimatedMemoryBytes = source.PeakEstimatedMemoryBytes;
        target.Error = source.Error;
    }

    static void Req(string? db)
    {
        if (string.IsNullOrWhiteSpace(db))
            throw new InvalidOperationException("missing required parameter db");
    }

    static string Time(long ns)
    {
        var seconds = Math.DivRem(ns, 1_000_000_000L, out var nanos);
        var dt = DateTimeOffset.FromUnixTimeSeconds(seconds).UtcDateTime;
        return $"{dt:yyyy-MM-ddTHH:mm:ss}.{nanos:D9}Z";
    }

    static long ParseTimeNs(object? value)
    {
        if (value is long l) return l;
        if (value is double d) return (long)d;
        if (value is not string s) return 0;
        if (long.TryParse(s, out var numeric)) return numeric;

        var trimmed = s.Trim();
        var z = trimmed.EndsWith("Z", StringComparison.OrdinalIgnoreCase) ? trimmed[..^1] : trimmed;
        var dot = z.IndexOf('.');
        var basePart = dot >= 0 ? z[..dot] : z;
        var fraction = dot >= 0 ? z[(dot + 1)..] : "";
        if (DateTime.TryParseExact(basePart, "yyyy-MM-ddTHH:mm:ss", null, System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal, out var dt))
        {
            var seconds = new DateTimeOffset(dt, TimeSpan.Zero).ToUnixTimeSeconds();
            var normalizedFraction = fraction.Length == 0 ? "0" : (fraction.Length >= 9 ? fraction[..9] : fraction.PadRight(9, '0'));
            var nanos = long.Parse(normalizedFraction);
            return seconds * 1_000_000_000L + nanos;
        }
        return DateTimeOffset.TryParse(s, out var dto) ? dto.ToUnixTimeMilliseconds() * 1_000_000 : 0;
    }

    static object? LinearFill(Dictionary<long, List<object?>> rows, long time, int col)
    {
        var prev = rows.Where(kv => kv.Key < time && kv.Value.Count > col && TryDouble(kv.Value[col], out _)).LastOrDefault();
        var next = rows.Where(kv => kv.Key > time && kv.Value.Count > col && TryDouble(kv.Value[col], out _)).FirstOrDefault();
        if (prev.Value == null || next.Value == null) return null;
        if (!TryDouble(prev.Value[col], out var y0) || !TryDouble(next.Value[col], out var y1)) return null;
        var ratio = (double)(time - prev.Key) / (next.Key - prev.Key);
        return y0 + (y1 - y0) * ratio;
    }

    static bool TryDouble(object? value, out double result)
    {
        result = 0;
        return value switch
        {
            double d => (result = d) == d,
            int i => (result = i) == i,
            long l => (result = l) == l,
            _ => value != null && double.TryParse(value.ToString(), out result)
        };
    }

    static string FormatSeriesKey(string? measurement, string tagsCanonical)
    {
        if (measurement == null) return tagsCanonical;
        return string.IsNullOrWhiteSpace(tagsCanonical) ? measurement : $"{measurement},{tagsCanonical}";
    }

    static List<List<object?>> OrderRowsByTime(List<List<object?>> rows, bool desc) =>
        desc
            ? rows.OrderByDescending(r => ParseTimeNs(r[0])).ToList()
            : rows.OrderBy(r => ParseTimeNs(r[0])).ToList();

    static List<QuerySeries> ApplySeriesWindow(IEnumerable<QuerySeries> series, int? offset, int? limit)
    {
        var ordered = series
            .OrderBy(s => s.Name, StringComparer.Ordinal)
            .ThenBy(s => FormatTagSet(s.Tags), StringComparer.Ordinal)
            .ToList();

        if (offset.HasValue) ordered = ordered.Skip(offset.Value).ToList();
        if (limit.HasValue) ordered = ordered.Take(limit.Value).ToList();
        return ordered;
    }

    static string FormatTagSet(Dictionary<string, string>? tags) =>
        tags == null
            ? string.Empty
            : string.Join(",", tags.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => $"{kv.Key}={kv.Value}"));

    static string FormatDuration(long ns)
    {
        if (ns <= 0) return "0s";
        var s = ns / 1_000_000_000;
        if (s % 86400 == 0 && s >= 86400) return $"{s / 86400}d";
        if (s % 3600 == 0 && s >= 3600) return $"{s / 3600}h";
        if (s % 60 == 0 && s >= 60) return $"{s / 60}m";
        return $"{s}s";
    }

    static string ResolveResultMeasurementName(ParsedQuery q) =>
        !string.IsNullOrWhiteSpace(q.Measurement)
            ? q.Measurement!
            : !string.IsNullOrWhiteSpace(q.Subquery?.Measurement)
                ? q.Subquery!.Measurement!
                : "subquery";

    private sealed record IntoTarget(string Database, string RetentionPolicy, string Measurement);
}
