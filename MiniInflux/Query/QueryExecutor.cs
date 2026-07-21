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

public sealed class QueryDebugResponse
{
    public QueryResponse Response { get; set; } = new();
    public QueryExecutionReport Report { get; set; } = new();
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
    public bool UsedStreamingRawSelect { get; set; }
    public bool UsedStreamingAggregate { get; set; }
    public int SegmentMetadataFooterHits { get; set; }
    public int SegmentMetadataFullReads { get; set; }
    public int SegmentColumnsRead { get; set; }
    public int PointsMaterialized { get; set; }
    public string? LimitPushdownStopReason { get; set; }
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

public sealed class QueryJsonExecutionOutcome
{
    public required byte[] Json { get; init; }
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
    public QueryExecutor(
        int maxResponseRows = 100_000,
        int maxQueryPoints = 1_000_000,
        int maxQueryDurationMs = 0,
        long maxQueryMemoryBytes = 0)
    {
        _maxResponseRows = maxResponseRows;
        _maxQueryPoints = maxQueryPoints;
        _maxQueryDurationMs = maxQueryDurationMs;
        _maxQueryMemoryBytes = maxQueryMemoryBytes;
    }

    public Task<QueryResponse> ExecuteAsync(TsdbEngine e, string? db, string q, CancellationToken cancellationToken = default)
        => Task.FromResult(ExecuteWithReport(e, db, q, cancellationToken).Response);

    public QueryJsonExecutionOutcome? TryExecuteBufferedRawDescendingJson(TsdbEngine e, string? db, string query, string? epoch = null, CancellationToken cancellationToken = default)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var report = new QueryExecutionReport();
        ParsedQuery q;
        try
        {
            q = InfluxQlParser.Parse(query);
        }
        catch
        {
            return null;
        }

        if (!CanWriteBufferedRawDescendingJson(q))
            return null;

        try
        {
            using var timeoutCts = _maxQueryDurationMs > 0 ? new CancellationTokenSource(_maxQueryDurationMs) : null;
            using var linkedCts = timeoutCts != null
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token)
                : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var token = linkedCts.Token;

            Req(q.SourceDatabase ?? db);
            var sourceDb = q.SourceDatabase ?? db!;
            var sourceRp = q.SourceRpName ?? e.GetDefaultRpName(sourceDb);
            var requestedFields = BuildRequestedFields(q);
            var seriesFilter = BuildSeriesFilter(e, sourceDb, q, report);
            if (seriesFilter is null || seriesFilter.Count != 1)
                return null;

            var rowLimit = Math.Min(q.Limit ?? _maxResponseRows, _maxResponseRows);
            var readLimit = checked(Math.Max(0, q.Offset ?? 0) + rowLimit);
            var tagsCanonical = seriesFilter.First();
            var resultMeasurement = ResolveResultMeasurementName(q);
            var fields = ResolveRawFields(e, sourceDb, q, requestedFields);
            var seriesTags = ParseTagsCanonical(tagsCanonical);
            var offset = Math.Max(0, q.Offset ?? 0);
            var epochDivisor = ParseEpochDivisor(epoch);
            if (fields.Count > 0)
            {
                var fieldsRead = e.TryReadFlushedFieldsDescending(
                    sourceDb,
                    sourceRp,
                    q.Measurement!,
                    tagsCanonical,
                    fields,
                    q.MinTimeNs,
                    q.MaxTimeNs,
                    readLimit,
                    token);
                if (fieldsRead != null)
                {
                    report.UsedStreamingRawSelect = true;
                    report.SegmentColumnsRead += fieldsRead.SegmentColumnsRead;
                    report.PointsMaterialized = 0;
                    report.LimitPushdownStopReason = fieldsRead.LimitPushdownStopReason;
                    report.ScannedPoints = fieldsRead.Timestamps.Count;
                    report.EstimatedInputBytes = EstimateFieldRowsBytes(fieldsRead.Timestamps, fieldsRead.Rows);
                    var fieldCols = new List<string>(1 + fields.Count) { "time" };
                    fieldCols.AddRange(fields);
                    var fieldResultBytes = EstimateRawSeriesShellBytes(resultMeasurement, fieldCols);
                    var fieldBuffer = new System.Buffers.ArrayBufferWriter<byte>();
                    using (var writer = new System.Text.Json.Utf8JsonWriter(fieldBuffer))
                        WriteFieldRowsRawJsonResponse(writer, resultMeasurement, seriesTags, fieldCols, fieldsRead, offset, rowLimit, epochDivisor, report, ref fieldResultBytes);

                    sw.Stop();
                    report.DurationMs = sw.ElapsedMilliseconds;
                    report.EstimatedResultBytes = fieldResultBytes;
                    report.PeakEstimatedMemoryBytes = report.EstimatedInputBytes + fieldResultBytes;
                    return new QueryJsonExecutionOutcome { Json = fieldBuffer.WrittenSpan.ToArray(), Report = report };
                }
            }

            var read = e.TryReadSeriesDescending(
                sourceDb,
                sourceRp,
                q.Measurement!,
                tagsCanonical,
                q.MinTimeNs,
                q.MaxTimeNs,
                requestedFields,
                readLimit,
                token);
            if (read is null)
                return null;
            var points = read.Points;
            report.UsedStreamingRawSelect = true;
            ApplyRawReadReport(report, read);

            var cols = new List<string>(1 + fields.Count) { "time" };
            cols.AddRange(fields);

            var resultBytes = EstimateRawSeriesShellBytes(resultMeasurement, cols);
            report.ScannedPoints = points.Count;
            report.EstimatedInputBytes = EstimatePointsBytes(points);

            var buffer = new System.Buffers.ArrayBufferWriter<byte>();
            using (var writer = new System.Text.Json.Utf8JsonWriter(buffer))
                WriteRawJsonResponse(writer, resultMeasurement, seriesTags, cols, fields, points, offset, rowLimit, epochDivisor, report, ref resultBytes);

            sw.Stop();
            report.DurationMs = sw.ElapsedMilliseconds;
            report.EstimatedResultBytes = resultBytes;
            report.PeakEstimatedMemoryBytes = report.EstimatedInputBytes + resultBytes;
            return new QueryJsonExecutionOutcome { Json = buffer.WrittenSpan.ToArray(), Report = report };
        }
        catch (OperationCanceledException) when (_maxQueryDurationMs > 0 && !cancellationToken.IsCancellationRequested)
        {
            sw.Stop();
            report.DurationMs = sw.ElapsedMilliseconds;
            report.TimedOut = true;
            report.Error = $"query timed out after {_maxQueryDurationMs} ms";
            return ErrorJsonOutcome(report);
        }
        catch (OperationCanceledException)
        {
            sw.Stop();
            report.DurationMs = sw.ElapsedMilliseconds;
            report.Canceled = true;
            report.Error = "query canceled";
            return ErrorJsonOutcome(report);
        }
        catch (Exception ex)
        {
            sw.Stop();
            report.DurationMs = sw.ElapsedMilliseconds;
            report.Error = ex.Message;
            return ErrorJsonOutcome(report);
        }
    }

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

            var parsed = InfluxQlParser.Parse(q);
            QueryResponse response;
            if (CanStreamRawSelectResponse(e, db, parsed))
            {
                response = ExecuteStreamingRawSelectResponse(e, db, parsed, report, token);
            }
            else
            {
                response = new QueryResponse
                {
                    Results = [new QueryResult { StatementId = 0, Series = Run(e, db, parsed, token, report) }]
                };
            }
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

    QueryResponse ExecuteStreamingRawSelectResponse(TsdbEngine e, string? db, ParsedQuery q, QueryExecutionReport report, CancellationToken cancellationToken)
    {
        report.UsedStreamingRawSelect = true;
        var rows = new List<List<object?>>();
        QuerySeries? firstSeries = null;
        foreach (var chunk in StreamRawSelectChunks(e, db, q, chunkSize: 4096, report, cancellationToken))
        {
            var series = chunk.Results[0].Series![0];
            firstSeries ??= new QuerySeries { Name = series.Name, Columns = [.. series.Columns], Values = rows };
            rows.AddRange(series.Values);
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, report.EstimatedInputBytes + report.EstimatedResultBytes);
        }

        firstSeries ??= new QuerySeries { Name = q.Measurement ?? "", Columns = ["time"], Values = rows };
        report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, report.EstimatedResultBytes);
        EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
        return new QueryResponse { Results = [new QueryResult { StatementId = 0, Series = [firstSeries] }] };
    }

    IEnumerable<QueryResponse> ExecuteStreamingRawSelect(TsdbEngine e, string? db, ParsedQuery q, int chunkSize, QueryExecutionReport report, CancellationToken cancellationToken)
    {
        report.UsedStreamingRawSelect = true;
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
        fields.RemoveAll(field => tags.Contains(field, StringComparer.Ordinal));
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
            var rowBytes = 16 + row.Sum(EstimateObjectBytes);
            report.EstimatedResultBytes += rowBytes;
            chunkBytes += rowBytes;
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, chunkBytes);
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
        if (q.ContinuousQueryForNs.HasValue && q.ContinuousQueryForNs.Value > 0)
        {
            if (q.ContinuousQueryForNs.Value < parsedSelect.GroupByNs.Value)
                throw new NotSupportedException("continuous query RESAMPLE FOR must be greater than or equal to GROUP BY time(...)");
            if (q.ContinuousQueryForNs.Value < cadence)
                throw new NotSupportedException("continuous query RESAMPLE FOR must be greater than or equal to RESAMPLE EVERY");
        }

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
        var pointsAreDescending = false;
        var tagFiltersApplied = false;
        var filtersMayChangePoints = true;
        if (q.Subquery != null)
        {
            var innerSeries = Run(e, db, q.Subquery, cancellationToken, report) ?? [];
            pts = MaterializeSubqueryPoints(innerSeries);
            EnsureQueryPointLimit(pts.Count);
            var materializedPointBytes = EstimatePointsBytes(pts);
            var innerSeriesBytes = EstimateQuerySeriesBytes(innerSeries);
            report.EstimatedInputBytes = Math.Max(report.EstimatedInputBytes, materializedPointBytes);
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, innerSeriesBytes + materializedPointBytes);
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
                    report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, report.EstimatedInputBytes + resultBytes);
                    EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
                    if (!string.IsNullOrWhiteSpace(q.IntoTarget))
                    {
                        var selectIntoBytes = EstimateSelectIntoPointBytes(e, sourceDb, q, pushedDown);
                        report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, resultBytes + selectIntoBytes);
                        EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
                        ExecuteSelectInto(e, sourceDb, q, pushedDown);
                    }
                    return pushedDown;
                }

                // Fast count fallback: read only timestamps from segments, skip value-block decoding.
                // This avoids the expensive ReadAllPoints path when the metadata pushdown fails due
                // to overlapping segments, missing stats, or buffered-point conflicts.
                if (q.GroupByTags.Count == 0 && !q.GroupByAllTags)
                {
                    var fastCount = TryFastCountFallback(e, sourceDb, sourceRp, q, requestedFields, seriesFilter, cancellationToken, report);
                    if (fastCount != null)
                    {
                        var resultBytes = EstimateQuerySeriesBytes(fastCount);
                        report.EstimatedResultBytes = resultBytes;
                        report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, report.EstimatedInputBytes + resultBytes);
                        EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
                        if (!string.IsNullOrWhiteSpace(q.IntoTarget))
                        {
                            var selectIntoBytes = EstimateSelectIntoPointBytes(e, sourceDb, q, fastCount);
                            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, resultBytes + selectIntoBytes);
                            EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
                            ExecuteSelectInto(e, sourceDb, q, fastCount);
                        }
                        return fastCount;
                    }
                }
            }

            var streamedAggregate = TryGroupByStreamingFunctions(e, sourceDb, sourceRp, q, requestedFields, seriesFilter, cancellationToken, report, resultMeasurement);
            if (streamedAggregate != null)
            {
                report.EstimatedResultBytes = EstimateQuerySeriesBytes(streamedAggregate);
                report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, report.EstimatedInputBytes + report.EstimatedResultBytes);
                EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
                return streamedAggregate;
            }

            var rawFieldResult = TryReadRawFieldDescending(e, sourceDb, sourceRp, q, requestedFields, seriesFilter, _maxResponseRows, cancellationToken, report, resultMeasurement);
            if (rawFieldResult != null)
                return rawFieldResult;

            var descendingRead = TryReadRawDescending(e, sourceDb, sourceRp, q, requestedFields, seriesFilter, _maxResponseRows, cancellationToken);
            if (descendingRead != null)
            {
                pts = descendingRead.Points;
                pointsAreDescending = true;
                tagFiltersApplied = true;
                filtersMayChangePoints = q.FieldFilters.Count != 0;
                report.UsedStreamingRawSelect = true;
                ApplyRawReadReport(report, descendingRead);
            }
            else
            {
                pts = e.ReadAllPoints(sourceDb, sourceRp, q.Measurement, q.MinTimeNs, q.MaxTimeNs, requestedFields, seriesFilter, q.FieldFilters, cancellationToken);
            }
            EnsureQueryPointLimit(pts.Count);
            report.ScannedPoints += pts.Count;
            report.EstimatedInputBytes = EstimatePointsBytes(pts);
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, report.EstimatedInputBytes);
            EnsureQueryMemoryLimit(report.EstimatedInputBytes);
        }

        cancellationToken.ThrowIfCancellationRequested();
        if (!tagFiltersApplied) pts = ApplyTagFilters(pts, q.TagFilters);
        cancellationToken.ThrowIfCancellationRequested();
        pts = ApplyFieldFilters(pts, q.FieldFilters);
        if (q.Desc && !pointsAreDescending) pts.Reverse();

        var filteredInputBytes = filtersMayChangePoints ? EstimatePointsBytes(pts) : report.EstimatedInputBytes;
        report.EstimatedInputBytes = Math.Max(report.EstimatedInputBytes, filteredInputBytes);
        report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, filteredInputBytes);

        if (q.GroupByNs.HasValue || q.GroupByTags.Count > 0 || q.GroupByAllTags)
        {
            var groupingStateBytes = EstimateGroupingStateBytes(pts, q.GroupByTags, q.GroupByAllTags, q.GroupByNs);
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, filteredInputBytes + groupingStateBytes);
            EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);

            var aggregateResult = AggGroupBy(pts, q, cancellationToken, resultMeasurement);
            report.EstimatedResultBytes = EstimateQuerySeriesBytes(aggregateResult);
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, filteredInputBytes + groupingStateBytes + report.EstimatedResultBytes);
            EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
            if (!string.IsNullOrWhiteSpace(q.IntoTarget))
            {
                var selectIntoBytes = EstimateSelectIntoPointBytes(e, sourceDb, q, aggregateResult);
                report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, filteredInputBytes + groupingStateBytes + report.EstimatedResultBytes + selectIntoBytes);
                EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
                ExecuteSelectInto(e, sourceDb, q, aggregateResult);
            }
            return aggregateResult;
        }

        if (q.Select.Any(s => s.Func != ""))
        {
            var functionStateBytes = EstimateFunctionStateBytes(pts, q.Select.Where(s => s.Func != "").ToList());
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, filteredInputBytes + functionStateBytes);
            EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);

            var functionResult = SelectFunctions(pts, q, cancellationToken, resultMeasurement);
            report.EstimatedResultBytes = EstimateQuerySeriesBytes(functionResult);
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, filteredInputBytes + functionStateBytes + report.EstimatedResultBytes);
            EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
            if (!string.IsNullOrWhiteSpace(q.IntoTarget))
            {
                var selectIntoBytes = EstimateSelectIntoPointBytes(e, sourceDb, q, functionResult);
                report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, filteredInputBytes + functionStateBytes + report.EstimatedResultBytes + selectIntoBytes);
                EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
                ExecuteSelectInto(e, sourceDb, q, functionResult);
            }
            return functionResult;
        }

        var rowLimit = Math.Min(q.Limit ?? _maxResponseRows, _maxResponseRows);
        if (!pointsAreDescending || (q.Offset ?? 0) != 0)
            pts = pts.Skip(q.Offset ?? 0).Take(rowLimit).ToList();

        var fields = q.Select.Count == 1 && q.Select[0].Field == "*"
            ? pointsAreDescending
                ? ResolveRawFields(e, sourceDb, q, requestedFields)
                : pts.SelectMany(p => p.Fields.Keys).Distinct().Order().ToList()
            : q.Select.Select(x => x.Field).ToList();
        var tags = pointsAreDescending && pts.Count > 0
            ? pts[0].Tags.Keys.Order().ToList()
            : pts.SelectMany(p => p.Tags.Keys).Distinct().Order().ToList();
        fields.RemoveAll(field => tags.Contains(field, StringComparer.Ordinal));
        var cols = new List<string> { "time" };
        cols.AddRange(tags);
        cols.AddRange(fields);

        var vals = new List<List<object?>>(pts.Count);
        var rowCapacity = 1 + tags.Count + fields.Count;
        var rawResultBytes = EstimateRawSeriesShellBytes(resultMeasurement, cols);
        foreach (var p in pts)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var row = new List<object?>(rowCapacity) { Time(p.TimestampNs) };
            foreach (var t in tags)
                row.Add(p.Tags.TryGetValue(t, out var v)
                    ? v
                    : p.Fields.TryGetValue(t, out var legacyTag) && legacyTag.Kind == FieldKind.String
                        ? legacyTag.String
                        : null);
            foreach (var f in fields) row.Add(p.Fields.TryGetValue(f, out var v) ? v.ToObject() : null);
            vals.Add(row);
            rawResultBytes += 32 + EstimateRowBytes(row);
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
        report.EstimatedResultBytes = rawResultBytes;
        report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, filteredInputBytes + report.EstimatedResultBytes);
        EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
        if (!string.IsNullOrWhiteSpace(q.IntoTarget))
        {
            var selectIntoBytes = EstimateSelectIntoPointBytes(e, sourceDb, q, rawResult);
            report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, filteredInputBytes + report.EstimatedResultBytes + selectIntoBytes);
            EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);
            ExecuteSelectInto(e, sourceDb, q, rawResult);
        }
        return rawResult;
    }

    List<QuerySeries>? TryReadRawFieldDescending(TsdbEngine e, string sourceDb, string sourceRp, ParsedQuery q,
        HashSet<string>? requestedFields, HashSet<string>? seriesFilter, int maxResponseRows, CancellationToken cancellationToken,
        QueryExecutionReport report, string resultMeasurement)
    {
        if (!q.Desc
            || q.Subquery != null
            || q.GroupByNs.HasValue
            || q.GroupByTags.Count > 0
            || q.GroupByAllTags
            || q.Select.Any(s => !string.IsNullOrEmpty(s.Func))
            || !string.IsNullOrWhiteSpace(q.IntoTarget)
            || string.IsNullOrWhiteSpace(q.Measurement)
            || (seriesFilter == null && q.TagFilters.Count != 0)
            || q.FieldFilters.Count != 0)
            return null;

        var fields = ResolveRawFields(e, sourceDb, q, requestedFields);
        if (fields.Count == 0)
            return null;

        var series = seriesFilter ?? e.ListSeries(sourceDb, q.Measurement).ToHashSet(StringComparer.Ordinal);
        if (series.Count == 0)
            return null;

        var rowLimit = Math.Min(q.Limit ?? maxResponseRows, maxResponseRows);
        var offset = Math.Max(0, q.Offset ?? 0);
        var readLimit = checked(offset + rowLimit);
        var tagKeys = ResolveRawTags(e, sourceDb, q.Measurement);
        var columns = new List<string> { "time" };
        columns.AddRange(tagKeys);
        columns.AddRange(fields);
        var rows = new List<(long Timestamp, List<object?> Row)>();
        long resultBytes = EstimateRawSeriesShellBytes(resultMeasurement, columns);
        long inputBytes = 0;
        var scanned = 0;
        var segmentColumnsRead = 0;
        foreach (var tagsCanonical in series)
        {
            var read = e.TryReadFlushedFieldsDescending(sourceDb, sourceRp, q.Measurement, tagsCanonical, fields, q.MinTimeNs, q.MaxTimeNs, readLimit, cancellationToken);
            if (read == null)
                return null;

            var tags = ParseTagsCanonical(tagsCanonical);
            scanned += read.Timestamps.Count;
            inputBytes += EstimateFieldRowsBytes(read.Timestamps, read.Rows);
            segmentColumnsRead += read.SegmentColumnsRead;
            for (var i = 0; i < read.Timestamps.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var row = new List<object?>(columns.Count) { Time(read.Timestamps[i]) };
                foreach (var tag in tagKeys)
                    row.Add(tags.TryGetValue(tag, out var value) ? value : null);
                foreach (var value in read.Rows[i])
                    row.Add(value.HasValue ? value.Value.ToObject() : null);
                rows.Add((read.Timestamps[i], row));
            }
        }

        var values = rows.OrderByDescending(row => row.Timestamp).Skip(offset).Take(rowLimit).Select(row => row.Row).ToList();
        foreach (var row in values)
            resultBytes += 16 + row.Sum(EstimateObjectBytes);

        report.UsedStreamingRawSelect = true;
        report.ScannedPoints += scanned;
        report.RowsReturned = values.Count;
        report.SegmentColumnsRead += segmentColumnsRead;
        report.PointsMaterialized = 0;
        report.LimitPushdownStopReason = series.Count == 1 ? "segment-limit" : "series-limit";
        report.EstimatedInputBytes = inputBytes;
        report.EstimatedResultBytes = resultBytes;
        report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, inputBytes + resultBytes);
        EnsureQueryMemoryLimit(report.PeakEstimatedMemoryBytes);

        return
        [
            new()
            {
                Name = resultMeasurement,
                Columns = columns,
                Values = values,
                TagColumns = new HashSet<string>(tagKeys, StringComparer.Ordinal)
            }
        ];
    }

    static TsdbEngine.DescendingSeriesReadResult? TryReadRawDescending(TsdbEngine e, string sourceDb, string sourceRp, ParsedQuery q,
        HashSet<string>? requestedFields, HashSet<string>? seriesFilter, int maxResponseRows, CancellationToken cancellationToken)
    {
        if (!q.Desc
            || q.Subquery != null
            || q.GroupByNs.HasValue
            || q.GroupByTags.Count > 0
            || q.GroupByAllTags
            || q.Select.Any(s => !string.IsNullOrEmpty(s.Func))
            || !string.IsNullOrWhiteSpace(q.IntoTarget)
            || string.IsNullOrWhiteSpace(q.Measurement)
            || seriesFilter == null
            || seriesFilter.Count != 1
            || q.FieldFilters.Count != 0)
            return null;

        var rowLimit = Math.Min(q.Limit ?? maxResponseRows, maxResponseRows);
        var readLimit = checked(Math.Max(0, q.Offset ?? 0) + rowLimit);
        return e.TryReadSeriesDescending(sourceDb, sourceRp, q.Measurement, seriesFilter.First(), q.MinTimeNs, q.MaxTimeNs, requestedFields, readLimit, cancellationToken);
    }

    List<QuerySeries>? TryGroupByStreamingFunctions(TsdbEngine e, string db, string rp, ParsedQuery q,
        HashSet<string>? requestedFields, HashSet<string>? seriesFilter, CancellationToken cancellationToken,
        QueryExecutionReport report, string resultMeasurement)
    {
        var items = q.Select.Where(x => x.Func != "").ToList();
        var countStar = items.Count > 0 && items.All(i => i.Func == "count" && i.Field == "*");
        if (!string.IsNullOrWhiteSpace(q.IntoTarget)
            || q.Subquery != null
            || (!countStar && !q.GroupByNs.HasValue && q.GroupByTags.Count == 0 && !q.GroupByAllTags))
            return null;

        if (items.Count == 0 || items.Count != q.Select.Count)
            return null;
        if (items.Any(i => !IsSimpleStreamingAggregate(i) || (i.Field == "*" && i.Func != "count")))
            return null;

        var groups = new Dictionary<(string TagKey, long? BucketTime), StreamingAggregateGroup>();
        var scanned = 0;
        foreach (var point in e.EnumeratePoints(db, rp, q.Measurement, q.MinTimeNs, q.MaxTimeNs, requestedFields, seriesFilter, q.FieldFilters, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            scanned++;
            if (!countStar)
                EnsureQueryPointLimit(scanned);
            report.ScannedPoints = scanned;
            report.EstimatedInputBytes += EstimatePointBytes(point);
            if (!MatchesTagFilters(point, q.TagFilters) || !MatchesFieldFilters(point, q.FieldFilters))
                continue;

            var tagKey = BuildGroupByTagKey(point.Tags, q.GroupByTags, q.GroupByAllTags);
            long? bucketTime = q.GroupByNs.HasValue ? point.TimestampNs / q.GroupByNs.Value * q.GroupByNs.Value : null;
            var key = (tagKey, bucketTime);
            if (!groups.TryGetValue(key, out var group))
            {
                group = new StreamingAggregateGroup(items.Count);
                groups[key] = group;
            }
            group.MaxTime = Math.Max(group.MaxTime, point.TimestampNs);

            for (var i = 0; i < items.Count; i++)
            {
                if (items[i].Field == "*")
                    group.States[i].AddCount();
                else if (point.Fields.TryGetValue(items[i].Field, out var value) && value.AsDouble() is { } number)
                    group.States[i].Add(number);
            }
        }

        if (groups.Count == 0)
            return null;

        var seriesMap = new Dictionary<string, QuerySeries>();
        foreach (var (key, group) in groups.OrderBy(g => g.Key.BucketTime ?? g.Value.MaxTime))
        {
            var tagsDict = BuildGroupByTags(key.TagKey, q.GroupByTags, q.GroupByAllTags);
            var seriesKey = key.TagKey;
            if (!seriesMap.TryGetValue(seriesKey, out var series))
            {
                series = new QuerySeries
                {
                    Name = resultMeasurement,
                    Tags = tagsDict,
                    Columns = ["time", .. items.Select(x => x.Alias)],
                    Values = []
                };
                seriesMap[seriesKey] = series;
            }

            var row = new List<object?> { Time(key.BucketTime ?? group.MaxTime) };
            for (var i = 0; i < items.Count; i++)
                row.Add(group.States[i].Value(items[i].Func));
            series.Values.Add(row);
        }

        if (q.GroupByNs.HasValue && q.Fill != FillMode.None && q.MinTimeNs.HasValue && q.MaxTimeNs.HasValue)
            ApplyFill(seriesMap, q, q.GroupByNs.Value, items);

        var rowLimit = Math.Min(q.Limit ?? _maxResponseRows, _maxResponseRows);
        foreach (var series in seriesMap.Values)
        {
            series.Values = OrderRowsByTime(series.Values, q.Desc);
            series.Values = series.Values.Skip(q.Offset ?? 0).Take(rowLimit).ToList();
        }

        var result = ApplySeriesWindow(seriesMap.Values, q.SeriesOffset, q.SeriesLimit);
        EnsureWithinLimit(result.Sum(s => s.Values.Count));
        report.UsedStreamingAggregate = true;
        report.RowsReturned = result.Sum(s => s.Values.Count);
        report.PeakEstimatedMemoryBytes = Math.Max(report.PeakEstimatedMemoryBytes, EstimateQuerySeriesBytes(result));
        return result;
    }

    List<QuerySeries> AggGroupBy(List<Point> pts, ParsedQuery q, CancellationToken cancellationToken, string resultMeasurement)
    {
        long? step = q.GroupByNs;
        var tagNames = q.GroupByTags;
        var groupByAllTags = q.GroupByAllTags;
        var items = q.Select.Where(x => x.Func != "").ToList();
        if (items.Count == 0) items = [new("count", "*", "count")];
        var hasRowExpandingFunctions = items.Any(IsRowExpandingGroupFunction);

        var groups = new Dictionary<(string TagKey, long? BucketTime), List<Point>>();
        foreach (var p in pts)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var tagKey = BuildGroupByTagKey(p.Tags, tagNames, groupByAllTags);
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
            var tagsDict = BuildGroupByTags(key.TagKey, tagNames, groupByAllTags);
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

    private sealed class StreamingAggregateGroup(int itemCount)
    {
        public readonly StreamingAggregateState[] States = Enumerable.Range(0, itemCount).Select(_ => new StreamingAggregateState()).ToArray();
        public long MaxTime;
    }

    private sealed class StreamingAggregateState
    {
        private double _sum;
        private double _min;
        private double _max;
        private int _count;

        public void Add(double value)
        {
            if (_count == 0)
            {
                _min = value;
                _max = value;
            }
            else
            {
                if (value < _min) _min = value;
                if (value > _max) _max = value;
            }

            _sum += value;
            _count++;
        }

        public void AddCount() => _count++;

        public object? Value(string func) => func switch
        {
            "count" => _count,
            "sum" => _count == 0 ? null : _sum,
            "mean" => _count == 0 ? null : _sum / _count,
            "min" => _count == 0 ? null : _min,
            "max" => _count == 0 ? null : _max,
            _ => null
        };
    }

    static bool IsSimpleStreamingAggregate(SelectItem item) => item.Func is "count" or "sum" or "mean" or "min" or "max";

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

        var fieldKeys = e.ListFieldKeys(db, q.Measurement)
            .Select(field => field.Field)
            .ToHashSet(StringComparer.Ordinal);
        HashSet<string>? candidates = null;
        foreach (var filter in q.TagFilters)
        {
            // A quoted string predicate can target a string field. Do not apply tag-index pushdown when ambiguous.
            if (fieldKeys.Contains(filter.Key)) continue;
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

    long EstimateSelectIntoPointBytes(TsdbEngine e, string defaultDb, ParsedQuery q, List<QuerySeries> seriesList)
    {
        var target = ParseIntoTarget(defaultDb, q.IntoTarget!, q.Measurement ?? "result");
        var knownFields = new HashSet<string>(e.ListFieldKeys(q.SourceDatabase ?? defaultDb, q.Measurement).Select(f => f.Field), StringComparer.OrdinalIgnoreCase);
        return EstimateConvertedPointBytes(target.Measurement, seriesList, q, knownFields);
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

    static long EstimateConvertedPointBytes(string measurement, List<QuerySeries> seriesList, ParsedQuery query, HashSet<string> knownFields)
    {
        var selectedColumns = new HashSet<string>(
            query.Select.Select(s => s.Func == "" ? s.Field : s.Alias),
            StringComparer.OrdinalIgnoreCase);

        long total = 0;
        foreach (var series in seriesList)
        {
            var tagKeys = new HashSet<string>(series.Tags?.Keys ?? Enumerable.Empty<string>(), StringComparer.Ordinal);
            if (series.TagColumns != null)
                tagKeys.UnionWith(series.TagColumns);

            foreach (var row in series.Values)
            {
                if (row.Count == 0)
                    continue;

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

                total += EstimatePointBytes(new Point
                {
                    Measurement = measurement,
                    Tags = tags,
                    Fields = fields,
                    TimestampNs = 0
                });
            }
        }

        return total;
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

    static long EstimateFieldRowsBytes(List<long> timestamps, List<FieldValue?[]> rows)
    {
        long size = 48 + timestamps.Count * 8L;
        foreach (var row in rows)
            foreach (var value in row)
                if (value.HasValue)
                    size += EstimateFieldValueBytes(value.Value);
        return size;
    }

    static long EstimateGroupingStateBytes(List<Point> points, List<string> groupByTags, bool groupByAllTags, long? groupByNs)
    {
        if (points.Count == 0)
            return 0;

        var groups = new Dictionary<(string TagKey, long? BucketTime), int>();
        foreach (var point in points)
        {
            var tagKey = BuildGroupByTagKey(point.Tags, groupByTags, groupByAllTags);
            long? bucketTime = groupByNs.HasValue ? point.TimestampNs / groupByNs.Value * groupByNs.Value : null;
            var key = (tagKey, bucketTime);
            groups.TryGetValue(key, out var count);
            groups[key] = count + 1;
        }

        long size = 0;
        foreach (var group in groups)
            size += 112 + EstimateStringBytes(group.Key.TagKey) + 16 + group.Value * 8L;
        return size;
    }

    static long EstimateFunctionStateBytes(List<Point> points, List<SelectItem> items)
    {
        long timelineBytes = 0;
        long maxInputWindowBytes = 0;
        var allTimesUpperBound = new HashSet<long>();

        foreach (var item in items)
        {
            var matchingCount = CountMatchingNumericPoints(points, item.Field);
            if (matchingCount == 0)
                continue;

            var outputCount = EstimateFunctionOutputCount(matchingCount, item);
            timelineBytes += EstimateTimelineBytes(outputCount);
            maxInputWindowBytes = Math.Max(maxInputWindowBytes, EstimateFunctionInputBytes(matchingCount));

            foreach (var point in points)
            {
                if (item.Field == "*")
                {
                    allTimesUpperBound.Add(point.TimestampNs);
                    continue;
                }

                if (point.Fields.TryGetValue(item.Field, out var value) && value.AsDouble().HasValue)
                    allTimesUpperBound.Add(point.TimestampNs);
            }
        }

        return timelineBytes + maxInputWindowBytes + allTimesUpperBound.Count * 8L;
    }

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

    static long EstimateRowBytes(List<object?> row)
    {
        long size = 0;
        foreach (var value in row)
            size += EstimateObjectBytes(value);
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

    static int CountMatchingNumericPoints(List<Point> points, string field)
    {
        if (field == "*")
            return points.Count;

        return points.Count(point =>
            point.Fields.TryGetValue(field, out var value)
            && value.AsDouble().HasValue);
    }

    static int EstimateFunctionOutputCount(int matchingCount, SelectItem item)
    {
        if (matchingCount <= 0)
            return 0;

        return item.Func switch
        {
            "difference" or "derivative" or "non_negative_derivative" or "integral" or "elapsed" => Math.Max(0, matchingCount - 1),
            "moving_average" => Math.Max(0, matchingCount - Math.Max(1, (int)item.Param) + 1),
            "cumulative_sum" => matchingCount,
            "top" or "bottom" or "sample" => Math.Min(matchingCount, Math.Max(1, (int)item.Param)),
            _ => 1
        };
    }

    static long EstimateFunctionInputBytes(int matchingCount) => 96 + matchingCount * 40L;

    static long EstimateTimelineBytes(int outputCount) => 96 + outputCount * 48L;

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
        return pts.Where(point => MatchesTagFilters(point, filters)).ToList();
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
        var targetDb = q.SourceDatabase ?? db;
        var targetRp = q.SourceRpName;
        if (q.TagFilters.Count == 0 && q.FieldFilters.Count == 0)
        {
            if (targetRp != null)
                e.DeleteFromMeasurement(targetDb, targetRp, q.Measurement!, q.MinTimeNs, q.MaxTimeNs);
            else
                e.DeleteFromMeasurement(targetDb, q.Measurement!, q.MinTimeNs, q.MaxTimeNs);
            return;
        }

        if (targetRp != null)
        {
            e.DeleteFromMeasurement(targetDb, targetRp, q.Measurement!, q.MinTimeNs, q.MaxTimeNs, p =>
                MatchesTagFilters(p, q.TagFilters) && MatchesFieldFilters(p, q.FieldFilters));
            return;
        }

        e.DeleteFromMeasurement(targetDb, q.Measurement!, q.MinTimeNs, q.MaxTimeNs, p =>
            MatchesTagFilters(p, q.TagFilters) && MatchesFieldFilters(p, q.FieldFilters));
    }

    static void DropSeries(TsdbEngine e, string db, ParsedQuery q)
    {
        var measurements = q.Measurements.Count > 0
            ? q.Measurements
            : q.Measurement != null ? [q.Measurement] : e.ListMeasurements(db).ToList();
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
        if (item.Func == "count" && item.Field == "*")
        {
            var countResult = new SortedDictionary<long, object?>();
            if (pts.Count > 0) countResult[pts.Max(p => p.TimestampNs)] = pts.Count;
            return countResult;
        }

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

        var countStarFields = IsCountStar(items)
            ? e.Schema.GetFields(db, q.Measurement).Select(field => field.FieldKey).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList()
            : [];
        if (IsCountStar(items) && countStarFields.Count == 0) return null;
        var aggregateFields = countStarFields.Count == 0 ? requestedFields : new HashSet<string>(countStarFields, StringComparer.Ordinal);
        var aggregateItems = countStarFields.Count == 0
            ? items
            : countStarFields.Select(field => new SelectItem("count", field, CountStarAlias(field))).ToList();

        var bufferPoints = e.ReadBufferedPoints(db, rp, q.Measurement, q.MinTimeNs, q.MaxTimeNs, aggregateFields, seriesFilter);
        report.ScannedPoints += bufferPoints.Count;
        var dedupedBufferPoints = DeduplicateAggregatePoints(bufferPoints);
        var metadata = e.ReadSegmentMetadataWithStats(db, rp, q.Measurement, q.MinTimeNs, q.MaxTimeNs, aggregateFields, seriesFilter, cancellationToken);
        report.SegmentMetadataFooterHits += metadata.FooterHits;
        report.SegmentMetadataFullReads += metadata.FullReads;
        var metas = metadata.Metas;
        if (metas.Count == 0 && dedupedBufferPoints.Count == 0) return null;
        if (metas.Count > 0)
        {
            if (metas.Any(m => !IsFullCoverage(m, q.MinTimeNs, q.MaxTimeNs) || m.Stats == null)) return null;
            if (HasPotentialAggregateDuplicates(metas, dedupedBufferPoints, aggregateItems)) return null;
            report.ScannedPoints += metas.Sum(m => m.PointCount);
        }

        var row = new List<object?> { Time(MaxTime(metas, dedupedBufferPoints)) };
        foreach (var item in aggregateItems)
        {
            var relevantMetas = metas.Where(m => m.Field == item.Field).ToList();
            if (relevantMetas.Count == 0 && !dedupedBufferPoints.Any(p => p.Fields.ContainsKey(item.Field))) return null;
            row.Add(CalcPushdownValue(item.Func, item.Field, relevantMetas, dedupedBufferPoints));
        }

        report.UsedAggregatePushdown = true;
        return [new QuerySeries
        {
            Name = q.Measurement ?? "",
            Columns = ["time", .. aggregateItems.Select(i => i.Alias)],
            Values = [row]
        }];
    }

    /// <summary>
    /// Fast count fallback used when the metadata-based aggregate pushdown fails (e.g. due to
    /// overlapping segments or missing stats). Reads only timestamp blocks from segments,
    /// skipping the expensive value-block decoding that ReadAllPoints performs.
    /// Handles count(*) and count(field) without GROUP BY time/tags.
    /// </summary>
    List<QuerySeries>? TryFastCountFallback(
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
        // Only handle count queries (not sum, min, max, mean, etc.)
        if (items.Any(i => i.Func != "count")) return null;
        if (q.FieldFilters.Count > 0) return null;

        // For count(*), expand to per-field counts (InfluxDB 1.x semantics).
        var countStarFields = IsCountStar(items)
            ? e.Schema.GetFields(db, q.Measurement).Select(field => field.FieldKey).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList()
            : [];
        if (IsCountStar(items) && countStarFields.Count == 0) return null;

        var aggregateFields = countStarFields.Count == 0 ? requestedFields : new HashSet<string>(countStarFields, StringComparer.Ordinal);
        var aggregateItems = countStarFields.Count == 0
            ? items
            : countStarFields.Select(field => new SelectItem("count", field, CountStarAlias(field))).ToList();

        var countResult = e.CountPointsByField(db, rp, q.Measurement, q.MinTimeNs, q.MaxTimeNs, aggregateFields, seriesFilter, cancellationToken);
        if (countResult == null) return null;

        report.ScannedPoints += countResult.ScannedPoints;
        report.UsedAggregatePushdown = true;

        var row = new List<object?> { Time(countResult.MaxTimestampNs) };
        foreach (var item in aggregateItems)
            row.Add(countResult.FieldCounts.TryGetValue(item.Field, out var count) ? (object?)count : 0);

        return [new QuerySeries
        {
            Name = q.Measurement ?? "",
            Columns = ["time", .. aggregateItems.Select(i => i.Alias)],
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
        if (q.GroupByTags.Count == 0 || q.GroupByNs.HasValue || q.GroupByAllTags)
            return null;

        var items = q.Select.Where(s => s.Func != "").ToList();
        if (items.Count == 0)
            return null;
        if (items.Any(i => i.Func is "difference" or "derivative" or "non_negative_derivative" or "moving_average" or "cumulative_sum" or "elapsed" or "top" or "bottom" or "sample" or "integral" or "percentile" or "median" or "stddev" or "spread" or "first" or "last"))
            return null;
        if (q.FieldFilters.Count > 0)
            return null;
        var countStarFields = IsCountStar(items)
            ? e.Schema.GetFields(db, q.Measurement).Select(field => field.FieldKey).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToList()
            : [];
        if (IsCountStar(items) && countStarFields.Count == 0)
            return null;
        if (items.Any(i => i.Field == "*") && countStarFields.Count == 0)
            return null;
        var aggregateFields = countStarFields.Count == 0 ? requestedFields : new HashSet<string>(countStarFields, StringComparer.Ordinal);
        var aggregateItems = countStarFields.Count == 0
            ? items
            : countStarFields.Select(field => new SelectItem("count", field, CountStarAlias(field))).ToList();

        var bufferPoints = e.ReadBufferedPoints(db, rp, q.Measurement, q.MinTimeNs, q.MaxTimeNs, aggregateFields, seriesFilter);
        report.ScannedPoints += bufferPoints.Count;
        var dedupedBufferPoints = DeduplicateAggregatePoints(bufferPoints);

        var metadata = e.ReadSegmentMetadataWithStats(db, rp, q.Measurement, q.MinTimeNs, q.MaxTimeNs, aggregateFields, seriesFilter, cancellationToken);
        report.SegmentMetadataFooterHits += metadata.FooterHits;
        report.SegmentMetadataFullReads += metadata.FullReads;
        var metas = metadata.Metas;
        if (metas.Count == 0 && dedupedBufferPoints.Count == 0)
            return null;
        if (metas.Count > 0)
        {
            if (metas.Any(m => !IsFullCoverage(m, q.MinTimeNs, q.MaxTimeNs) || m.Stats == null))
                return null;
            if (HasPotentialAggregateDuplicates(metas, dedupedBufferPoints, aggregateItems))
                return null;
            report.ScannedPoints += metas.Sum(m => m.PointCount);
        }

        var parsedTags = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
        var groupedMetas = metas.GroupBy(meta =>
        {
            if (!parsedTags.TryGetValue(meta.TagsCanonical, out var tags))
            {
                tags = ParseTagsCanonical(meta.TagsCanonical);
                parsedTags[meta.TagsCanonical] = tags;
            }
            return BuildGroupByTagKey(tags, q.GroupByTags, q.GroupByAllTags);
        }).ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);

        var groupedBufferPoints = dedupedBufferPoints.GroupBy(point => BuildGroupByTagKey(point.Tags, q.GroupByTags, q.GroupByAllTags))
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
            foreach (var item in aggregateItems)
            {
                var relevantMetas = groupMetas.Where(m => m.Field == item.Field).ToList();
                if (relevantMetas.Count == 0 && !groupBufferPoints.Any(p => p.Fields.ContainsKey(item.Field)))
                    return null;
                row.Add(CalcPushdownValue(item.Func, item.Field, relevantMetas, groupBufferPoints));
            }

            result.Add(new QuerySeries
            {
                Name = resultMeasurement,
                Tags = BuildGroupByTags(groupKey, q.GroupByTags, q.GroupByAllTags),
                Columns = ["time", .. aggregateItems.Select(i => i.Alias)],
                Values = [row]
            });
        }

        report.UsedAggregatePushdown = true;
        return ApplySeriesWindow(result, q.SeriesOffset, q.SeriesLimit);
    }

    static bool IsFullCoverage(SegmentColumnMeta meta, long? minTimeNs, long? maxTimeNs) =>
        (!minTimeNs.HasValue || minTimeNs.Value <= meta.MinTime) && (!maxTimeNs.HasValue || maxTimeNs.Value >= meta.MaxTime);

    static bool HasPotentialAggregateDuplicates(List<SegmentColumnMeta> metas, List<Point> bufferPoints, List<SelectItem> items)
    {
        var fields = items.Select(i => i.Field).Where(f => f != "*").ToHashSet(StringComparer.Ordinal);
        foreach (var group in metas
            .Where(m => fields.Contains(m.Field))
            .GroupBy(m => (m.Measurement, m.TagsCanonical, m.Field)))
        {
            long? maxTime = null;
            foreach (var meta in group.OrderBy(m => m.MinTime))
            {
                if (maxTime.HasValue && meta.MinTime <= maxTime.Value)
                    return true;
                maxTime = maxTime.HasValue ? Math.Max(maxTime.Value, meta.MaxTime) : meta.MaxTime;
            }
        }

        foreach (var point in bufferPoints)
        {
            var tags = ToCanonicalTagKey(point.Tags);
            foreach (var field in fields)
            {
                if (!point.Fields.ContainsKey(field))
                    continue;
                if (metas.Any(m => m.Measurement == point.Measurement
                    && m.TagsCanonical == tags
                    && m.Field == field
                    && point.TimestampNs >= m.MinTime
                    && point.TimestampNs <= m.MaxTime))
                    return true;
            }
        }

        return false;
    }

    static List<Point> DeduplicateAggregatePoints(List<Point> points)
    {
        if (points.Count <= 1) return points;
        var map = new Dictionary<(string Measurement, string Tags, long Timestamp), Point>();
        foreach (var point in points)
        {
            var key = (point.Measurement, ToCanonicalTagKey(point.Tags), point.TimestampNs);
            if (map.TryGetValue(key, out var existing))
            {
                foreach (var field in point.Fields)
                    existing.Fields[field.Key] = field.Value;
            }
            else
            {
                map[key] = new Point
                {
                    Measurement = point.Measurement,
                    Tags = point.Tags,
                    Fields = new Dictionary<string, FieldValue>(point.Fields, StringComparer.Ordinal),
                    TimestampNs = point.TimestampNs
                };
            }
        }
        return map.Values.ToList();
    }

    static long MaxTime(List<SegmentColumnMeta> metas, List<Point> bufferPoints)
    {
        var metaMax = metas.Count == 0 ? 0 : metas.Max(m => m.MaxTime);
        var bufferMax = bufferPoints.Count == 0 ? 0 : bufferPoints.Max(p => p.TimestampNs);
        return Math.Max(metaMax, bufferMax);
    }

    static long MaxTime(List<SegmentColumnMeta> metas, BufferedStatsSnapshot bufferStats)
    {
        var metaMax = metas.Count == 0 ? 0 : metas.Max(m => m.MaxTime);
        return Math.Max(metaMax, bufferStats.MaxTime);
    }

    static object? CalcPushdownValue(string func, string field, List<SegmentColumnMeta> metas, BufferedStatsSnapshot bufferStats)
    {
        bufferStats.Fields.TryGetValue(field, out var bufferFieldStats);
        var metaStats = metas.Select(m => m.Stats!).ToList();
        return func switch
        {
            "count" => metas.Sum(m => m.PointCount) + bufferFieldStats.Count,
            "sum" => metaStats.Sum(s => s.Sum) + bufferFieldStats.Sum,
            "min" => CombineMin(metaStats, bufferFieldStats),
            "max" => CombineMax(metaStats, bufferFieldStats),
            "mean" => CombineMean(metaStats, bufferFieldStats),
            _ => null
        };
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

    static bool IsCountStar(List<SelectItem> items) =>
        items.Count == 1 && items[0].Func == "count" && items[0].Field == "*";

    static string CountStarAlias(string field) => $"count_{field}";

    static string BuildGroupByTagKey(IReadOnlyDictionary<string, string> tags, List<string> groupByTags, bool groupByAllTags)
    {
        if (groupByAllTags)
            return ToCanonicalTagKey(tags);
        if (groupByTags.Count == 0)
            return string.Empty;
        return string.Join("|", groupByTags.Select(tag => tags.TryGetValue(tag, out var value) ? value : ""));
    }

    static Dictionary<string, string>? BuildGroupByTags(string groupKey, List<string> groupByTags, bool groupByAllTags)
    {
        if (groupByAllTags)
            return string.IsNullOrEmpty(groupKey) ? null : ParseTagsCanonical(groupKey);
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

    static string ToCanonicalTagKey(IReadOnlyDictionary<string, string> tags) =>
        string.Join(",",
            tags.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => $"{kv.Key}={kv.Value}"));

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

    static object? CombineMin(List<BlockStats> stats, BufferedFieldStats bufferStats)
    {
        if (stats.Count == 0 && bufferStats.Count == 0) return null;
        var min = stats.Count == 0 ? bufferStats.Min : stats.Min(s => s.Min);
        return bufferStats.Count == 0 ? min : Math.Min(min, bufferStats.Min);
    }

    static object? CombineMax(List<BlockStats> stats, BufferedFieldStats bufferStats)
    {
        if (stats.Count == 0 && bufferStats.Count == 0) return null;
        var max = stats.Count == 0 ? bufferStats.Max : stats.Max(s => s.Max);
        return bufferStats.Count == 0 ? max : Math.Max(max, bufferStats.Max);
    }

    static object? CombineMean(List<BlockStats> stats, BufferedFieldStats bufferStats)
    {
        var sum = stats.Sum(s => s.Sum) + bufferStats.Sum;
        var count = stats.Sum(s => s.Count) + bufferStats.Count;
        return count == 0 ? null : sum / count;
    }

    static int CountRows(QueryResponse response) =>
        response.Results.SelectMany(r => r.Series ?? []).Sum(s => s.Values.Count);

    static bool CanStreamRawSelect(ParsedQuery q) =>
        q.Kind == QueryKind.Select
        && q.Subquery == null
        && q.GroupByNs == null
        && q.GroupByTags.Count == 0
        && !q.GroupByAllTags
        && q.Select.All(s => string.IsNullOrEmpty(s.Func))
        && string.IsNullOrWhiteSpace(q.IntoTarget)
        && !q.Desc
        && !string.IsNullOrWhiteSpace(q.Measurement);

    static bool CanStreamRawSelectResponse(TsdbEngine e, string? db, ParsedQuery q)
    {
        if (!CanStreamRawSelect(q))
            return false;
        Req(q.SourceDatabase ?? db);
        var sourceDb = q.SourceDatabase ?? db!;
        var sourceRp = q.SourceRpName ?? e.GetDefaultRpName(sourceDb);
        return !e.HasSegments(sourceDb, sourceRp, q.MinTimeNs, q.MaxTimeNs);
    }

    static bool CanWriteBufferedRawDescendingJson(ParsedQuery q) =>
        q.Kind == QueryKind.Select
        && q.Subquery == null
        && q.GroupByNs == null
        && q.GroupByTags.Count == 0
        && !q.GroupByAllTags
        && q.Select.All(s => string.IsNullOrEmpty(s.Func))
        && string.IsNullOrWhiteSpace(q.IntoTarget)
        && q.Desc
        && q.FieldFilters.Count == 0
        && !string.IsNullOrWhiteSpace(q.Measurement);

    static QueryJsonExecutionOutcome ErrorJsonOutcome(QueryExecutionReport report)
    {
        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        using (var writer = new System.Text.Json.Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("results");
            writer.WriteStartArray();
            writer.WriteStartObject();
            writer.WriteNumber("statement_id", 0);
            writer.WriteString("error", report.Error);
            writer.WriteEndObject();
            writer.WriteEndArray();
            writer.WriteEndObject();
        }
        return new QueryJsonExecutionOutcome { Json = buffer.WrittenSpan.ToArray(), Report = report };
    }

    static void WriteRawJsonResponse(
        System.Text.Json.Utf8JsonWriter writer,
        string measurement,
        Dictionary<string, string> seriesTags,
        List<string> columns,
        List<string> fields,
        List<Point> points,
        int offset,
        int rowLimit,
        long epochDivisor,
        QueryExecutionReport report,
        ref long resultBytes)
    {
        writer.WriteStartObject();
        writer.WritePropertyName("results");
        writer.WriteStartArray();
        writer.WriteStartObject();
        writer.WriteNumber("statement_id", 0);
        writer.WritePropertyName("series");
        writer.WriteStartArray();
        writer.WriteStartObject();
        writer.WriteString("name", measurement);
        if (seriesTags.Count > 0)
        {
            writer.WritePropertyName("tags");
            writer.WriteStartObject();
            foreach (var (key, value) in seriesTags)
                writer.WriteString(key, value);
            writer.WriteEndObject();
        }
        writer.WritePropertyName("columns");
        writer.WriteStartArray();
        foreach (var column in columns)
            writer.WriteStringValue(column);
        writer.WriteEndArray();
        writer.WritePropertyName("values");
        writer.WriteStartArray();

        var emitted = 0;
        for (var i = offset; i < points.Count && emitted < rowLimit; i++)
        {
            var point = points[i];
            writer.WriteStartArray();
            long rowBytes = 32;
            if (epochDivisor > 0)
            {
                var timestamp = point.TimestampNs / epochDivisor;
                writer.WriteNumberValue(timestamp);
                rowBytes += EstimateObjectBytes(timestamp);
            }
            else
            {
                var timestamp = Time(point.TimestampNs);
                writer.WriteStringValue(timestamp);
                rowBytes += EstimateObjectBytes(timestamp);
            }
            foreach (var field in fields)
            {
                point.Fields.TryGetValue(field, out var fieldValue);
                var value = fieldValue.ToObject();
                WriteRawJsonValue(writer, value);
                rowBytes += EstimateObjectBytes(value);
            }
            writer.WriteEndArray();
            emitted++;
            resultBytes += rowBytes;
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
        writer.WriteEndArray();
        writer.WriteEndObject();
        writer.WriteEndArray();
        writer.WriteEndObject();
        report.RowsReturned = emitted;
    }

    static void WriteFieldRowsRawJsonResponse(
        System.Text.Json.Utf8JsonWriter writer,
        string measurement,
        Dictionary<string, string> seriesTags,
        List<string> columns,
        TsdbEngine.DescendingFieldsReadResult read,
        int offset,
        int rowLimit,
        long epochDivisor,
        QueryExecutionReport report,
        ref long resultBytes)
    {
        writer.WriteStartObject();
        writer.WritePropertyName("results");
        writer.WriteStartArray();
        writer.WriteStartObject();
        writer.WriteNumber("statement_id", 0);
        writer.WritePropertyName("series");
        writer.WriteStartArray();
        writer.WriteStartObject();
        writer.WriteString("name", measurement);
        if (seriesTags.Count > 0)
        {
            writer.WritePropertyName("tags");
            writer.WriteStartObject();
            foreach (var (key, value) in seriesTags)
                writer.WriteString(key, value);
            writer.WriteEndObject();
        }

        writer.WritePropertyName("columns");
        writer.WriteStartArray();
        foreach (var column in columns)
            writer.WriteStringValue(column);
        writer.WriteEndArray();
        writer.WritePropertyName("values");
        writer.WriteStartArray();

        var emitted = 0;
        for (var i = offset; i < read.Timestamps.Count && emitted < rowLimit; i++)
        {
            writer.WriteStartArray();
            long rowBytes = 32;
            if (epochDivisor > 0)
            {
                var timestamp = read.Timestamps[i] / epochDivisor;
                writer.WriteNumberValue(timestamp);
                rowBytes += EstimateObjectBytes(timestamp);
            }
            else
            {
                var timestamp = Time(read.Timestamps[i]);
                writer.WriteStringValue(timestamp);
                rowBytes += EstimateObjectBytes(timestamp);
            }

            foreach (var fieldValue in read.Rows[i])
            {
                var value = fieldValue?.ToObject();
                WriteRawJsonValue(writer, value);
                rowBytes += EstimateObjectBytes(value);
            }
            writer.WriteEndArray();
            emitted++;
            resultBytes += rowBytes;
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
        writer.WriteEndArray();
        writer.WriteEndObject();
        writer.WriteEndArray();
        writer.WriteEndObject();
        report.RowsReturned = emitted;
    }

    static long ParseEpochDivisor(string? epoch) => epoch switch
    {
        "ns" => 1,
        "u" or "µ" => 1_000,
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        "m" => 60L * 1_000_000_000,
        "h" => 3600L * 1_000_000_000,
        _ => 0
    };

    static void WriteRawJsonValue(System.Text.Json.Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case string text:
                writer.WriteStringValue(text);
                break;
            case bool boolean:
                writer.WriteBooleanValue(boolean);
                break;
            case int number:
                writer.WriteNumberValue(number);
                break;
            case long number:
                writer.WriteNumberValue(number);
                break;
            case double number:
                writer.WriteNumberValue(number);
                break;
            case float number:
                writer.WriteNumberValue(number);
                break;
            default:
                writer.WriteStringValue(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture));
                break;
        }
    }

    static HashSet<string>? BuildRequestedFields(ParsedQuery q)
    {
        HashSet<string>? requestedFields = null;
        if (q.Select.Count > 0 && !(q.Select.Count == 1 && q.Select[0].Field == "*"))
        {
            requestedFields = new HashSet<string>(q.Select.Select(x => x.Field).Where(field => field != "*"), StringComparer.Ordinal);
            if (requestedFields.Count == 0) requestedFields = null;
        }
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
            var tagVal = point.Tags.TryGetValue(f.Key, out var v)
                ? v
                : point.Fields.TryGetValue(f.Key, out var field) && field.Kind == FieldKind.String
                    ? field.String
                    : null;
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
        var row = new List<object?>(1 + tags.Count + fields.Count) { Time(point.TimestampNs) };
        foreach (var tag in tags)
            row.Add(point.Tags.TryGetValue(tag, out var value)
                ? value
                : point.Fields.TryGetValue(tag, out var legacyTag) && legacyTag.Kind == FieldKind.String
                    ? legacyTag.String
                    : null);
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
        target.UsedStreamingRawSelect = source.UsedStreamingRawSelect;
        target.UsedStreamingAggregate = source.UsedStreamingAggregate;
        target.SegmentMetadataFooterHits = source.SegmentMetadataFooterHits;
        target.SegmentMetadataFullReads = source.SegmentMetadataFullReads;
        target.SegmentColumnsRead = source.SegmentColumnsRead;
        target.PointsMaterialized = source.PointsMaterialized;
        target.LimitPushdownStopReason = source.LimitPushdownStopReason;
        target.EstimatedInputBytes = source.EstimatedInputBytes;
        target.EstimatedResultBytes = source.EstimatedResultBytes;
        target.PeakEstimatedMemoryBytes = source.PeakEstimatedMemoryBytes;
        target.Error = source.Error;
    }

    static void ApplyRawReadReport(QueryExecutionReport report, TsdbEngine.DescendingSeriesReadResult read)
    {
        report.SegmentColumnsRead += read.SegmentColumnsRead;
        report.PointsMaterialized += read.PointsMaterialized;
        report.LimitPushdownStopReason = read.LimitPushdownStopReason;
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
