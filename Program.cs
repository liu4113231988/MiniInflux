using System.IO.Compression;
using System.Text;
using System.Text.Json;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

var builder = WebApplication.CreateSlimBuilder(args);
var options = MiniInfluxOptions.Load(builder.Configuration);
BackupManager.ApplyPendingRestore(options.DataPath);

if (args.Length > 0 && string.Equals(args[0], "benchmark", StringComparison.OrdinalIgnoreCase))
{
    var benchmarkRoot = Path.Combine(options.DataPath, "benchmarks", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString());
    var benchmarkOptions = ParseBenchmarkOptions(args.Skip(1).ToArray());
    var benchmarkFormat = ParseBenchmarkFormat(args.Skip(1).ToArray());
    var benchmark = BenchmarkRunner.Run(benchmarkRoot, benchmarkOptions);
    Console.WriteLine(benchmarkFormat switch
    {
        "json" => BenchmarkRunner.FormatJson(benchmark),
        "prometheus" => BenchmarkRunner.FormatPrometheus(benchmark),
        _ => BenchmarkRunner.FormatText(benchmark)
    });
    return;
}

builder.WebHost.UseUrls(options.Urls);
if (options.Tls.Enabled && !string.IsNullOrWhiteSpace(options.Tls.CertPath))
{
    builder.WebHost.ConfigureKestrel(k =>
    {
        k.ListenAnyIP(options.Tls.Port, listen => listen.UseHttps(options.Tls.CertPath, options.Tls.Password));
    });
}

builder.Services.ConfigureHttpJsonOptions(jsonOptions =>
{
    jsonOptions.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonContext.Default);
});

var engine = new TsdbEngine(
    options.DataPath,
    options.FlushThreshold,
    options.Wal.MaxWalFileBytes,
    options.Wal.Fsync,
    options.Wal.FsyncIntervalMs,
    options.Storage.RpCheckIntervalMs,
    options.Storage.MaxSeriesPerDatabase,
    options.Storage.MaxFieldsPerMeasurement,
    maxBufferPoints: options.Storage.MaxBufferPoints,
    maxBufferBytes: options.Storage.MaxBufferBytes);
var authStore = new AuthStore(options.DataPath);

builder.Services.AddSingleton(engine);
builder.Services.AddSingleton(authStore);
builder.Services.AddSingleton(new QueryExecutor(
    options.Storage.MaxResponseRows,
    options.Storage.MaxQueryPoints,
    options.Storage.MaxQueryDurationMs,
    options.Storage.MaxQueryMemoryBytes,
    authStore));
builder.Services.AddSingleton(options);
builder.Services.AddSingleton<MetricsCollector>();
builder.Services.AddSingleton(sp => new WriteQueue(sp.GetRequiredService<TsdbEngine>(), options.Write.QueueCapacity, options.Write.BatchSize));

var app = builder.Build();

engine.Recover();

app.MapGet("/ping", () => Results.NoContent());

app.MapGet("/health", () => Results.Ok(new { name = "miniinflux", message = "ready", status = "pass" }));

app.MapGet("/debug/stats", (HttpRequest request, MetricsCollector metrics) =>
{
    if (!EnsureAuthorized(request, options, authStore, AuthPermission.Admin, null, out _, out var authResult))
        return authResult;
    var stats = metrics.CollectStats();
    return Results.Json(stats, AppJsonContext.Default.DebugStats);
});

app.MapGet("/metrics", (HttpRequest request, MetricsCollector metrics) =>
{
    if (!EnsureAuthorized(request, options, authStore, AuthPermission.Admin, null, out _, out var authResult))
        return authResult;
    var text = metrics.FormatPrometheus();
    return Results.Text(text, "text/plain; version=0.0.4; charset=utf-8");
});

app.MapPost("/write", async (HttpRequest request, TsdbEngine tsdbEngine, WriteQueue writeQueue, MetricsCollector metrics, string db, string? rp, string? precision) =>
{
    if (!EnsureAuthorized(request, options, authStore, AuthPermission.Write, db, out _, out var authResult))
        return authResult;
    if (string.IsNullOrWhiteSpace(db)) return Results.BadRequest(new ErrorResponse("missing required parameter db"));
    if (request.ContentLength > options.Write.MaxRequestBodyBytes)
        return Results.StatusCode(413);

    Stream input = request.Body;
    if (request.Headers.ContentEncoding.ToString().Contains("gzip", StringComparison.OrdinalIgnoreCase))
        input = new GZipStream(request.Body, CompressionMode.Decompress);

    using var reader = new StreamReader(input, Encoding.UTF8);
    var body = await reader.ReadToEndAsync();
    if (string.IsNullOrWhiteSpace(body)) return Results.NoContent();

    try
    {
        var points = LineProtocolParser.ParseMany(body, TimestampPrecision.Parse(precision));

        try
        {
            await writeQueue.EnqueueAsync(db, rp ?? "autogen", points, request.HttpContext.RequestAborted);
            metrics.RecordWrite(points.Count);
            return Results.NoContent();
        }
        catch (WriteQueueFullException)
        {
            return Results.StatusCode(429);
        }
        catch (FieldConflictException ex)
        {
            return Results.BadRequest(new ErrorResponse(ex.Message));
        }
        catch (CardinalityLimitExceededException)
        {
            return Results.StatusCode(429);
        }
        catch (MemoryLimitExceededException)
        {
            return Results.StatusCode(429);
        }
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new ErrorResponse(ex.Message));
    }
});

app.MapMethods("/query", ["GET", "POST"], async (HttpRequest request, QueryExecutor executor, TsdbEngine tsdbEngine, MetricsCollector metrics, string? db, string? q) =>
{
    var chunked = TryParseBool(request.Query["chunked"].ToString());
    var chunkSize = ParseChunkSize(request.Query["chunk_size"].ToString());
    if (string.IsNullOrWhiteSpace(q) && request.HasFormContentType)
    {
        var form = await request.ReadFormAsync();
        q = form["q"];
        db ??= form["db"];
        chunked = chunked || TryParseBool(form["chunked"].ToString());
        chunkSize ??= ParseChunkSize(form["chunk_size"].ToString());
    }

    if (string.IsNullOrWhiteSpace(q))
        return Results.BadRequest(new ErrorResponse("missing required parameter q"));

    ParsedQuery? parsed = null;
    try { parsed = InfluxQlParser.Parse(q); } catch { }

    if (!EnsureQueryAuthorized(request, options, authStore, db, parsed, out var authResult))
        return authResult;

    var outcome = executor.ExecuteWithReport(tsdbEngine, db, q, request.HttpContext.RequestAborted);
    metrics.RecordQuery(outcome.Report);
    if (chunked)
        return ChunkedResult(outcome.Response, chunkSize ?? 5000);
    return Results.Json(outcome.Response, AppJsonContext.Default.QueryResponse);
});

app.MapPost("/admin/backup", (HttpRequest request, TsdbEngine tsdbEngine, string path) =>
{
    if (!EnsureAuthorized(request, options, authStore, AuthPermission.Admin, null, out _, out var authResult))
        return authResult;
    tsdbEngine.FlushAll();
    BackupManager.CreateBackup(tsdbEngine.RootPath, path);
    return Results.Ok(new AdminMessage("backup completed"));
});

app.MapPost("/admin/restore", (HttpRequest request, TsdbEngine tsdbEngine, string path) =>
{
    if (!EnsureAuthorized(request, options, authStore, AuthPermission.Admin, null, out _, out var authResult))
        return authResult;
    try
    {
        BackupManager.PrepareRestore(path, tsdbEngine.RootPath);
        return Results.Ok(new AdminMessage("restore prepared; restart required"));
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new ErrorResponse(ex.Message));
    }
});

app.MapGet("/debug/benchmark", (HttpRequest request, TsdbEngine tsdbEngine) =>
{
    if (!EnsureAuthorized(request, options, authStore, AuthPermission.Admin, null, out _, out var authResult))
        return authResult;
    var sw = System.Diagnostics.Stopwatch.StartNew();
    var dbCount = tsdbEngine.ListDatabases().Count;
    var buffered = tsdbEngine.GetBufferedPointCount();
    var bufferedBytes = tsdbEngine.GetBufferedByteCount();
    sw.Stop();
    return Results.Ok(new BenchmarkSnapshot(dbCount, buffered, bufferedBytes, sw.Elapsed.TotalMilliseconds));
});

app.Lifetime.ApplicationStopping.Register(() =>
{
    var wq = app.Services.GetRequiredService<WriteQueue>();
    wq.Dispose();
    var eng = app.Services.GetRequiredService<TsdbEngine>();
    eng.Dispose();
});

app.Run();

static bool EnsureQueryAuthorized(HttpRequest request, MiniInfluxOptions options, AuthStore authStore, string? db, ParsedQuery? parsed, out IResult result)
{
    result = Results.Unauthorized();
    if (parsed == null)
        return EnsureAuthorized(request, options, authStore, AuthPermission.Read, db, out _, out result);

    if (parsed.Kind is QueryKind.ShowUsers or QueryKind.ShowGrants or QueryKind.CreateUser or QueryKind.GrantPrivilege or QueryKind.RevokePrivilege or QueryKind.DropUser
        or QueryKind.CreateDatabase or QueryKind.DropDatabase or QueryKind.CreateRetentionPolicy or QueryKind.AlterRetentionPolicy
        or QueryKind.DropRetentionPolicy or QueryKind.DropShard)
    {
        return EnsureAuthorized(request, options, authStore, AuthPermission.Admin, parsed.Database ?? db, out _, out result);
    }

    if (parsed.Kind is QueryKind.DropMeasurement or QueryKind.DropSeries or QueryKind.Delete)
        return EnsureAuthorized(request, options, authStore, AuthPermission.Write, db, out _, out result);

    if (parsed.Kind == QueryKind.Select)
    {
        var sourceDb = parsed.SourceDatabase ?? db;
        if (!EnsureAuthorized(request, options, authStore, AuthPermission.Read, sourceDb, out _, out result))
            return false;
        if (!string.IsNullOrWhiteSpace(parsed.IntoTarget))
        {
            var targetDb = GetIntoTargetDatabase(db, parsed.IntoTarget!);
            if (!EnsureAuthorized(request, options, authStore, AuthPermission.Write, targetDb, out _, out result))
                return false;
        }
        return true;
    }

    return EnsureAuthorized(request, options, authStore, AuthPermission.Read, db, out _, out result);
}

static bool EnsureAuthorized(HttpRequest request, MiniInfluxOptions options, AuthStore authStore, AuthPermission permission, string? db, out AuthIdentity? identity, out IResult result)
{
    result = Results.Unauthorized();
    if (!options.Auth.Enabled)
    {
        identity = new AuthIdentity("anonymous", true, new Dictionary<string, string>(StringComparer.Ordinal));
        return true;
    }

    if (!TryAuthenticate(request, options, authStore, out identity))
    {
        result = Results.Json(new ErrorResponse("unauthorized"), AppJsonContext.Default.ErrorResponse, statusCode: 401);
        return false;
    }

    if (permission == AuthPermission.Admin)
    {
        if (identity!.IsAdmin) return true;
        result = Results.Json(new ErrorResponse("forbidden"), AppJsonContext.Default.ErrorResponse, statusCode: 403);
        return false;
    }

    if (string.IsNullOrWhiteSpace(db) || authStore.IsAuthorized(identity, db!, permission))
        return true;

    result = Results.Json(new ErrorResponse("forbidden"), AppJsonContext.Default.ErrorResponse, statusCode: 403);
    return false;
}

static bool TryAuthenticate(HttpRequest request, MiniInfluxOptions options, AuthStore authStore, out AuthIdentity? identity)
{
    identity = null;
    var user = request.Query["u"].ToString();
    var pass = request.Query["p"].ToString();
    if (TryValidateUser(options, authStore, user, pass, out identity))
        return true;

    var auth = request.Headers.Authorization.ToString();
    if (auth.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase))
    {
        try
        {
            var raw = Encoding.UTF8.GetString(Convert.FromBase64String(auth["Basic ".Length..].Trim()));
            var i = raw.IndexOf(':');
            if (i > 0 && TryValidateUser(options, authStore, raw[..i], raw[(i + 1)..], out identity))
                return true;
        }
        catch { }
    }

    return false;
}

static bool TryValidateUser(MiniInfluxOptions options, AuthStore authStore, string user, string password, out AuthIdentity? identity)
{
    identity = null;
    if (string.IsNullOrWhiteSpace(user))
        return false;

    if (options.Auth.Users.TryGetValue(user, out var expected) && expected == password)
    {
        identity = new AuthIdentity(user, true, new Dictionary<string, string>(StringComparer.Ordinal));
        return true;
    }

    return authStore.Validate(user, password, out identity);
}

static string? GetIntoTargetDatabase(string? defaultDb, string intoTarget)
{
    var parts = intoTarget.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    return parts.Length >= 2 ? Unquote(parts[0]) : defaultDb;
}

static BenchmarkRunOptions ParseBenchmarkOptions(string[] args)
{
    var points = 10_000;
    var concurrency = 1;
    for (int i = 0; i < args.Length; i++)
    {
        if (string.Equals(args[i], "--points", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length && int.TryParse(args[i + 1], out var parsedPoints))
        {
            points = parsedPoints;
            i++;
        }
        else if (string.Equals(args[i], "--concurrency", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length && int.TryParse(args[i + 1], out var parsedConcurrency))
        {
            concurrency = parsedConcurrency;
            i++;
        }
    }
    return new BenchmarkRunOptions(points, concurrency);
}

static string ParseBenchmarkFormat(string[] args)
{
    for (int i = 0; i < args.Length; i++)
    {
        if (string.Equals(args[i], "--format", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            return args[i + 1].Trim().ToLowerInvariant();
    }
    return "text";
}

static string Unquote(string value)
{
    value = value.Trim();
    return value.Length >= 2 && value[0] == '"' && value[^1] == '"' ? value[1..^1] : value;
}

static IResult ChunkedResult(QueryResponse result, int chunkSize)
{
    var safeChunkSize = Math.Max(1, chunkSize);
    return Results.Stream(async stream =>
    {
        foreach (var chunk in ChunkResponse(result, safeChunkSize))
        {
            var bytes = JsonSerializer.SerializeToUtf8Bytes(chunk, AppJsonContext.Default.QueryResponse);
            await stream.WriteAsync(bytes);
            await stream.WriteAsync("\n"u8.ToArray());
            await stream.FlushAsync();
        }
    }, "application/x-ndjson; charset=utf-8");
}

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
    Values = values
};

static bool TryParseBool(string? value) =>
    !string.IsNullOrWhiteSpace(value)
    && bool.TryParse(value, out var parsed)
    && parsed;

static int? ParseChunkSize(string? value) =>
    int.TryParse(value, out var parsed) && parsed > 0 ? parsed : null;

public sealed record ErrorResponse(string Error);
public sealed record AdminMessage(string Message);
public sealed record BenchmarkSnapshot(int DatabaseCount, long BufferedPoints, long BufferedBytes, double MetadataScanMs);
