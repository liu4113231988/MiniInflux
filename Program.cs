using System.Globalization;
using System.IO.Compression;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Buffers;
using Microsoft.Extensions.Logging;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

var builder = WebApplication.CreateSlimBuilder(args);
var options = MiniInfluxOptions.Load(builder.Configuration);
BackupManager.ApplyPendingRestore(options.DataPath);

var cliExitCode = ManagementCli.TryRun(args, options, Console.Out, Console.Error);
if (cliExitCode.HasValue)
{
    Environment.ExitCode = cliExitCode.Value;
    return;
}

builder.Logging.ClearProviders();
builder.Logging.SetMinimumLevel(ParseLogLevel(options.Logging.Level));
if (options.Logging.ConsoleEnabled)
{
    builder.Logging.AddSimpleConsole(console =>
    {
        console.SingleLine = true;
        console.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ ";
        console.UseUtcTimestamp = true;
    });
}
if (options.Logging.FileEnabled)
    builder.Logging.AddProvider(new FileLoggerProvider(options.Logging.FilePath));

if (!options.Http.Enabled)
{
    var bootstrapLogger = LoggerFactory.Create(logging =>
    {
        logging.SetMinimumLevel(ParseLogLevel(options.Logging.Level));
        if (options.Logging.ConsoleEnabled)
            logging.AddSimpleConsole();
        if (options.Logging.FileEnabled)
            logging.AddProvider(new FileLoggerProvider(options.Logging.FilePath));
    }).CreateLogger("MiniInflux.Bootstrap");
    bootstrapLogger.LogWarning("HTTP service disabled by configuration. CLI commands remain available.");
    return;
}

if (options.Auth.Enabled && (string.IsNullOrWhiteSpace(options.Auth.Username) || string.IsNullOrEmpty(options.Auth.Password)))
    throw new InvalidOperationException("Auth.Username and Auth.Password are required when Auth.Enabled is true.");

var authenticationGuard = new AuthenticationGuard(options.Auth);

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

builder.Services.AddSingleton(engine);
builder.Services.AddSingleton(new QueryExecutor(
    options.Storage.MaxResponseRows,
    options.Storage.MaxQueryPoints,
    options.Storage.MaxQueryDurationMs,
    options.Storage.MaxQueryMemoryBytes));
builder.Services.AddSingleton(options);
builder.Services.AddSingleton<MetricsCollector>();
builder.Services.AddSingleton(new AccessLogWriter(options.Http.AccessLogPath));
builder.Services.AddSingleton<ContinuousQueryRunner>();
builder.Services.AddHostedService<ContinuousQueryHostedService>();

var app = builder.Build();
var runtimeLogger = app.Logger;
var accessLogWriter = app.Services.GetRequiredService<AccessLogWriter>();
var staticAssets = Assembly.GetExecutingAssembly()
    .GetManifestResourceNames()
    .Where(name => name.StartsWith("wwwroot/", StringComparison.Ordinal))
    .ToDictionary(name => name.Replace('\\', '/'), StringComparer.Ordinal);

engine.Recover();
runtimeLogger.LogInformation("MiniInflux started with data dir {DataDir}, bind {BindAddress}, auth {AuthEnabled}, log level {LogLevel}",
    Path.GetFullPath(options.DataPath), options.Http.BindAddress, options.Auth.Enabled, options.Logging.Level);

if (options.Http.LogEnabled)
{
    app.Use(async (context, next) =>
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await next();
        }
        finally
        {
            sw.Stop();
            var shouldLog = HttpLoggingSupport.ShouldLogRequest(options.Http, context.Request.Path, context.Response.StatusCode);
            if (shouldLog)
            {
                var line = HttpLoggingSupport.FormatAccessLogLine(context, sw.ElapsedMilliseconds);
                if (accessLogWriter.Enabled)
                    accessLogWriter.Write(line);
                else
                    runtimeLogger.LogInformation("{AccessLog}", line);
            }
        }
    });
}

app.Use(async (context, next) =>
{
    if (!context.Request.Path.StartsWithSegments("/admin/api", out var remainingPath))
    {
        await next();
        return;
    }

    context.Response.Headers.CacheControl = "no-store";
    if (remainingPath.Equals("/session", StringComparison.OrdinalIgnoreCase))
    {
        await next();
        return;
    }

    if (!options.Auth.Enabled)
    {
        await next();
        return;
    }

    var attempt = authenticationGuard.Evaluate(context.Request);
    AuditAuthenticationAttempt(runtimeLogger, context.Request, attempt, options.Auth);
    if (!attempt.Authenticated)
    {
        if (attempt.IsRateLimited)
            ApplyRetryAfterHeader(context.Response, attempt);
        await Results.Json(
            new ErrorResponse(attempt.IsRateLimited ? BuildRateLimitMessage(attempt) : "unauthorized"),
            AppJsonContext.Default.ErrorResponse,
            statusCode: attempt.IsRateLimited ? 429 : 401).ExecuteAsync(context);
        return;
    }

    await next();
});

app.MapGet("/ping", () => Results.NoContent());

app.MapGet("/health", () => Results.Ok(new { name = "miniinflux", message = "ready", status = "pass" }));

app.MapGet("/debug/stats", (HttpRequest request, MetricsCollector metrics) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;
    var stats = metrics.CollectStats();
    return Results.Json(stats, AppJsonContext.Default.DebugStats);
});

app.MapGet("/metrics", (HttpRequest request, MetricsCollector metrics) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;
    var text = metrics.FormatPrometheus();
    return Results.Text(text, "text/plain; version=0.0.4; charset=utf-8");
});

app.MapPost("/write", async (HttpRequest request, TsdbEngine tsdbEngine, MetricsCollector metrics, string db, string? rp, string? precision) =>
{
    if (string.IsNullOrWhiteSpace(db)) return Results.BadRequest(new ErrorResponse("missing required parameter db"));
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;
    if (request.ContentLength > options.Write.MaxRequestBodyBytes)
        return Results.StatusCode(413);

    Stream input = request.Body;
    if (request.Headers.ContentEncoding.ToString().Contains("gzip", StringComparison.OrdinalIgnoreCase))
        input = new GZipStream(request.Body, CompressionMode.Decompress);

    using var reader = new StreamReader(input, Encoding.UTF8);
    var body = await reader.ReadToEndAsync();
    if (string.IsNullOrWhiteSpace(body)) return Results.NoContent();
    if (options.Http.WriteTracing)
        runtimeLogger.LogDebug("write trace db={Db} rp={Rp} precision={Precision} body={Body}", db, rp ?? "autogen", precision ?? "ns", body);

    try
    {
        var points = LineProtocolParser.ParseMany(body, TimestampPrecision.Parse(precision));
        try
        {
            await tsdbEngine.WriteInternalAsync(db, rp ?? "autogen", points);
            metrics.RecordWrite(points.Count);
            runtimeLogger.LogDebug("write accepted db={Db} rp={Rp} points={PointCount}", db, rp ?? "autogen", points.Count);
            return Results.NoContent();
        }
        catch (FieldConflictException ex)
        {
            runtimeLogger.LogWarning(ex, "write rejected by field conflict db={Db} rp={Rp}", db, rp ?? "autogen");
            return Results.BadRequest(new ErrorResponse(ex.Message));
        }
        catch (CardinalityLimitExceededException)
        {
            runtimeLogger.LogWarning("write rejected by cardinality limit db={Db} rp={Rp}", db, rp ?? "autogen");
            return Results.StatusCode(429);
        }
        catch (MemoryLimitExceededException)
        {
            runtimeLogger.LogWarning("write rejected by memory limit db={Db} rp={Rp}", db, rp ?? "autogen");
            return Results.StatusCode(429);
        }
    }
    catch (Exception ex)
    {
        runtimeLogger.LogWarning(ex, "write parse failure db={Db} rp={Rp}", db, rp ?? "autogen");
        return Results.BadRequest(new ErrorResponse(ex.Message));
    }
});

app.MapMethods("/query", ["GET", "POST"], async (HttpRequest request, QueryExecutor executor, TsdbEngine tsdbEngine, MetricsCollector metrics, string? db, string? q) =>
{
    var chunked = TryParseBool(request.Query["chunked"].ToString());
    var debug = TryParseBool(request.Query["debug"].ToString());
    var epoch = request.Query["epoch"].ToString();
    var chunkSize = ParseChunkSize(request.Query["chunk_size"].ToString());
    if (string.IsNullOrWhiteSpace(q) && request.HasFormContentType)
    {
        var form = await request.ReadFormAsync();
        q = form["q"];
        db ??= form["db"];
        chunked = chunked || TryParseBool(form["chunked"].ToString());
        debug = debug || TryParseBool(form["debug"].ToString());
        if (string.IsNullOrWhiteSpace(epoch))
            epoch = form["epoch"].ToString();
        chunkSize ??= ParseChunkSize(form["chunk_size"].ToString());
    }

    if (string.IsNullOrWhiteSpace(q))
        return Results.BadRequest(new ErrorResponse("missing required parameter q"));

    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;

    if (options.Data.QueryLogEnabled)
        runtimeLogger.LogInformation("query db={Db} text={Query}", db ?? "-", q);

    if (chunked)
    {
        var chunkOutcome = executor.ExecuteChunkedWithReport(tsdbEngine, db, q, chunkSize ?? 5000, request.HttpContext.RequestAborted);
        return ChunkedResult(chunkOutcome, metrics, runtimeLogger, db);
    }

    if (!debug)
    {
        var rawJsonOutcome = executor.TryExecuteBufferedRawDescendingJson(tsdbEngine, db, q, epoch, request.HttpContext.RequestAborted);
        if (rawJsonOutcome != null)
        {
            metrics.RecordQuery(rawJsonOutcome.Report);
            LogQueryOutcome(runtimeLogger, db, rawJsonOutcome.Report, rawJsonOutcome.Report.Error);
            return Results.Bytes(rawJsonOutcome.Json, "application/json; charset=utf-8");
        }
    }

    var outcome = executor.ExecuteWithReport(tsdbEngine, db, q, request.HttpContext.RequestAborted);
    metrics.RecordQuery(outcome.Report);
    LogQueryOutcome(runtimeLogger, db, outcome.Report, outcome.Response.Results.FirstOrDefault()?.Error);
    if (debug)
        return Results.Json(new QueryDebugResponse { Response = outcome.Response, Report = outcome.Report }, AppJsonContext.Default.QueryDebugResponse);
    return QueryResponseResult(outcome.Response, ParseEpochDivisor(epoch));
});

app.MapPost("/admin/backup", (HttpRequest request, TsdbEngine tsdbEngine, string path) =>
{
        if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
            return authResult;
    tsdbEngine.FlushAll();
    BackupManager.CreateBackup(tsdbEngine.RootPath, path);
    runtimeLogger.LogInformation("backup created path={Path}", Path.GetFullPath(path));
    return Results.Ok(new AdminMessage("backup completed"));
});

app.MapPost("/admin/restore", (HttpRequest request, TsdbEngine tsdbEngine, string path) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;
    try
    {
        BackupManager.PrepareRestore(path, tsdbEngine.RootPath);
        runtimeLogger.LogInformation("restore prepared path={Path}", Path.GetFullPath(path));
        return Results.Ok(new AdminMessage("restore prepared; restart required"));
    }
    catch (Exception ex)
    {
        runtimeLogger.LogWarning(ex, "restore prepare failed path={Path}", path);
        return Results.BadRequest(new ErrorResponse(ex.Message));
    }
});

app.MapGet("/debug/benchmark", (HttpRequest request, TsdbEngine tsdbEngine) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;
    var sw = System.Diagnostics.Stopwatch.StartNew();
    var dbCount = tsdbEngine.ListDatabases().Count;
    var buffered = tsdbEngine.GetBufferedPointCount();
    var bufferedBytes = tsdbEngine.GetBufferedByteCount();
    sw.Stop();
    return Results.Ok(new BenchmarkSnapshot(dbCount, buffered, bufferedBytes, sw.Elapsed.TotalMilliseconds));
});

var adminApi = app.MapGroup("/admin/api");

adminApi.MapGet("/session", (HttpRequest request) =>
{
    if (!options.Auth.Enabled)
    {
        return Results.Json(new AdminSessionResponse
        {
            RequiresAuthentication = false,
            Authenticated = true,
            UserName = null
        }, AppJsonContext.Default.AdminSessionResponse);
    }

    var attempt = authenticationGuard.Evaluate(request);
    AuditAuthenticationAttempt(runtimeLogger, request, attempt, options.Auth);
    return Results.Json(new AdminSessionResponse
    {
        RequiresAuthentication = true,
        Authenticated = attempt.Authenticated,
        UserName = attempt.Authenticated ? options.Auth.Username : null,
        RateLimited = attempt.IsRateLimited,
        RetryAfterSeconds = attempt.RetryAfterSeconds > 0 ? attempt.RetryAfterSeconds : null
    }, AppJsonContext.Default.AdminSessionResponse);
});

adminApi.MapGet("/overview", (HttpRequest request, MetricsCollector metrics) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;

    var databases = engine.ListDatabases();
    var payload = new AdminOverviewResponse
    {
        DataPath = Path.GetFullPath(options.DataPath),
        HttpBindAddress = options.Http.BindAddress,
        AuthEnabled = options.Auth.Enabled,
        TlsEnabled = options.Tls.Enabled,
        RestorePending = Directory.Exists(options.DataPath + ".restore-pending"),
        RestorePreviousExists = Directory.Exists(options.DataPath + ".restore-previous"),
        DatabaseCount = databases.Count,
        ContinuousQueryCount = engine.Meta.ListContinuousQueries().Count,
        Stats = metrics.CollectStats()
    };
    return Results.Json(payload, AppJsonContext.Default.AdminOverviewResponse);
});

adminApi.MapGet("/databases", (HttpRequest request) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;

    var payload = engine.ListDatabases()
        .Select(db =>
        {
            var rpSummaries = engine.Meta.ListRetentionPolicies(db)
                .OrderBy(rp => rp.Name, StringComparer.Ordinal)
                .Select(rp =>
                {
                    var shards = engine.Meta.GetShards(db, rp.Name);
                    return new AdminRetentionPolicySummary
                    {
                        Name = rp.Name,
                        DurationNs = rp.DurationNs,
                        IsDefault = rp.IsDefault,
                        ShardCount = shards.Count,
                        SegmentCount = shards.Sum(shard => shard.SegmentFiles.Count)
                    };
                })
                .ToList();

            return new AdminDatabaseSummary
            {
                Name = db,
                DefaultRetentionPolicy = engine.GetDefaultRpName(db),
                MeasurementCount = engine.ListMeasurements(db).Count,
                SeriesCardinality = engine.GetSeriesCardinality(db),
                ShardCount = rpSummaries.Sum(rp => rp.ShardCount),
                SegmentCount = rpSummaries.Sum(rp => rp.SegmentCount),
                RetentionPolicies = rpSummaries
            };
        })
        .OrderBy(db => db.Name, StringComparer.Ordinal)
        .ToList();

    return Results.Json(payload, AppJsonContext.Default.ListAdminDatabaseSummary);
});

adminApi.MapGet("/continuous-queries", (HttpRequest request) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;

    var payload = engine.Meta.ListContinuousQueries()
        .Select(cq => new AdminContinuousQuerySummary
        {
            Database = cq.Database,
            Name = cq.Name,
            QueryText = cq.QueryText,
            EveryNs = cq.EveryNs,
            ForNs = cq.ForNs,
            RecomputeRecentBuckets = cq.RecomputeRecentBuckets,
            LastCompletedBucketStartNs = cq.LastCompletedBucketStartNs == long.MinValue ? null : cq.LastCompletedBucketStartNs
        })
        .ToList();

    return Results.Json(payload, AppJsonContext.Default.ListAdminContinuousQuerySummary);
});

adminApi.MapPost("/backup", async (HttpRequest request) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;

    var payload = await ReadJsonAsync(request, AppJsonContext.Default.BackupPathRequest);
    if (payload == null || string.IsNullOrWhiteSpace(payload.Path))
        return Results.BadRequest(new ErrorResponse("path is required"));

    try
    {
        engine.FlushAll();
        BackupManager.CreateBackup(engine.RootPath, payload.Path.Trim());
        runtimeLogger.LogInformation("admin ui backup created path={Path}", Path.GetFullPath(payload.Path.Trim()));
        return Results.Json(new AdminMessage("backup completed"), AppJsonContext.Default.AdminMessage);
    }
    catch (Exception ex)
    {
        runtimeLogger.LogWarning(ex, "admin ui backup failed path={Path}", payload.Path.Trim());
        return Results.BadRequest(new ErrorResponse(ex.Message));
    }
});

adminApi.MapPost("/restore", async (HttpRequest request) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;

    var payload = await ReadJsonAsync(request, AppJsonContext.Default.BackupPathRequest);
    if (payload == null || string.IsNullOrWhiteSpace(payload.Path))
        return Results.BadRequest(new ErrorResponse("path is required"));

    try
    {
        BackupManager.PrepareRestore(payload.Path.Trim(), engine.RootPath);
        runtimeLogger.LogInformation("admin ui restore prepared path={Path}", Path.GetFullPath(payload.Path.Trim()));
        return Results.Json(new AdminMessage("restore prepared; restart required"), AppJsonContext.Default.AdminMessage);
    }
    catch (Exception ex)
    {
        runtimeLogger.LogWarning(ex, "admin ui restore prepare failed path={Path}", payload.Path.Trim());
        return Results.BadRequest(new ErrorResponse(ex.Message));
    }
});

adminApi.MapPost("/maintenance/flush", (HttpRequest request) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;

    engine.FlushAll();
    return Results.Json(new MaintenanceResult { Message = "flush completed" }, AppJsonContext.Default.MaintenanceResult);
});

adminApi.MapPost("/maintenance/compact", (HttpRequest request) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;

    engine.FlushAll();
    var merged = engine.CompactNow();
    var stats = engine.GetCompactionStats();
    runtimeLogger.LogInformation("admin ui compaction run merged={Merged}", merged);
    return Results.Json(new MaintenanceResult
    {
        Message = "compaction completed",
        CompactionTasksMerged = merged,
        Compaction = stats
    }, AppJsonContext.Default.MaintenanceResult);
});

adminApi.MapPost("/maintenance/cq/run", async (HttpRequest request, ContinuousQueryRunner runner) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;

    var executed = await runner.ExecuteDueQueriesAsync(request.HttpContext.RequestAborted);
    runtimeLogger.LogInformation("admin ui continuous query cycle executed={Executed}", executed);
    return Results.Json(new MaintenanceResult
    {
        Message = "continuous query cycle completed",
        ContinuousQueriesExecuted = executed
    }, AppJsonContext.Default.MaintenanceResult);
});

app.MapGet("/admin", () => EmbeddedFile(staticAssets, "admin/index.html", "text/html; charset=utf-8"));

app.MapGet("/admin/assets/{**assetPath}", (string? assetPath, HttpResponse response) =>
{
    if (string.IsNullOrWhiteSpace(assetPath))
        return Results.NotFound();

    var resourcePath = "admin/assets/" + assetPath.Replace('\\', '/');
    if (resourcePath.Contains("../", StringComparison.Ordinal) || !staticAssets.ContainsKey("wwwroot/" + resourcePath))
        return Results.NotFound();

    response.Headers.CacheControl = "public,max-age=31536000,immutable";
    return EmbeddedFile(staticAssets, resourcePath, GetAdminAssetContentType(resourcePath));
});

app.MapGet("/admin/{**path}", (string? path) =>
{
    if (string.Equals(path, "api", StringComparison.OrdinalIgnoreCase)
        || string.Equals(path, "api/", StringComparison.OrdinalIgnoreCase)
        || path?.StartsWith("api/", StringComparison.OrdinalIgnoreCase) == true)
        return Results.NotFound();
    if (!string.IsNullOrWhiteSpace(path) && Path.HasExtension(path))
        return Results.NotFound();
    return EmbeddedFile(staticAssets, "admin/index.html", "text/html; charset=utf-8");
});

app.MapGet("/", () => EmbeddedFile(staticAssets, "index.html", "text/html; charset=utf-8"));

app.MapGet("/{**staticPath}", (string? staticPath) =>
{
    if (string.IsNullOrWhiteSpace(staticPath))
        return EmbeddedFile(staticAssets, "index.html", "text/html; charset=utf-8");

    var resourcePath = staticPath.Replace('\\', '/');
    if (resourcePath.Contains("../", StringComparison.Ordinal))
        return Results.NotFound();

    if (!Path.HasExtension(resourcePath))
        resourcePath = resourcePath.TrimEnd('/') + "/index.html";

    return EmbeddedFile(staticAssets, resourcePath, GetAdminAssetContentType(resourcePath));
});

app.Lifetime.ApplicationStopping.Register(() =>
{
    runtimeLogger.LogInformation("MiniInflux shutting down");
});

app.Run();

static bool EnsureAuthorized(HttpRequest request, MiniInfluxOptions options, AuthenticationGuard authenticationGuard, ILogger logger, out IResult result)
{
    if (!options.Auth.Enabled)
    {
        result = Results.Json(new ErrorResponse("unauthorized"), AppJsonContext.Default.ErrorResponse, statusCode: 401);
        return true;
    }

    var attempt = authenticationGuard.Evaluate(request);
    AuditAuthenticationAttempt(logger, request, attempt, options.Auth);
    if (attempt.Authenticated)
    {
        result = Results.Json(new ErrorResponse("unauthorized"), AppJsonContext.Default.ErrorResponse, statusCode: 401);
        return true;
    }

    if (attempt.IsRateLimited)
    {
        ApplyRetryAfterHeader(request.HttpContext.Response, attempt);
        result = Results.Json(new ErrorResponse(BuildRateLimitMessage(attempt)), AppJsonContext.Default.ErrorResponse, statusCode: 429);
        return false;
    }

    result = Results.Json(new ErrorResponse("unauthorized"), AppJsonContext.Default.ErrorResponse, statusCode: 401);
    return false;
}

static void AuditAuthenticationAttempt(ILogger logger, HttpRequest request, AuthenticationAttempt attempt, AuthOptions options)
{
    if (!options.AuditFailures)
        return;

    if (attempt.Status == AuthenticationAttemptStatus.InvalidCredentials)
    {
        logger.LogWarning(
            "authentication failed client={ClientId} path={Path} method={Method} source={Source} user={UserName} failures={FailureCount}/{MaxFailedAttempts}",
            attempt.ClientId,
            request.Path,
            request.Method,
            attempt.CredentialSource,
            attempt.PresentedUserName ?? "-",
            attempt.FailureCount,
            attempt.MaxFailedAttempts);
    }
    else if (attempt.Status == AuthenticationAttemptStatus.RateLimited)
    {
        logger.LogWarning(
            "authentication rate limited client={ClientId} path={Path} method={Method} source={Source} user={UserName} retry_after_s={RetryAfterSeconds}",
            attempt.ClientId,
            request.Path,
            request.Method,
            attempt.CredentialSource,
            attempt.PresentedUserName ?? "-",
            attempt.RetryAfterSeconds);
    }
}

static void ApplyRetryAfterHeader(HttpResponse response, AuthenticationAttempt attempt)
{
    if (attempt.RetryAfterSeconds > 0)
        response.Headers.RetryAfter = attempt.RetryAfterSeconds.ToString(CultureInfo.InvariantCulture);
}

static string BuildRateLimitMessage(AuthenticationAttempt attempt)
{
    return attempt.RetryAfterSeconds > 0
        ? $"too many authentication failures; retry after {attempt.RetryAfterSeconds}s"
        : "too many authentication failures; retry later";
}
static IResult ChunkedResult(QueryChunkedExecutionOutcome outcome, MetricsCollector metrics, ILogger logger, string? db)
{
    return Results.Stream(async stream =>
    {
        try
        {
            foreach (var chunk in outcome.Responses)
            {
                await WriteQueryResponseAsync(stream, chunk);
                await stream.WriteAsync("\n"u8.ToArray());
                await stream.FlushAsync();
            }
        }
        finally
        {
            metrics.RecordQuery(outcome.Report);
            LogQueryOutcome(logger, db, outcome.Report, outcome.Report.Error);
        }
    }, "application/x-ndjson; charset=utf-8");
}

static IResult QueryResponseResult(QueryResponse response, long epochDivisor)
{
    return Results.Stream(stream => WriteQueryResponseAsync(stream, response, epochDivisor), "application/json; charset=utf-8");
}

static async Task WriteQueryResponseAsync(Stream stream, QueryResponse response, long epochDivisor = 0)
{
    var buffer = new ArrayBufferWriter<byte>();
    using (var writer = new Utf8JsonWriter(buffer))
    {
        WriteQueryResponse(writer, response, epochDivisor);
        writer.Flush();
    }
    await stream.WriteAsync(buffer.WrittenMemory);
}

static void WriteQueryResponse(Utf8JsonWriter writer, QueryResponse response, long epochDivisor)
{
    writer.WriteStartObject();
    writer.WritePropertyName(nameof(QueryResponse.Results));
    writer.WriteStartArray();
    foreach (var result in response.Results)
        WriteQueryResult(writer, result, epochDivisor);
    writer.WriteEndArray();
    writer.WriteEndObject();
}

static void WriteQueryResult(Utf8JsonWriter writer, QueryResult result, long epochDivisor)
{
    writer.WriteStartObject();
    writer.WriteNumber(nameof(QueryResult.StatementId), result.StatementId);
    writer.WritePropertyName(nameof(QueryResult.Series));
    if (result.Series is null)
    {
        writer.WriteNullValue();
    }
    else
    {
        writer.WriteStartArray();
        foreach (var series in result.Series)
            WriteQuerySeries(writer, series, epochDivisor);
        writer.WriteEndArray();
    }

    writer.WritePropertyName(nameof(QueryResult.Error));
    if (result.Error is null)
        writer.WriteNullValue();
    else
        writer.WriteStringValue(result.Error);
    writer.WriteEndObject();
}

static void WriteQuerySeries(Utf8JsonWriter writer, QuerySeries series, long epochDivisor)
{
    writer.WriteStartObject();
    writer.WriteString(nameof(QuerySeries.Name), series.Name);
    writer.WritePropertyName(nameof(QuerySeries.Tags));
    if (series.Tags is null)
    {
        writer.WriteNullValue();
    }
    else
    {
        writer.WriteStartObject();
        foreach (var (key, value) in series.Tags)
            writer.WriteString(key, value);
        writer.WriteEndObject();
    }

    writer.WritePropertyName(nameof(QuerySeries.Columns));
    writer.WriteStartArray();
    foreach (var column in series.Columns)
        writer.WriteStringValue(column);
    writer.WriteEndArray();

    writer.WritePropertyName(nameof(QuerySeries.Values));
    writer.WriteStartArray();
    var timeColumnIndex = epochDivisor > 0 ? series.Columns.FindIndex(x => string.Equals(x, "time", StringComparison.OrdinalIgnoreCase)) : -1;
    foreach (var row in series.Values)
    {
        writer.WriteStartArray();
        for (var i = 0; i < row.Count; i++)
        {
            if (i == timeColumnIndex && TryWriteEpochValue(writer, row[i], epochDivisor))
                continue;
            WriteQueryValue(writer, row[i]);
        }
        writer.WriteEndArray();
    }
    writer.WriteEndArray();
    writer.WriteEndObject();
}

static bool TryWriteEpochValue(Utf8JsonWriter writer, object? value, long epochDivisor)
{
    if (value is long numeric)
    {
        writer.WriteNumberValue(numeric / epochDivisor);
        return true;
    }
    if (value is string text && TryParseRfc3339Ns(text, out var ns))
    {
        writer.WriteNumberValue(ns / epochDivisor);
        return true;
    }
    return false;
}

static bool TryParseRfc3339Ns(string text, out long ns)
{
    ns = 0;
    var dot = text.IndexOf('.');
    var z = text.EndsWith("Z", StringComparison.Ordinal) ? text.Length - 1 : text.Length;
    var secondEnd = dot >= 0 ? dot : z;
    if (!DateTimeOffset.TryParse(text[..secondEnd] + "Z", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dto))
        return false;
    var nanos = 0L;
    if (dot >= 0)
    {
        var scale = 100_000_000L;
        for (var i = dot + 1; i < z && scale > 0; i++, scale /= 10)
        {
            var ch = text[i];
            if (ch < '0' || ch > '9') return false;
            nanos += (ch - '0') * scale;
        }
    }
    ns = checked(dto.ToUnixTimeSeconds() * 1_000_000_000L + nanos);
    return true;
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

static void WriteQueryValue(Utf8JsonWriter writer, object? value)
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
        case uint number:
            writer.WriteNumberValue(number);
            break;
        case long number:
            writer.WriteNumberValue(number);
            break;
        case ulong number:
            writer.WriteNumberValue(number);
            break;
        case short number:
            writer.WriteNumberValue(number);
            break;
        case ushort number:
            writer.WriteNumberValue(number);
            break;
        case byte number:
            writer.WriteNumberValue(number);
            break;
        case sbyte number:
            writer.WriteNumberValue(number);
            break;
        case double number:
            writer.WriteNumberValue(number);
            break;
        case float number:
            writer.WriteNumberValue(number);
            break;
        case decimal number:
            writer.WriteNumberValue(number);
            break;
        default:
            writer.WriteStringValue(Convert.ToString(value, CultureInfo.InvariantCulture));
            break;
    }
}

static void LogQueryOutcome(ILogger logger, string? db, QueryExecutionReport report, string? error)
{
    if (!string.IsNullOrWhiteSpace(error))
        logger.LogWarning("query failed db={Db} error={Error}", db ?? "-", error);
    else
        logger.LogDebug("query completed db={Db} rows={Rows} scanned={ScannedPoints} duration_ms={DurationMs}",
            db ?? "-", report.RowsReturned, report.ScannedPoints, report.DurationMs);
}

static bool TryParseBool(string? value) =>
    !string.IsNullOrWhiteSpace(value)
    && bool.TryParse(value, out var parsed)
    && parsed;

static int? ParseChunkSize(string? value) =>
    int.TryParse(value, out var parsed) && parsed > 0 ? parsed : null;

static LogLevel ParseLogLevel(string? value) =>
    Enum.TryParse<LogLevel>(value, true, out var parsed) ? parsed : LogLevel.Information;

static async Task<T?> ReadJsonAsync<T>(HttpRequest request, JsonTypeInfo<T> typeInfo)
{
    if (request.ContentLength == 0)
        return default;
    return await JsonSerializer.DeserializeAsync(request.Body, typeInfo, request.HttpContext.RequestAborted);
}

static string GetAdminAssetContentType(string path)
{
    return Path.GetExtension(path).ToLowerInvariant() switch
    {
        ".css" => "text/css; charset=utf-8",
        ".js" => "text/javascript; charset=utf-8",
        ".json" => "application/json; charset=utf-8",
        ".html" => "text/html; charset=utf-8",
        ".svg" => "image/svg+xml",
        ".png" => "image/png",
        ".jpg" or ".jpeg" => "image/jpeg",
        ".webp" => "image/webp",
        ".ico" => "image/x-icon",
        ".woff" => "font/woff",
        ".woff2" => "font/woff2",
        _ => "application/octet-stream"
    };
}

static IResult EmbeddedFile(Dictionary<string, string> staticAssets, string path, string contentType)
{
    var resourceName = "wwwroot/" + path.TrimStart('/').Replace('\\', '/');
    if (!staticAssets.TryGetValue(resourceName, out var manifestName))
        return Results.NotFound();

    var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(manifestName);
    return stream is null ? Results.NotFound() : Results.Stream(stream, contentType);
}

public sealed record ErrorResponse([property: System.Text.Json.Serialization.JsonPropertyName("error")] string Error);
public sealed record AdminMessage([property: System.Text.Json.Serialization.JsonPropertyName("message")] string Message);
public sealed record BenchmarkSnapshot(int DatabaseCount, long BufferedPoints, long BufferedBytes, double MetadataScanMs);
