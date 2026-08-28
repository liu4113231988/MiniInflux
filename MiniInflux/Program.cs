using System.Globalization;
using System.IO.Compression;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Buffers;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using MiniInflux.Net10;
using MiniInflux.Net10.Model;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory
});
builder.Host.UseConsoleLifetime();
var options = MiniInfluxOptions.Load(builder.Configuration);
BackupManager.ApplyPendingRestore(options.DataPath);

var cliExitCode = ManagementCli.TryRun(args, options, Console.Out, Console.Error);
if (cliExitCode.HasValue)
{
    Environment.ExitCode = cliExitCode.Value;
    return;
}

builder.Logging.ClearProviders();
var appLevel = ParseLogLevel(options.Logging.Level);
var systemLevel = ParseLogLevel(options.Logging.SystemLevel);
// Keep the lower threshold globally, then enforce each namespace's configured threshold.
builder.Logging.SetMinimumLevel(appLevel < systemLevel ? appLevel : systemLevel);
builder.Logging.AddFilter("MiniInflux", appLevel);
builder.Logging.AddFilter("Microsoft", systemLevel);
builder.Logging.AddFilter("Microsoft.AspNetCore", systemLevel);
builder.Logging.AddFilter("Microsoft.Hosting", systemLevel);
builder.Logging.AddFilter("System", systemLevel);
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
    builder.Logging.AddProvider(new FileLoggerProvider(options.Logging.FilePath, options.Logging.FileMaxBytes, options.Logging.FileRetainedFileCount));

if (!options.Http.Enabled)
{
    var bootstrapLogger = LoggerFactory.Create(logging =>
    {
        logging.SetMinimumLevel(appLevel);
        if (options.Logging.ConsoleEnabled)
            logging.AddSimpleConsole();
        if (options.Logging.FileEnabled)
            logging.AddProvider(new FileLoggerProvider(options.Logging.FilePath, options.Logging.FileMaxBytes, options.Logging.FileRetainedFileCount));
    }).CreateLogger("MiniInflux.Bootstrap");
    bootstrapLogger.LogWarning("HTTP service disabled by configuration. CLI commands remain available.");
    return;
}

var authenticationConfigurationMissing = options.Auth.Enabled
    && (string.IsNullOrWhiteSpace(options.Auth.Username) || string.IsNullOrEmpty(options.Auth.Password));
if (authenticationConfigurationMissing)
    options.Auth.Enabled = false;

var tlsConfigurationMissing = options.Tls.Enabled
    && (string.IsNullOrWhiteSpace(options.Tls.CertPath) || !File.Exists(options.Tls.CertPath));
if (tlsConfigurationMissing)
    options.Tls.Enabled = false;

var authenticationGuard = new AuthenticationGuard(options.Auth);
var tokenStore = new TokenStore(options.DataPath);
authenticationGuard.SetTokenStore(tokenStore);

if (options.Tls.Enabled)
{
    builder.WebHost.ConfigureKestrel(k =>
    {
        k.ListenAnyIP(options.Tls.Port, listen => listen.UseHttps(options.Tls.CertPath!, options.Tls.Password));
    });
}
else
    builder.WebHost.UseUrls(options.Urls);

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
    maxBufferBytes: options.Storage.MaxBufferBytes,
    flushColdDurationMs: options.Storage.FlushColdDurationMs,
    compactionTargetBytes: options.Storage.CompactionTargetBytes,
    maxConcurrentQueries: options.Storage.MaxConcurrentQueries,
    maxQueryMemoryBytes: options.Storage.MaxQueryMemoryBytes,
    maxSegmentFileBytes: options.Storage.MaxSegmentFileBytes,
    minSegmentFileBytes: options.Storage.MinSegmentFileBytes,
    segmentFillRatio: options.Storage.MinSegmentFillRatio,
    compactionMaxWriteBytesPerSecond: options.Storage.CompactionMaxWriteBytesPerSecond);

builder.Services.AddSingleton(engine);
var writeQueue = new WriteQueue(engine, options.Write.QueueCapacity, options.Write.BatchSize);
builder.Services.AddSingleton(writeQueue);
builder.Services.AddSingleton(new QueryExecutor(
    options.Storage.MaxResponseRows,
    options.Storage.MaxQueryPoints,
    options.Storage.MaxQueryDurationMs,
    options.Storage.MaxQueryMemoryBytes));
builder.Services.AddSingleton(options);
builder.Services.AddSingleton(tokenStore);
builder.Services.AddSingleton(authenticationGuard);
builder.Services.AddSingleton(new MetricsCollector(engine, writeQueue));
builder.Services.AddSingleton(new AccessLogWriter(options.Http.AccessLogPath));
builder.Services.AddSingleton<ContinuousQueryRunner>();
builder.Services.AddHostedService<ContinuousQueryHostedService>();

var app = builder.Build();
var runtimeLogger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("MiniInflux.Runtime");
var accessLogWriter = app.Services.GetRequiredService<AccessLogWriter>();
var staticAssets = Assembly.GetExecutingAssembly()
    .GetManifestResourceNames()
    .Where(name => name.StartsWith("wwwroot/", StringComparison.Ordinal))
    .ToDictionary(name => name.Replace('\\', '/'), StringComparer.Ordinal);

engine.Recover();
if (!options.Auth.Enabled)
    runtimeLogger.LogWarning("authentication is disabled; all HTTP endpoints are publicly accessible");
if (authenticationConfigurationMissing)
    runtimeLogger.LogWarning("authentication was requested but credentials are missing; authentication has been disabled");
if (!options.Tls.Enabled)
    runtimeLogger.LogWarning("TLS is disabled; HTTP traffic is unencrypted");
if (tlsConfigurationMissing)
    runtimeLogger.LogWarning("TLS was requested but Tls.CertPath is missing or unreadable; TLS has been disabled");
runtimeLogger.LogInformation("MiniInflux started with data dir {DataDir}, bind {BindAddress}, auth {AuthEnabled}, app log level {AppLogLevel}, system log level {SystemLogLevel}",
    Path.GetFullPath(options.DataPath), options.Http.BindAddress, options.Auth.Enabled, options.Logging.Level, options.Logging.SystemLevel);
runtimeLogger.LogInformation("Performance tuning: adjust Write.QueueCapacity={QueueCapacity}, Write.BatchSize={BatchSize}, Storage.MaxBufferPoints={MaxBufferPoints}, Storage.MaxQueryDurationMs={MaxQueryDurationMs}, Storage.MaxQueryMemoryBytes={MaxQueryMemoryBytes}, and Wal.Fsync={WalFsync} as needed",
    options.Write.QueueCapacity, options.Write.BatchSize, options.Storage.MaxBufferPoints, options.Storage.MaxQueryDurationMs, options.Storage.MaxQueryMemoryBytes, options.Wal.Fsync);

if (options.Http.LogEnabled)
{
    app.Use(async (context, next) =>
    {
        context.Response.Headers["X-Request-Id"] = context.TraceIdentifier;
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

// ponytail: opt-in gzip response compression for JSON/text endpoints. Decided by path up front so
// streaming chunked queries are wrapped transparently; the gzip stream writes its header lazily,
// so empty bodies stay empty. The response body is routed through the gzip stream and restored
// before disposal so the trailer flushes into the raw body. Disposal in finally flushes the
// gzip trailer.
app.Use(async (context, next) =>
{
    var gzip = ResponseCompressionSupport.TryWrap(context.Request, context.Response);
    if (gzip is null)
    {
        await next();
        return;
    }

    // Endpoints like Results.Bytes set Content-Length for the *uncompressed* payload after this
    // middleware runs; the compressed length is unknown, so clear it right before headers flush.
    context.Response.OnStarting(() =>
    {
        context.Response.Headers.ContentLength = null;
        return Task.CompletedTask;
    });
    var originalBody = context.Response.Body;
    context.Response.Body = gzip;
    try
    {
        await next();
    }
    finally
    {
        context.Response.Body = originalBody;
        await gzip.DisposeAsync();
    }
});

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

app.MapGet("/health", (TsdbEngine tsdbEngine) =>
{
    var health = tsdbEngine.Health;
    var availableDiskBytes = GetAvailableDiskBytes(options.DataPath);
    var diskHealthy = options.Storage.MinFreeDiskBytes <= 0 || availableDiskBytes >= options.Storage.MinFreeDiskBytes;
    var response = new HealthResponse(
        health.WriteAvailable && diskHealthy ? "ready" : health.WriteAvailable ? "insufficient disk space" : "write path unavailable",
        health.WriteAvailable && diskHealthy ? "pass" : "fail",
        health.FailureCount,
        health.LastFailureComponent,
        health.LastFailureUtc);
    return Results.Json(response, AppJsonContext.Default.HealthResponse, statusCode: health.WriteAvailable && diskHealthy ? 200 : 503);
});

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

app.MapPost("/write", (HttpRequest request, WriteQueue writeQueue, MetricsCollector metrics, string? db, string? rp, string? precision) =>
{
    if (string.IsNullOrWhiteSpace(db))
        return Task.FromResult<IResult>(Results.BadRequest(new ErrorResponse("missing required parameter db")));
    return WritePointsAsync(request, db, rp, precision, writeQueue, metrics);
});

// InfluxDB 2.x compatible write endpoint: Telegraf and the v2/v3 client libraries target this
// route by default. `bucket` maps to the database (or `db/rp`); `org` is accepted and ignored
// for compatibility. Error bodies use the v2 `{"code","message"}` shape so stock v2 clients
// surface the message correctly; status codes mirror InfluxDB v2 (400/413/429/503).
app.MapPost("/api/v2/write", (HttpRequest request, WriteQueue writeQueue, MetricsCollector metrics, string? bucket, string? org, string? precision) =>
{
    _ = org; // accepted and ignored per InfluxDB 2.x spec (required param but no org model)
    if (!V2WriteSupport.TryResolveBucket(bucket, out var db, out var rp, out var error))
        return Task.FromResult<IResult>(Results.Json(new V2ErrorResponse("invalid", error), AppJsonContext.Default.V2ErrorResponse, statusCode: 400));
    return WritePointsAsync(request, db, rp, precision, writeQueue, metrics, isV2: true);
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

    // P3 参数化查询：v1 `?params={"host":"a"}`（或 form 字段）中的 `$name` 占位符由
    // 执行器在解析树级别绑定——值不回拼 SQL 文本（无注入面），模板解析可跨参数值复用
    Dictionary<string, JsonElement>? queryParams = null;
    {
        var paramsJson = request.Query["params"].ToString();
        if (string.IsNullOrWhiteSpace(paramsJson) && request.HasFormContentType)
        {
            try { paramsJson = (await request.ReadFormAsync())["params"].ToString(); } catch { }
        }
        if (!string.IsNullOrWhiteSpace(paramsJson) && QueryParamBinder.TryParseParamsJson(paramsJson, out var parsedParams))
            queryParams = parsedParams;
    }

    if (options.Data.QueryLogEnabled)
        runtimeLogger.LogInformation("query db={Db} text={Query}", db ?? "-", q);

    if (chunked)
    {
        var chunkOutcome = executor.ExecuteChunkedWithReport(tsdbEngine, db, q, chunkSize ?? 5000, request.HttpContext.RequestAborted, queryParams);
        return ChunkedResult(chunkOutcome, metrics, runtimeLogger, db);
    }

    if (!debug)
    {
        var rawJsonOutcome = executor.TryExecuteBufferedRawDescendingJson(tsdbEngine, db, q, epoch, request.HttpContext.RequestAborted, queryParams);
        if (rawJsonOutcome != null)
        {
            metrics.RecordQuery(rawJsonOutcome.Report);
            LogQueryOutcome(runtimeLogger, db, rawJsonOutcome.Report, rawJsonOutcome.Report.Error);
            return Results.Bytes(rawJsonOutcome.Json, "application/json; charset=utf-8");
        }
    }

    var outcome = executor.ExecuteWithReport(tsdbEngine, db, q, request.HttpContext.RequestAborted, queryParams);
    metrics.RecordQuery(outcome.Report);
    LogQueryOutcome(runtimeLogger, db, outcome.Report, outcome.Response.Results.FirstOrDefault()?.Error);
    if (debug)
        return Results.Json(new QueryDebugResponse { Response = outcome.Response, Report = outcome.Report }, AppJsonContext.Default.QueryDebugResponse);
    return QueryResponseResult(outcome.Response, ParseEpochDivisor(epoch));
});

// P2 v3 查询端点：POST /api/v3/query_influxql JSON {db, q, params?, format?, epoch?}。
// `db`/`q` 必填（对齐 InfluxDB 3 规范）；params 走解析树级绑定；默认 format=json 返回
// v3 的 v1 风格 {"results":[...]} 信封，执行错误（非超时/取消）以 400 + {"error"} 返回
app.MapPost("/api/v3/query_influxql", async (HttpRequest request, QueryExecutor executor, TsdbEngine tsdbEngine, MetricsCollector metrics) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;
    V3QueryRequest? body = null;
    if (request.ContentLength > 0 && request.ContentType?.Contains("application/json", StringComparison.OrdinalIgnoreCase) == true)
    {
        try { body = await ReadJsonAsync(request, AppJsonContext.Default.V3QueryRequest); } catch { }
    }
    var db = body?.Db ?? body?.Database ?? request.Query["db"].ToString();
    if (string.IsNullOrWhiteSpace(db)) db = request.Query["database"].ToString();
    var q = body?.Q ?? body?.Query ?? request.Query["q"].ToString();
    if (string.IsNullOrWhiteSpace(q)) q = request.Query["query"].ToString();
    if (string.IsNullOrWhiteSpace(q) && request.HasFormContentType)
    {
        var form = await request.ReadFormAsync();
        q = form["q"].ToString();
        if (string.IsNullOrWhiteSpace(q)) q = form["query"].ToString();
        if (string.IsNullOrWhiteSpace(db)) db = form["db"].ToString();
        if (string.IsNullOrWhiteSpace(db)) db = form["database"].ToString();
    }
    if (string.IsNullOrWhiteSpace(q))
        return Results.BadRequest(new ErrorResponse("missing required parameter q"));
    if (string.IsNullOrWhiteSpace(db))
        return Results.BadRequest(new ErrorResponse("missing required parameter db"));

    var format = body?.Format ?? request.Query["format"].ToString();
    if (string.IsNullOrWhiteSpace(format)) format = "json";
    if (!string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
        return Results.BadRequest(new ErrorResponse($"unsupported format: {format} (supported: json)"));

    var epoch = body?.Epoch ?? request.Query["epoch"].ToString();
    Dictionary<string, JsonElement>? queryParams = null;
    if (body?.Params is { Count: > 0 })
        queryParams = body.Params;
    else
    {
        var paramsJson = request.Query["params"].ToString();
        if (string.IsNullOrWhiteSpace(paramsJson) && request.HasFormContentType)
        {
            try { paramsJson = (await request.ReadFormAsync())["params"].ToString(); } catch { }
        }
        if (!string.IsNullOrWhiteSpace(paramsJson) && QueryParamBinder.TryParseParamsJson(paramsJson, out var parsedParams))
            queryParams = parsedParams;
    }

    if (options.Data.QueryLogEnabled)
        runtimeLogger.LogInformation("v3 query db={Db} text={Query}", db, q);
    var outcome = executor.ExecuteWithReport(tsdbEngine, db, q, request.HttpContext.RequestAborted, queryParams);
    metrics.RecordQuery(outcome.Report);
    LogQueryOutcome(runtimeLogger, db, outcome.Report, outcome.Response.Results.FirstOrDefault()?.Error);
    if (outcome.Report.Error is not null && !outcome.Report.TimedOut && !outcome.Report.Canceled)
        return Results.Json(new ErrorResponse(outcome.Report.Error), AppJsonContext.Default.ErrorResponse, statusCode: 400);
    return QueryResponseResult(outcome.Response, ParseEpochDivisor(epoch));
});

// Read-only query console for the admin UI. Only SELECT / SHOW statements are
// permitted; mutation statements (DELETE/DROP/CREATE/...) are rejected before execution.
app.MapPost("/admin/api/query", async (HttpRequest request, QueryExecutor executor, TsdbEngine tsdbEngine, MetricsCollector metrics, string? db, string? q) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;

    if (string.IsNullOrWhiteSpace(q) && request.HasFormContentType)
    {
        var form = await request.ReadFormAsync();
        q = form["q"];
        db ??= form["db"];
    }

    if (string.IsNullOrWhiteSpace(q))
        return Results.BadRequest(new ErrorResponse("missing required parameter q"));

    ParsedQuery parsed;
    try
    {
        parsed = InfluxQlParser.Parse(q);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new ErrorResponse($"parse error: {ex.Message}"));
    }

    if (!IsAdminQueryAllowed(parsed.Kind))
        return Results.BadRequest(new ErrorResponse($"statement type '{parsed.Kind}' is not allowed in the admin query console (read-only)"));

    var epoch = request.Query["epoch"].ToString();
    var outcome = executor.ExecuteWithReport(tsdbEngine, db, q, request.HttpContext.RequestAborted);
    metrics.RecordQuery(outcome.Report);
    LogQueryOutcome(runtimeLogger, db, outcome.Report, outcome.Response.Results.FirstOrDefault()?.Error);
    return QueryResponseResult(outcome.Response, ParseEpochDivisor(epoch));
});

// Management commands used by the admin UI. This is deliberately separate from
// /admin/api/query so the interactive query console stays read-only.
app.MapPost("/admin/api/command", async (HttpRequest request, QueryExecutor executor, TsdbEngine tsdbEngine, MetricsCollector metrics, string? db, string? q) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;

    if (string.IsNullOrWhiteSpace(q) && request.HasFormContentType)
    {
        var form = await request.ReadFormAsync();
        q = form["q"];
        db ??= form["db"];
    }
    if (string.IsNullOrWhiteSpace(q))
        return Results.BadRequest(new ErrorResponse("missing required parameter q"));

    ParsedQuery parsed;
    try
    {
        parsed = InfluxQlParser.Parse(q);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new ErrorResponse($"parse error: {ex.Message}"));
    }
    if (!AdminCommandSupport.IsAllowed(parsed.Kind))
        return Results.BadRequest(new ErrorResponse($"statement type '{parsed.Kind}' is not allowed by the admin management API"));

    var outcome = executor.ExecuteWithReport(tsdbEngine, db, q, request.HttpContext.RequestAborted);
    metrics.RecordQuery(outcome.Report);
    LogQueryOutcome(runtimeLogger, db, outcome.Report, outcome.Response.Results.FirstOrDefault()?.Error);
    return QueryResponseResult(outcome.Response, 0);
});

app.MapPost("/admin/backup", (HttpRequest request, TsdbEngine tsdbEngine, string path) =>
{
        if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
            return authResult;
    if (!TryResolveManagedBackupPath(options, path, out var backupPath))
        return Results.BadRequest(new ErrorResponse("backup path must be a relative name under Data.BackupDir"));
    tsdbEngine.FlushAll();
    BackupManager.CreateBackup(tsdbEngine.RootPath, backupPath);
    runtimeLogger.LogInformation("backup created path={Path}", backupPath);
    return Results.Ok(new AdminMessage("backup completed"));
});

app.MapPost("/admin/restore", (HttpRequest request, TsdbEngine tsdbEngine, string path) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;
    try
    {
        if (!TryResolveManagedBackupPath(options, path, out var backupPath))
            return Results.BadRequest(new ErrorResponse("backup path must be a relative name under Data.BackupDir"));
        BackupManager.PrepareRestore(backupPath, tsdbEngine.RootPath);
        runtimeLogger.LogInformation("restore prepared path={Path}", backupPath);
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
                    var sizeBytes = shards.Sum(shard =>
                    {
                        var shardDir = Path.Combine(engine.RootPath, "db", db, rp.Name, "shards", shard.Id.ToString("D6"));
                        return shard.SegmentFiles
                            .Select(file => Path.Combine(shardDir, file))
                            .Where(File.Exists)
                            .Sum(seg => new FileInfo(seg).Length);
                    });
                    return new AdminRetentionPolicySummary
                    {
                        Name = rp.Name,
                        DurationNs = rp.DurationNs,
                        IsDefault = rp.IsDefault,
                        ShardCount = shards.Count,
                        SegmentCount = shards.Sum(shard => shard.SegmentFiles.Count),
                        SizeBytes = sizeBytes
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
                SizeBytes = rpSummaries.Sum(rp => rp.SizeBytes),
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
    if (payload == null || !TryResolveManagedBackupPath(options, payload.Path, out var backupPath))
        return Results.BadRequest(new ErrorResponse("path must be a relative name under Data.BackupDir"));

    try
    {
        engine.FlushAll();
        BackupManager.CreateBackup(engine.RootPath, backupPath);
        runtimeLogger.LogInformation("admin ui backup created path={Path}", backupPath);
        return Results.Json(new AdminMessage("backup completed"), AppJsonContext.Default.AdminMessage);
    }
    catch (Exception ex)
    {
        runtimeLogger.LogWarning(ex, "admin ui backup failed path={Path}", backupPath);
        return Results.BadRequest(new ErrorResponse(ex.Message));
    }
});

adminApi.MapPost("/restore", async (HttpRequest request) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;

    var payload = await ReadJsonAsync(request, AppJsonContext.Default.BackupPathRequest);
    if (payload == null || !TryResolveManagedBackupPath(options, payload.Path, out var backupPath))
        return Results.BadRequest(new ErrorResponse("path must be a relative name under Data.BackupDir"));

    try
    {
        BackupManager.PrepareRestore(backupPath, engine.RootPath);
        runtimeLogger.LogInformation("admin ui restore prepared path={Path}", backupPath);
        return Results.Json(new AdminMessage("restore prepared; restart required"), AppJsonContext.Default.AdminMessage);
    }
    catch (Exception ex)
    {
        runtimeLogger.LogWarning(ex, "admin ui restore prepare failed path={Path}", backupPath);
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

adminApi.MapGet("/maintenance/cache-stats", (HttpRequest request) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;
    var stats = engine.GetMetadataCacheStats();
    return Results.Ok(new CacheStatsResponse { Hits = stats.Hits, Misses = stats.Misses, CachedCount = stats.CachedCount });
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

// P2 Token 认证体系（等权 token）：创建 / 列出 / 吊销，与 Basic 并存
adminApi.MapGet("/tokens", (HttpRequest request) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;
    var list = tokenStore.List().Select(t => new TokenResponse(t.Id, t.Name, null, t.Prefix, t.CreatedAtNs)).ToList();
    return Results.Json(list, AppJsonContext.Default.ListTokenResponse);
});

adminApi.MapPost("/tokens", async (HttpRequest request) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;
    var body = await ReadJsonAsync(request, AppJsonContext.Default.CreateTokenRequest);
    if (body == null || string.IsNullOrWhiteSpace(body.Name))
        return Results.BadRequest(new ErrorResponse("missing required field 'name'"));
    try
    {
        var (rec, raw) = tokenStore.Create(body.Name);
        runtimeLogger.LogInformation("token created id={Id} name={Name} prefix={Prefix}", rec.Id, rec.Name, rec.Prefix);
        return Results.Json(new TokenResponse(rec.Id, rec.Name, raw, rec.Prefix, rec.CreatedAtNs), AppJsonContext.Default.TokenResponse, statusCode: 201);
    }
    catch (ArgumentException ex) { return Results.BadRequest(new ErrorResponse(ex.Message)); }
    catch (InvalidOperationException ex) { return Results.Json(new ErrorResponse(ex.Message), AppJsonContext.Default.ErrorResponse, statusCode: 409); }
});

adminApi.MapDelete("/tokens/{id}", (HttpRequest request, string id) =>
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult))
        return authResult;
    if (string.IsNullOrWhiteSpace(id)) return Results.BadRequest(new ErrorResponse("missing token id"));
    var ok = tokenStore.Revoke(id);
    if (!ok) return Results.NotFound(new ErrorResponse("token not found"));
    runtimeLogger.LogInformation("token revoked id={Id}", id);
    return Results.Json(new AdminMessage("token revoked"), AppJsonContext.Default.AdminMessage);
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
app.Lifetime.ApplicationStopped.Register(() =>
{
    writeQueue.Dispose();
    engine.Dispose();
    accessLogWriter.Dispose();
});

app.Run();

static bool EnsureAuthorized(HttpRequest request, MiniInfluxOptions options, AuthenticationGuard authenticationGuard, ILogger logger, out IResult result, bool v2ErrorFormat = false)
{
    if (AuthorizationSupport.IsAuthorized(request, options.Auth, authenticationGuard, out var attempt))
    {
        result = Results.Empty;
        return true;
    }

    var failedAttempt = attempt!;
    AuditAuthenticationAttempt(logger, request, failedAttempt, options.Auth);

    if (failedAttempt.IsRateLimited)
    {
        ApplyRetryAfterHeader(request.HttpContext.Response, failedAttempt);
        var message = BuildRateLimitMessage(failedAttempt);
        result = v2ErrorFormat
            ? Results.Json(new V2ErrorResponse("too many requests", message), AppJsonContext.Default.V2ErrorResponse, statusCode: 429)
            : Results.Json(new ErrorResponse(message), AppJsonContext.Default.ErrorResponse, statusCode: 429);
        return false;
    }

    result = v2ErrorFormat
        ? Results.Json(new V2ErrorResponse("unauthorized", "unauthorized"), AppJsonContext.Default.V2ErrorResponse, statusCode: 401)
        : Results.Json(new ErrorResponse("unauthorized"), AppJsonContext.Default.ErrorResponse, statusCode: 401);
    return false;
}

static bool TryResolveManagedBackupPath(MiniInfluxOptions options, string? name, out string path)
{
    path = "";
    if (string.IsNullOrWhiteSpace(options.Data.BackupDir) || string.IsNullOrWhiteSpace(name) || Path.IsPathRooted(name))
        return false;

    var root = Path.GetFullPath(options.Data.BackupDir);
    var candidate = Path.GetFullPath(Path.Combine(root, name));
    var rootPrefix = root.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
    if (!candidate.StartsWith(rootPrefix, StringComparison.OrdinalIgnoreCase))
        return false;

    Directory.CreateDirectory(root);
    path = candidate;
    return true;
}

static long GetAvailableDiskBytes(string dataPath)
{
    var root = Path.GetPathRoot(Path.GetFullPath(dataPath));
    return string.IsNullOrWhiteSpace(root) ? 0 : new DriveInfo(root).AvailableFreeSpace;
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
    writer.WritePropertyName("results");
    writer.WriteStartArray();
    foreach (var result in response.Results)
        WriteQueryResult(writer, result, epochDivisor);
    writer.WriteEndArray();
    writer.WriteEndObject();
}

static void WriteQueryResult(Utf8JsonWriter writer, QueryResult result, long epochDivisor)
{
    writer.WriteStartObject();
    writer.WriteNumber("statement_id", result.StatementId);
    if (result.Series is not null)
    {
        writer.WritePropertyName("series");
        writer.WriteStartArray();
        foreach (var series in result.Series)
            WriteQuerySeries(writer, series, epochDivisor);
        writer.WriteEndArray();
    }

    if (result.Error is not null)
        writer.WriteString("error", result.Error);
    writer.WriteEndObject();
}

static void WriteQuerySeries(Utf8JsonWriter writer, QuerySeries series, long epochDivisor)
{
    writer.WriteStartObject();
    writer.WriteString("name", series.Name);
    if (series.Tags is not null)
    {
        writer.WritePropertyName("tags");
        writer.WriteStartObject();
        foreach (var (key, value) in series.Tags)
            writer.WriteString(key, value);
        writer.WriteEndObject();
    }

    writer.WritePropertyName("columns");
    writer.WriteStartArray();
    foreach (var column in series.Columns)
        writer.WriteStringValue(column);
    writer.WriteEndArray();

    writer.WritePropertyName("values");
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

static bool IsAdminQueryAllowed(QueryKind kind) => kind switch
{
    QueryKind.Select => true,
    QueryKind.ShowDatabases => true,
    QueryKind.ShowMeasurements => true,
    QueryKind.ShowFieldKeys => true,
    QueryKind.ShowTagKeys => true,
    QueryKind.ShowTagValues => true,
    QueryKind.ShowRetentionPolicies => true,
    QueryKind.ShowSeries => true,
    QueryKind.ShowSeriesCardinality => true,
    QueryKind.ShowMeasurementCardinality => true,
    QueryKind.ShowTagValuesCardinality => true,
    QueryKind.ShowTagKeyCardinality => true,
    QueryKind.ShowFieldKeyCardinality => true,
    QueryKind.ShowShards => true,
    QueryKind.ShowShardGroups => true,
    QueryKind.ShowStats => true,
    QueryKind.ShowDiagnostics => true,
    QueryKind.ShowContinuousQueries => true,
    QueryKind.ShowQueries => true,
    _ => false
};

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

/// <summary>
/// Shared line-protocol write pipeline for the v1 (/write) and v2 (/api/v2/write) endpoints:
/// auth, body-size enforcement, UTF-8 parsing, enqueue, and the InfluxDB-compatible error
/// mapping (400 parse/conflict, 429 limits/pressure, 413 too large, 503 storage unavailable).
/// When <paramref name="isV2"/> is true, error bodies use the v2 `{"code","message"}` shape.
/// </summary>
async Task<IResult> WritePointsAsync(HttpRequest request, string db, string? rp, string? precision, WriteQueue writeQueue, MetricsCollector metrics, bool isV2 = false)
{
    if (!EnsureAuthorized(request, options, authenticationGuard, runtimeLogger, out var authResult, v2ErrorFormat: isV2))
        return authResult;
    if (request.ContentLength > options.Write.MaxRequestBodyBytes)
        return isV2
            ? Results.Json(new V2ErrorResponse("too large", "request body too large"), AppJsonContext.Default.V2ErrorResponse, statusCode: 413)
            : Results.StatusCode(413);

    // Read the body as raw UTF-8 bytes and parse directly from them: no whole-payload
    // UTF-8 → UTF-16 transcode (the request cap is 25MB, so that string alone was 50MB LOH).
    byte[] bodyBuffer;
    int bodyLength;
    try
    {
        (bodyBuffer, bodyLength) = await ReadRequestBodyBytesAsync(request, options.Write.MaxRequestBodyBytes, request.HttpContext.RequestAborted);
    }
    catch (RequestBodyTooLargeException)
    {
        return isV2
            ? Results.Json(new V2ErrorResponse("too large", "request body too large"), AppJsonContext.Default.V2ErrorResponse, statusCode: 413)
            : Results.StatusCode(413);
    }
    var body = bodyBuffer.AsSpan(0, bodyLength);
    if (bodyLength == 0 || body.IndexOfAnyExcept(" \t\r\n\v\f"u8) < 0)
        return Results.NoContent();
    if (options.Http.WriteTracing)
        runtimeLogger.LogDebug("write trace db={Db} rp={Rp} precision={Precision} body={Body}", db, rp ?? "autogen", precision ?? "ns", Encoding.UTF8.GetString(body));

    try
    {
        var points = LineProtocolParser.ParseMany(body, TimestampPrecision.Parse(precision));
        if (points.Count == 0) return Results.NoContent();
        try
        {
            await writeQueue.EnqueueAsync(db, rp ?? "autogen", points, request.HttpContext.RequestAborted);
            metrics.RecordWrite(points.Count);
            runtimeLogger.LogDebug("write accepted db={Db} rp={Rp} points={PointCount}", db, rp ?? "autogen", points.Count);
            return Results.NoContent();
        }
        catch (FieldConflictException ex)
        {
            runtimeLogger.LogWarning(ex, "write rejected by field conflict db={Db} rp={Rp}", db, rp ?? "autogen");
            return isV2
                ? Results.Json(new V2ErrorResponse("invalid", ex.Message), AppJsonContext.Default.V2ErrorResponse, statusCode: 400)
                : Results.BadRequest(new ErrorResponse(ex.Message));
        }
        catch (CardinalityLimitExceededException ex)
        {
            runtimeLogger.LogWarning("write rejected by cardinality limit db={Db} rp={Rp}", db, rp ?? "autogen");
            return isV2
                ? Results.Json(new V2ErrorResponse("too many requests", ex.Message), AppJsonContext.Default.V2ErrorResponse, statusCode: 429)
                : Results.StatusCode(429);
        }
        catch (MemoryLimitExceededException ex)
        {
            runtimeLogger.LogWarning("write rejected by memory limit db={Db} rp={Rp}", db, rp ?? "autogen");
            return isV2
                ? Results.Json(new V2ErrorResponse("too many requests", ex.Message), AppJsonContext.Default.V2ErrorResponse, statusCode: 429)
                : Results.StatusCode(429);
        }
        catch (WriteQueueFullException ex)
        {
            runtimeLogger.LogWarning("write rejected by queue pressure db={Db} rp={Rp}", db, rp ?? "autogen");
            return isV2
                ? Results.Json(new V2ErrorResponse("too many requests", ex.Message), AppJsonContext.Default.V2ErrorResponse, statusCode: 429)
                : Results.StatusCode(429);
        }
        catch (IOException ex)
        {
            runtimeLogger.LogError(ex, "write rejected because storage is unavailable db={Db} rp={Rp}", db, rp ?? "autogen");
            return isV2
                ? Results.Json(new V2ErrorResponse("unavailable", ex.Message), AppJsonContext.Default.V2ErrorResponse, statusCode: 503)
                : Results.StatusCode(503);
        }
    }
    catch (Exception ex)
    {
        runtimeLogger.LogWarning(ex, "write parse failure db={Db} rp={Rp}", db, rp ?? "autogen");
        return isV2
            ? Results.Json(new V2ErrorResponse("invalid", ex.Message), AppJsonContext.Default.V2ErrorResponse, statusCode: 400)
            : Results.BadRequest(new ErrorResponse(ex.Message));
    }
}

/// <summary>
/// Read the (optionally gzip-compressed) request body into a single byte buffer, enforcing the
/// configured size cap on the *decompressed* bytes so gzip bombs can't expand past the limit.
/// Returns the backing buffer plus the actual length (avoids a final ToArray copy).
/// </summary>
static async Task<(byte[] Buffer, int Length)> ReadRequestBodyBytesAsync(HttpRequest request, long maxBytes, CancellationToken cancellationToken)
{
    Stream input = request.Body;
    if (request.Headers.ContentEncoding.ToString().Contains("gzip", StringComparison.OrdinalIgnoreCase))
        input = new GZipStream(request.Body, CompressionMode.Decompress);

    var capacity = request.ContentLength is > 0 and < 4 * 1024 * 1024 ? (int)request.ContentLength.Value : 64 * 1024;
    var ms = new MemoryStream(capacity);
    await using (input.ConfigureAwait(false))
    {
        var buffer = new byte[64 * 1024];
        int read;
        while ((read = await input.ReadAsync(buffer, cancellationToken)) > 0)
        {
            if (ms.Length + read > maxBytes)
                throw new RequestBodyTooLargeException();
            ms.Write(buffer, 0, read);
        }
    }

    var backing = ms.GetBuffer();
    return backing.Length == ms.Length ? (backing, (int)ms.Length) : (ms.ToArray(), (int)ms.Length);
}

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
public sealed record V2ErrorResponse([property: System.Text.Json.Serialization.JsonPropertyName("code")] string Code, [property: System.Text.Json.Serialization.JsonPropertyName("message")] string Message);
public sealed record AdminMessage([property: System.Text.Json.Serialization.JsonPropertyName("message")] string Message);
public sealed record CreateTokenRequest([property: System.Text.Json.Serialization.JsonPropertyName("name")] string Name);
public sealed record TokenResponse(
    [property: System.Text.Json.Serialization.JsonPropertyName("id")] string Id,
    [property: System.Text.Json.Serialization.JsonPropertyName("name")] string Name,
    [property: System.Text.Json.Serialization.JsonPropertyName("token")] string? Token,
    [property: System.Text.Json.Serialization.JsonPropertyName("prefix")] string Prefix,
    [property: System.Text.Json.Serialization.JsonPropertyName("createdAtNs")] long CreatedAtNs);
public sealed record V3QueryRequest(
    [property: System.Text.Json.Serialization.JsonPropertyName("db")] string? Db,
    [property: System.Text.Json.Serialization.JsonPropertyName("database")] string? Database,
    [property: System.Text.Json.Serialization.JsonPropertyName("q")] string? Q,
    [property: System.Text.Json.Serialization.JsonPropertyName("query")] string? Query,
    [property: System.Text.Json.Serialization.JsonPropertyName("params")] Dictionary<string, System.Text.Json.JsonElement>? Params,
    [property: System.Text.Json.Serialization.JsonPropertyName("epoch")] string? Epoch,
    [property: System.Text.Json.Serialization.JsonPropertyName("format")] string? Format);
public sealed record BenchmarkSnapshot(int DatabaseCount, long BufferedPoints, long BufferedBytes, double MetadataScanMs);
public sealed record HealthResponse(string Message, string Status, long StorageFailures, string? LastFailureComponent, DateTimeOffset? LastFailureUtc)
{
    public string Name { get; init; } = "miniinflux";
}

public sealed class RequestBodyTooLargeException : Exception;
