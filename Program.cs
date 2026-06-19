using System.IO.Compression;
using System.Text;
using MiniInflux.Net10.Protocol;
using MiniInflux.Net10.Query;
using MiniInflux.Net10.Storage;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonContext.Default);
});

// Configuration
var dataPath = builder.Configuration["MiniInflux:DataPath"]
    ?? Environment.GetEnvironmentVariable("MINI_INFLUX_DATA") ?? "./data";

var flushThreshold = int.TryParse(builder.Configuration["MiniInflux:FlushThreshold"], out var ft) ? ft : 50_000;
var maxWalFileBytes = long.TryParse(builder.Configuration["Wal:MaxWalFileBytes"], out var mwb) ? mwb : 16 * 1024 * 1024;
var walFsync = bool.TryParse(builder.Configuration["Wal:Fsync"], out var wf) && wf;
var walFsyncIntervalMs = int.TryParse(builder.Configuration["Wal:FsyncIntervalMs"], out var wfi) ? wfi : 1000;
var queueCapacity = int.TryParse(builder.Configuration["Write:QueueCapacity"], out var qc) ? qc : 100_000;
var batchSize = int.TryParse(builder.Configuration["Write:BatchSize"], out var bs) ? bs : 10_000;
var rpCheckIntervalMs = int.TryParse(builder.Configuration["Storage:RpCheckIntervalMs"], out var rci) ? rci : 60_000;
var maxSeriesPerDb = long.TryParse(builder.Configuration["Storage:MaxSeriesPerDatabase"], out var msd) ? msd : 10_000_000;
var maxFieldsPerMeasurement = int.TryParse(builder.Configuration["Storage:MaxFieldsPerMeasurement"], out var mfp) ? mfp : 1024;

// Register services
var engine = new TsdbEngine(dataPath, flushThreshold, maxWalFileBytes, walFsync, walFsyncIntervalMs,
    rpCheckIntervalMs, maxSeriesPerDb, maxFieldsPerMeasurement);
builder.Services.AddSingleton(engine);
builder.Services.AddSingleton<QueryExecutor>();
builder.Services.AddSingleton<MetricsCollector>();
builder.Services.AddSingleton(sp => new WriteQueue(sp.GetRequiredService<TsdbEngine>(), queueCapacity, batchSize));

var app = builder.Build();

// Recover from WAL on startup
engine.Recover();

app.MapGet("/ping", () => Results.NoContent());

app.MapGet("/health", () => Results.Ok(new { name = "miniinflux", message = "ready", status = "pass" }));

app.MapGet("/debug/stats", (MetricsCollector metrics) =>
{
    var stats = metrics.CollectStats();
    return Results.Json(stats, AppJsonContext.Default.DebugStats);
});

app.MapGet("/metrics", (MetricsCollector metrics) =>
{
    var text = metrics.FormatPrometheus();
    return Results.Text(text, "text/plain; version=0.0.4; charset=utf-8");
});

app.MapPost("/write", async (HttpRequest request, TsdbEngine tsdbEngine, WriteQueue writeQueue, MetricsCollector metrics, string db, string? rp, string? precision) =>
{
    if (string.IsNullOrWhiteSpace(db)) return Results.BadRequest(new ErrorResponse("missing required parameter db"));

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
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new ErrorResponse(ex.Message));
    }
});

app.MapMethods("/query", new[] { "GET", "POST" }, async (HttpRequest request, QueryExecutor executor, TsdbEngine tsdbEngine, MetricsCollector metrics, string? db, string? q) =>
{
    if (string.IsNullOrWhiteSpace(q) && request.HasFormContentType)
    {
        var form = await request.ReadFormAsync();
        q = form["q"];
        db ??= form["db"];
    }

    if (string.IsNullOrWhiteSpace(q))
        return Results.BadRequest(new ErrorResponse("missing required parameter q"));

    var result = await executor.ExecuteAsync(tsdbEngine, db, q);
    metrics.RecordQuery();
    return Results.Json(result, AppJsonContext.Default.QueryResponse);
});

app.Lifetime.ApplicationStopping.Register(() =>
{
    var wq = app.Services.GetRequiredService<WriteQueue>();
    wq.Dispose();
    var eng = app.Services.GetRequiredService<TsdbEngine>();
    eng.Dispose();
});

app.Run();

public sealed record ErrorResponse(string Error);
