using System;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

// Shared by the PowerShell benchmark and its tests. No PowerShell runspace work occurs
// on the async workers; every worker keeps at most one HTTP request in flight.
public static class HttpWriteBenchmark
{
    public sealed class Result
    {
        public double Seconds { get; set; }
        public long SuccessfulPoints { get; set; }
        public int SuccessfulRequests { get; set; }
        public int FailedRequests { get; set; }
        public int PeakConcurrency { get; set; }
        public double P50Ms { get; set; }
        public double P95Ms { get; set; }
        public double P99Ms { get; set; }
        public double[] SamplesMs { get; set; } = Array.Empty<double>();
        public string[] Errors { get; set; } = Array.Empty<string>();
    }

    public static async Task<Result> RunAsync(HttpClient client, string url, byte[][] payloads,
        int[] pointCounts, int concurrency)
    {
        if (concurrency < 1) throw new ArgumentOutOfRangeException(nameof(concurrency));
        if (payloads.Length != pointCounts.Length || pointCounts.Any(count => count < 0))
            throw new ArgumentException("One non-negative point count is required per payload.");

        var result = new Result();
        var samples = new double[payloads.Length];
        var errors = new string[payloads.Length];
        var next = -1;
        var active = 0;
        var peak = 0;
        long successfulPoints = 0;
        var successfulRequests = 0;
        var watch = Stopwatch.StartNew();
        var workers = new Task[Math.Min(concurrency, payloads.Length)];
        for (var i = 0; i < workers.Length; i++) workers[i] = WorkerAsync();
        await Task.WhenAll(workers).ConfigureAwait(false);
        watch.Stop();

        result.Seconds = watch.Elapsed.TotalSeconds;
        result.SuccessfulPoints = successfulPoints;
        result.SuccessfulRequests = successfulRequests;
        result.FailedRequests = payloads.Length - successfulRequests;
        result.PeakConcurrency = peak;
        result.SamplesMs = samples;
        result.Errors = errors.Where(error => error != null).ToArray();
        var ordered = samples.OrderBy(value => value).ToArray();
        result.P50Ms = Percentile(ordered, 0.50);
        result.P95Ms = Percentile(ordered, 0.95);
        result.P99Ms = Percentile(ordered, 0.99);
        return result;

        async Task WorkerAsync()
        {
            int index;
            while ((index = Interlocked.Increment(ref next)) < payloads.Length)
            {
                var started = Stopwatch.GetTimestamp();
                var current = Interlocked.Increment(ref active);
                int observed;
                do
                {
                    observed = Volatile.Read(ref peak);
                    if (current <= observed) break;
                } while (Interlocked.CompareExchange(ref peak, current, observed) != observed);
                try
                {
                    using (var content = new ByteArrayContent(payloads[index]))
                    {
                        content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
                        using (var response = await client.PostAsync(url, content).ConfigureAwait(false))
                        {
                            if (response.IsSuccessStatusCode)
                            {
                                Interlocked.Add(ref successfulPoints, pointCounts[index]);
                                Interlocked.Increment(ref successfulRequests);
                            }
                            else
                            {
                                var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                                errors[index] = $"Batch {index}: {(int)response.StatusCode} {body}";
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    errors[index] = $"Batch {index}: {ex.Message}";
                }
                finally
                {
                    samples[index] = (Stopwatch.GetTimestamp() - started) * 1000.0 / Stopwatch.Frequency;
                    Interlocked.Decrement(ref active);
                }
            }
        }
    }

    private static double Percentile(double[] ordered, double fraction)
        => ordered.Length == 0 ? 0 : ordered[(int)Math.Ceiling(ordered.Length * fraction) - 1];
}
