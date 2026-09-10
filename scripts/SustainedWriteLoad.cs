using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

// A closed-loop load: each worker prepares a unique batch then waits for its ACK.
public sealed class SustainedWriteLoad
{
    public sealed class Sample
    {
        public double EndSeconds { get; set; }
        public double LatencyMs { get; set; }
        public double PrepareMs { get; set; }
        public int Points { get; set; }
        public string Error { get; set; }
    }
    public readonly ConcurrentQueue<Sample> Samples = new ConcurrentQueue<Sample>();
    private long nextPoint;
    public async Task RunAsync(string url, int seconds, int concurrency, int batchSize)
    {
        using (var client = new HttpClient { Timeout = TimeSpan.FromSeconds(60) })
        {
            var watch = Stopwatch.StartNew();
            await Task.WhenAll(Enumerable.Range(0, concurrency).Select(_ => Worker())).ConfigureAwait(false);
            async Task Worker()
            {
                while (watch.Elapsed.TotalSeconds < seconds)
                {
                    var prep = Stopwatch.StartNew();
                    var start = Interlocked.Add(ref nextPoint, batchSize) - batchSize;
                    var text = new StringBuilder(batchSize * 64);
                    for (int i = 0; i < batchSize; i++)
                    {
                        long n = start + i;
                        text.Append("cpu,host=h").Append(n % 16).Append(" value=").Append(n % 1000)
                            .Append("i ").Append(1788825600000000000L + n * 1000).Append('\n');
                    }
                    var bytes = Encoding.UTF8.GetBytes(text.ToString());
                    var sample = new Sample { PrepareMs = prep.Elapsed.TotalMilliseconds };
                    var request = Stopwatch.StartNew();
                    try
                    {
                        using (var content = new ByteArrayContent(bytes))
                        using (var response = await client.PostAsync(url, content).ConfigureAwait(false))
                        {
                            if (response.IsSuccessStatusCode) sample.Points = batchSize;
                            else sample.Error = ((int)response.StatusCode) + " " + await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                        }
                    }
                    catch (Exception ex) { sample.Error = ex.Message; }
                    sample.LatencyMs = request.Elapsed.TotalMilliseconds;
                    sample.EndSeconds = watch.Elapsed.TotalSeconds;
                    Samples.Enqueue(sample);
                }
            }
        }
    }
}
