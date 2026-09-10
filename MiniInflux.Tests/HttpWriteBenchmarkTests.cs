using System.Net;

namespace MiniInflux.Tests;

public sealed class HttpWriteBenchmarkTests
{
    [Fact]
    public async Task RunAsync_ConcurrentRequests_RespectsLimitAndCountsOnlySuccessfulPoints()
    {
        using var handler = new GatedHandler();
        using var client = new HttpClient(handler);
        var run = HttpWriteBenchmark.RunAsync(client, "http://localhost/write",
            [[0], [1], [2], [3], [4]], [2, 3, 5, 7, 11], 3);

        try
        {
            await handler.ThreeStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(3, handler.Started);
        }
        finally { handler.Release.TrySetResult(); }

        var result = await run.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(3, result.PeakConcurrency);
        Assert.Equal(4, result.SuccessfulRequests);
        Assert.Equal(1, result.FailedRequests);
        Assert.Equal(25, result.SuccessfulPoints); // batch 1 (three points) was rejected
        Assert.Single(result.Errors);
        Assert.Contains("429", result.Errors[0]);
        Assert.Equal(5, result.SamplesMs.Length);
        Assert.True(result.P99Ms >= result.P95Ms && result.P95Ms >= result.P50Ms);
        Assert.All(handler.Requests, request => Assert.Throws<ObjectDisposedException>(
            () => request.Content!.ReadAsByteArrayAsync().GetAwaiter().GetResult()));
    }

    [Fact]
    public async Task RunAsync_InvalidConcurrency_RejectsBeforeSendingRequests()
    {
        using var client = new HttpClient();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            HttpWriteBenchmark.RunAsync(client, "http://localhost/write", [[0]], [1], 0));
    }

    private sealed class GatedHandler : HttpMessageHandler
    {
        public readonly TaskCompletionSource ThreeStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly System.Collections.Concurrent.ConcurrentBag<HttpRequestMessage> Requests = [];
        public int Started;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add(request);
            var bytes = await request.Content!.ReadAsByteArrayAsync(ct);
            if (Interlocked.Increment(ref Started) == 3) ThreeStarted.TrySetResult();
            await Release.Task.WaitAsync(ct);
            return new HttpResponseMessage(bytes[0] == 1 ? HttpStatusCode.TooManyRequests : HttpStatusCode.NoContent)
            {
                Content = new StringContent("")
            };
        }
    }
}
