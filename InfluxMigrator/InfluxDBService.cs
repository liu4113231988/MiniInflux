using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static InfluxdbDataSync.FormMain;

namespace InfluxdbDataSync
{
    public class InfluxDBService : IDisposable
    {
        private const int WriteBatchSize = 1000;
        private readonly HttpClient _httpClient;
        readonly string _url, _db, _username, _password;


        public InfluxDBService(string host, int port, string db, string user, string password)
        {
            // 复用 HttpClient 以提高性能
            _httpClient = new HttpClient();
            _httpClient.Timeout = TimeSpan.FromSeconds(60); // 防止大查询超时
            _db = db;
            _url = $"http://{host}:{port}";
            _username = user;
            _password = password;
        }

        /// <summary>
        /// [导出] 从 InfluxDB 1.7 获取数据，转为 LineProtocol 格式并 GZip 压缩存储
        /// </summary>
        public async Task ExportToCompressedFileAsync(string measurement, string point, DateTime startTime, DateTime endTime, string outputFilePath, LogDelegate logDelegate)
        {
            //  准备输出流 (File -> GZip -> StreamWriter)
            using var fs = new FileStream(outputFilePath, FileMode.Create);
            using var gzip = new GZipStream(fs, CompressionLevel.SmallestSize);
            using var writer = new StreamWriter(gzip, Encoding.UTF8);

            var currentTime = startTime;
            while (currentTime < endTime)
            {
                var nextTime = currentTime.AddHours(6);
                if (nextTime > endTime) nextTime = endTime;

                string timeFilter = $"time >= '{ToInfluxTime(currentTime)}' AND time < '{ToInfluxTime(nextTime)}'";

                logDelegate($"正在查询 {currentTime:MM-dd HH:mm} -> {nextTime:MM-dd HH:mm}...");

                // 构造 URL
                string query = $"SELECT * FROM \"{EscapeInfluxIdentifier(measurement)}\" WHERE \"tag\"='{EscapeInfluxString(point)}' AND {timeFilter}";
                string requestUrl = $"{_url.TrimEnd('/')}/query?db={_db}&epoch=ms&q={System.Net.WebUtility.UrlEncode(query)}";

                using var request = new HttpRequestMessage(HttpMethod.Get, requestUrl);
                if (!string.IsNullOrWhiteSpace(_username) || !string.IsNullOrWhiteSpace(_password))
                {
                    var authHeader = new System.Net.Http.Headers.AuthenticationHeaderValue(
                     "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_username}:{_password}")));
                    request.Headers.Authorization = authHeader;
                }

                using var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();

                // 读取响应并解析
                var jsonString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                var results = JObject.Parse(jsonString)["results"];
                if (results != null)
                    await writer.WriteAsync(await Task.Run(() => ProcessData(measurement, results, 0)).ConfigureAwait(false)).ConfigureAwait(false);
                await writer.FlushAsync().ConfigureAwait(false);
                currentTime = nextTime;
            }
            logDelegate("导出完成。");
        }

        /// <summary>
        /// [导入] 读取压缩文件并批量写入目标 InfluxDB 1.7
        /// </summary>
        public async Task ImportFromCompressedFileAsync(string inputFilePath, LogDelegate logDelegate)
        {

            logDelegate("开始读取文件并写入...");
            using var fs = new FileStream(inputFilePath, FileMode.Open);
            using var gzip = new GZipStream(fs, CompressionMode.Decompress);
            using var reader = new StreamReader(gzip, Encoding.UTF8);

            string? line;
            long count = 0;
            long batchSize = 5000; // 每5000条刷写一次，或者依赖 Client 内部的自动 Batch
            var pointData = new StringBuilder();
            while ((line = await reader.ReadLineAsync()) != null)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                pointData.Append(line + "\n");
                count++;
                if (count % batchSize == 0)
                {
                    logDelegate($"已读取并排队 {count} 条数据...");
                    await WritePointsAsync(pointData.ToString());
                    pointData.Clear();
                }
            }
            if (pointData.Length > 0)
            {
                await WritePointsAsync(pointData.ToString());
                pointData.Clear();
            }
            // Dispose 会触发 Flush，确保所有数据写入
            logDelegate($"导入成功！共处理 {count} 条数据。");
        }

        public async Task WritePointsAsync(string measurement, List<TagData> points)
        {
            var content = new StringBuilder();
            foreach (var point in points)
            {
                // 简单构建line protocol格式的数据
                content.Append($"{measurement},tag={point.TagId} value={point.Value} {point.Timestamp}\n");
            }

            await WritePointsAsync(content.ToString());
        }

        public async Task WritePointsAsync(string content, CancellationToken cancellationToken = default)
        {
            using var reader = new StringReader(content);
            var batch = new StringBuilder();
            int count = 0;
            while (await reader.ReadLineAsync(cancellationToken) is { } line)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                batch.AppendLine(line);
                if (++count < WriteBatchSize) continue;
                await PostLineProtocolAsync(batch.ToString(), cancellationToken);
                batch.Clear();
                count = 0;
            }

            if (batch.Length > 0)
                await PostLineProtocolAsync(batch.ToString(), cancellationToken);
        }

        private async Task PostLineProtocolAsync(string content, CancellationToken cancellationToken)
        {
            string? error = null;
            for (int attempt = 0; attempt < 3; attempt++)
            {
                try
                {
                    using var request = new HttpRequestMessage(HttpMethod.Post, $"{_url}/write?db={Uri.EscapeDataString(_db)}&precision=ms")
                    {
                        Content = new StringContent(content, Encoding.UTF8)
                    };
                    AddAuthHeader(request, _username, _password);
                    using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                    if (response.IsSuccessStatusCode) return;
                    error = $"{response.StatusCode}: {await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false)}";
                    if ((int)response.StatusCode < 500) break;
                }
                catch (HttpRequestException ex)
                {
                    error = ex.Message;
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    error = $"写入请求超过 {_httpClient.Timeout.TotalSeconds:g} 秒；服务端可能已完成写入，将安全重试";
                }

                if (attempt < 2)
                    await Task.Delay(TimeSpan.FromSeconds(attempt + 1), cancellationToken).ConfigureAwait(false);
            }

            throw new InvalidOperationException($"写入 MiniInflux 失败（已重试 3 次）: {error}");
        }

        // --- 辅助方法：Line Protocol 转义规则 ---
        // Tag keys/values, Measurement: 转义逗号, 等号, 空格
        private string EscapeKey(string val)
        {
            return val.Replace("\\", "\\\\").Replace(",", "\\,").Replace("=", "\\=").Replace(" ", "\\ ");
        }

        // String Field values: 转义双引号
        private string EscapeString(string val)
        {
            return val.Replace("\\", "\\\\").Replace("\"", "\\\"");
        }

        /// <summary>
        /// 测试InfluxDB连接（Ping）
        /// </summary>
        public async Task<bool> TestInfluxConnection(LogDelegate log, CancellationToken cancellationToken = default)
        {
            try
            {
                string pingUrl = _url + "/ping";
                using var request = new HttpRequestMessage(HttpMethod.Get, pingUrl);
                AddAuthHeader(request, _username, _password);

                using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                if (response.IsSuccessStatusCode)
                {
                    return true;
                }
                else
                {
                    string error = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                    log($"连接失败: {response.StatusCode} - {error}");
                    return false;
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                log($"连接测试异常: {ex.Message}");
                return false;
            }
        }

        public async Task<List<string>> GetTagValuesAsync(string measurement, string tagKey = "tag", int pageSize = 1000, CancellationToken cancellationToken = default)
        {
            var tagValues = new HashSet<string>(StringComparer.Ordinal);
            for (var offset = 0; ; offset += pageSize)
            {
                var query = $"SHOW TAG VALUES FROM \"{EscapeInfluxIdentifier(measurement)}\" WITH KEY = \"{EscapeInfluxIdentifier(tagKey)}\" LIMIT {pageSize} OFFSET {offset}";
                var url = $"{_url.TrimEnd('/')}/query?db={Uri.EscapeDataString(_db)}&q={Uri.EscapeDataString(query)}";
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                AddAuthHeader(request, _username, _password);
                using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                var content = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                    throw new HttpRequestException($"获取测点失败: {response.StatusCode} - {content}");

                var root = JObject.Parse(content);
                var firstResult = root["results"]?.FirstOrDefault();
                if (firstResult?["error"] != null)
                    throw new InvalidOperationException($"InfluxDB 查询错误: {firstResult["error"]}");

                var pageCount = 0;
                foreach (var series in firstResult?["series"] ?? new JArray())
                {
                    var columns = series["columns"]?.ToObject<List<string>>();
                    var values = series["values"];
                    if (columns == null || values == null) continue;
                    var valueIndex = columns.IndexOf("value");
                    if (valueIndex < 0) continue;
                    foreach (var row in values)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var value = row[valueIndex]?.ToString();
                        if (!string.IsNullOrWhiteSpace(value)) tagValues.Add(value);
                        pageCount++;
                    }
                }

                if (pageCount < pageSize) break;
            }

            return tagValues.Order(StringComparer.Ordinal).ToList();
        }

        /// <summary>
        /// 从远程InfluxDB查询数据（通过API）
        /// </summary>
        public async Task<string> QueryRemoteData(string sourceMeasurement, string targetMeasurement, string? tag, DateTime start, DateTime end, LogDelegate logDelegate, int sampleIntervalSeconds, CancellationToken cancellationToken = default)
        {
            try
            {

                string timeFilter = $"time >= '{ToInfluxTime(start)}' AND time < '{ToInfluxTime(end)}'";
                string tagFilter = string.IsNullOrWhiteSpace(tag) ? "" : $" AND \"tag\"='{EscapeInfluxString(tag)}'";
                string query = sampleIntervalSeconds > 0
                    ? $"SELECT LAST(\"value\") AS \"value\" FROM \"{EscapeInfluxIdentifier(sourceMeasurement)}\" WHERE {timeFilter}{tagFilter} GROUP BY time({sampleIntervalSeconds}s), \"tag\" fill(none)"
                    : $"SELECT * FROM \"{EscapeInfluxIdentifier(sourceMeasurement)}\" WHERE {timeFilter}{tagFilter}";

                string url = $"{_url.TrimEnd('/')}/query?db={Uri.EscapeDataString(_db)}&q={Uri.EscapeDataString(query)}&epoch=ms";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                AddAuthHeader(request, _username, _password);

                logDelegate($"查询: {query}");

                using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                if (response.IsSuccessStatusCode)
                {
                    string content = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                    var root = JObject.Parse(content);

                    // 检查 InfluxDB 是否返回了查询错误（在 results 数组中）
                    var firstResult = root["results"]?.FirstOrDefault();
                    if (firstResult?["error"] != null)
                    {
                        string errorMsg = firstResult["error"]?.ToString() ?? "未知错误";
                        throw new InvalidOperationException($"InfluxDB 查询错误: {errorMsg}");
                    }
                    var result = root["results"];

                    return result == null ? "" : await Task.Run(() => ProcessData(targetMeasurement, result, 0, cancellationToken), cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    string error = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                    throw new HttpRequestException($"查询失败: {response.StatusCode} - {error}");
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logDelegate($"查询数据异常: {ex.Message}");
                throw;
            }
        }


        private string ProcessData(string measurementName, JToken results, int sampleIntervalSeconds, CancellationToken cancellationToken = default)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var result in results)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var seriesList = result["series"];
                if (seriesList == null) continue;

                foreach (var series in seriesList)
                {
                    // 1. 处理 Tags (Series 级别的 Tags)
                    StringBuilder sbTags = new StringBuilder();
                    bool hasSeriesTag = false;
                    if (series["tags"] is JObject tags)
                    {
                        foreach (var tag in tags.Properties())
                        {
                            // LineProtocol: ,tagKey=tagValue
                            sbTags.Append($",{EscapeKey(tag.Name)}={EscapeKey(tag.Value.ToString())}");
                            if (tag.Name.Equals("tag", StringComparison.Ordinal)) hasSeriesTag = true;
                        }
                    }

                    // 2. 获取列定义
                    var columns = series["columns"]?.ToObject<List<string>>();
                    var values = series["values"]; // 这是一个数组的数组
                    if (columns == null || values == null) continue;
                    int timeIndex = columns.IndexOf("time");
                    if (timeIndex < 0) continue;

                    long? lastBucket = null;
                    JToken? sampledRow = null;
                    foreach (var row in values)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (sampleIntervalSeconds <= 0)
                        {
                            AppendLineProtocol(sb, measurementName, sbTags, hasSeriesTag, columns, timeIndex, row);
                            continue;
                        }

                        var timeValue = row[timeIndex];
                        if (timeValue == null) continue;
                        long bucket = Convert.ToInt64(timeValue) / (sampleIntervalSeconds * 1000L);
                        if (lastBucket == bucket)
                        {
                            sampledRow = row;
                            continue;
                        }

                        if (sampledRow != null)
                            AppendLineProtocol(sb, measurementName, sbTags, hasSeriesTag, columns, timeIndex, sampledRow);
                        lastBucket = bucket;
                        sampledRow = row;
                    }

                    if (sampledRow != null)
                        AppendLineProtocol(sb, measurementName, sbTags, hasSeriesTag, columns, timeIndex, sampledRow);
                }
            }
            return sb.ToString();
        }

        private void AppendLineProtocol(StringBuilder output, string measurementName, StringBuilder seriesTags, bool hasSeriesTag, List<string> columns, int timeIndex, JToken row)
        {
            var timestamp = row[timeIndex];
            if (timestamp == null) return;
            var tags = new StringBuilder(seriesTags.ToString());
            var fields = new StringBuilder();
            for (int i = 0; i < columns.Count; i++)
            {
                var value = row[i];
                if (i == timeIndex || value == null || value.Type == JTokenType.Null) continue;
                if (columns[i].Equals("tag", StringComparison.Ordinal))
                {
                    if (!hasSeriesTag) tags.Append(",tag=").Append(EscapeKey(value.ToString()));
                    continue;
                }
                if (fields.Length > 0) fields.Append(',');
                fields.Append(EscapeKey(columns[i])).Append('=').Append(FormatFieldValue(value));
            }

            if (fields.Length > 0)
                output.Append(EscapeKey(measurementName)).Append(tags).Append(' ').Append(fields).Append(' ').Append(timestamp).Append('\n');
        }

        private string FormatFieldValue(JToken value) => value.Type switch
        {
            JTokenType.String => $"\"{EscapeString(value.ToString())}\"",
            JTokenType.Integer => Convert.ToString(value.Value<long>(), System.Globalization.CultureInfo.InvariantCulture)!,
            JTokenType.Float => Convert.ToString(value.Value<object>(), System.Globalization.CultureInfo.InvariantCulture)!,
            JTokenType.Boolean => value.ToString().ToLowerInvariant(),
            _ => $"\"{EscapeString(value.ToString())}\""
        };

        private string EscapeInfluxIdentifier(string value) => value.Replace("\\", "\\\\").Replace("\"", "\\\"");

        private string EscapeInfluxString(string value) => value.Replace("\\", "\\\\").Replace("'", "\\'");

        private static string ToInfluxTime(DateTime value) => new DateTimeOffset(value).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ");

        /// <summary>
        /// 检查并创建本地数据库
        /// </summary>
        public async Task<bool> EnsureLocalDatabaseExists(LogDelegate log, CancellationToken cancellationToken = default)
        {
            try
            {
                string url = $"{_url}/query";
                string query = $"CREATE DATABASE IF NOT EXISTS {_db}";
                string requestUrl = $"{url}?q={Uri.EscapeDataString(query)}";

                using var request = new HttpRequestMessage(HttpMethod.Post, requestUrl);
                AddAuthHeader(request, _username, _password);

                using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                if (response.IsSuccessStatusCode)
                {
                    log($"本地数据库 {_db} 检查/创建成功");
                    return true;
                }
                else
                {
                    string error = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                    log($"创建本地数据库失败: {response.StatusCode} - {error}");
                    return false;
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                log($"数据库创建异常: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// 添加认证头（如果有用户名密码）
        /// </summary>
        private void AddAuthHeader(HttpRequestMessage request, string username, string password)
        {
            if (!string.IsNullOrWhiteSpace(username) && !string.IsNullOrWhiteSpace(password))
            {
                string auth = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{username}:{password}"));
                request.Headers.Add("Authorization", $"Basic {auth}");
            }
        }

        public void Dispose() => _httpClient.Dispose();

    }
}
