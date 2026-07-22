using InfluxDB.LineProtocol.Client;
using InfluxDB.LineProtocol.Payload;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Diagnostics.Metrics;
using System.IO.Compression;
using System.Net.Http;
using System.Security.Policy;
using System.Text;
using static InfluxdbDataSync.FormMain;
using static System.Windows.Forms.VisualStyles.VisualStyleElement.StartPanel;

namespace InfluxdbDataSync
{
    public partial class FormMain : Form
    {
        // 用于显示日志的委托
        public delegate void LogDelegate(string message);

        // HTTP客户端（复用提升性能）
        private static readonly HttpClient _httpClient = new HttpClient();

        private const int MAX_SIZE = 5000; // 32MB 分片大小
        private CancellationTokenSource? _syncCancellation;
        private string? _resumeKey;
        private string? _resumeTag;
        private DateTime _resumeTime;
        public FormMain()
        {
            InitializeComponent();
            InitializeUI();
        }

        private void InitializeUI()
        {
            // 设置日期时间默认值为最近1小时
            dtpStartTime.Value = DateTime.Parse(DateTime.Now.AddHours(-2).ToString("yyyy-MM-dd HH:00:00"));
            dtpEndTime.Value = DateTime.Parse(DateTime.Now.ToString("yyyy-MM-dd HH:00:00"));

            // 远程InfluxDB默认配置
            txtRemoteHost.Text = "localhost";
            txtRemotePort.Text = "38086";
            txtRemoteDatabase.Text = "ofm_tsdb";
            txtRemoteMeasure.Text = "INDEX";

            // 本地InfluxDB默认配置
            txtLocalHost.Text = "localhost";
            txtLocalPort.Text = "38086";
            txtLocalDatabase.Text = "ofm_tsdb";
            txtLocalMeasure.Text = "INDEX";
            txtTimeInterval.Text = "0";

            // 初始化日志文本框
            rtbLog.ReadOnly = true;
        }

        /// <summary>
        /// 向日志区域添加消息
        /// </summary>
        private static DateTime GetWindowEnd(DateTime start, DateTime end, int sampleIntervalSeconds, TimeSpan queryWindow)
        {
            var latest = start.Add(queryWindow);
            if (latest > end || sampleIntervalSeconds <= 0) return latest > end ? end : latest;

            long intervalMs = sampleIntervalSeconds * 1000L;
            long startMs = new DateTimeOffset(start).ToUnixTimeMilliseconds();
            long latestMs = new DateTimeOffset(latest).ToUnixTimeMilliseconds();
            long windowEndMs = latestMs / intervalMs * intervalMs;
            if (windowEndMs <= startMs) return latest;
            return windowEndMs >= new DateTimeOffset(end).ToUnixTimeMilliseconds()
                ? end
                : DateTimeOffset.FromUnixTimeMilliseconds(windowEndMs).LocalDateTime;
        }

        private static bool IsTimeout(Exception ex) =>
            ex is TimeoutException or TaskCanceledException
            || ex.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("超时", StringComparison.Ordinal);
        private void AddLog(string message)
        {
            if (rtbLog.InvokeRequired)
            {
                rtbLog.Invoke(new LogDelegate(AddLog), message);
            }
            else
            {
                rtbLog.AppendText($"[{DateTime.Now:HH:mm:ss}] {message}{Environment.NewLine}");
                rtbLog.ScrollToCaret();
            }
        }


        private void SetOperationButtonsEnabled(bool enabled)
        {
            btnSync.Enabled = enabled;
            btnBackupToFile.Enabled = enabled;
            btnRestoreFromFile.Enabled = enabled;
            btnFillData.Enabled = enabled;
        }

        private void btnCancelSync_Click(object sender, EventArgs e)
        {
            if (_syncCancellation == null) return;
            btnCancelSync.Enabled = false;
            AddLog("正在取消同步...");
            _syncCancellation.Cancel();
        }
        private async void btnSync_Click(object sender, EventArgs e)
        {
            try
            {
                // 禁用按钮防止重复点击
                SetOperationButtonsEnabled(false);
                _syncCancellation = new CancellationTokenSource();
                var cancellationToken = _syncCancellation.Token;
                btnCancelSync.Enabled = true;
                AddLog("开始数据同步...");
                string remoteHost = txtRemoteHost.Text, strRemotePort = txtRemotePort.Text,
                   remoteUser = txtRemoteUsername.Text, remotePassword = txtRemotePassword.Text,
                   remoteDB = txtRemoteDatabase.Text, remoteMeasure = txtRemoteMeasure.Text;

                string localHost = txtLocalHost.Text, strlocalPort = txtLocalPort.Text,
                    localUser = txtLocalUsername.Text, localPassword = txtLocalPassword.Text,
                    localDB = txtLocalDatabase.Text, localMeasure = txtLocalMeasure.Text;
                string points = rtbMeasurements.Text;

                if (string.IsNullOrWhiteSpace(remoteMeasure))
                {
                    AddLog("请输入远程序列名称");
                    return;
                }
                else if (string.IsNullOrWhiteSpace(remoteHost))
                {
                    AddLog("请输入远程地址");
                    return;
                }
                else if (string.IsNullOrWhiteSpace(remoteDB))
                {
                    AddLog("请输入远程数据库名称");
                    return;
                }
                if (string.IsNullOrWhiteSpace(strRemotePort))
                {
                    AddLog("请输入远程端口");
                    return;
                }
                if (!int.TryParse(strRemotePort, out int remotePort) || remotePort <= 0 || remotePort > 65535)
                {
                    AddLog("远程端口格式错误");
                    return;
                }

                if (string.IsNullOrWhiteSpace(localMeasure))
                {
                    AddLog("请输入目标序列名称");
                    return;
                }
                else if (string.IsNullOrWhiteSpace(localHost))
                {
                    AddLog("请输入目标地址");
                    return;
                }
                else if (string.IsNullOrWhiteSpace(localDB))
                {
                    AddLog("请输入目标数据库名称");
                    return;
                }
                if (!int.TryParse(strlocalPort, out int localPort) || localPort <= 0 || localPort > 65535)
                {
                    AddLog("目标端口格式错误");
                    return;
                }

                if (dtpStartTime.Value >= dtpEndTime.Value)
                {
                    AddLog("开始时间必须早于结束时间");
                    return;
                }
                if (!int.TryParse(txtTimeInterval.Text, out int sampleIntervalSeconds) || sampleIntervalSeconds < 0)
                {
                    AddLog("采样时间间隔必须是大于或等于 0 的秒数（0 表示原始数据）");
                    return;
                }
                var migrationConcurrency = (int)numMigrationConcurrency.Value;
                var writeBatchSize = (int)numWriteBatchSize.Value;
                using var remoteInfluxService = new InfluxDBService(remoteHost, remotePort, remoteDB, remoteUser, remotePassword);

                // 测试连接
                AddLog("测试远程InfluxDB连接...");
                if (!await remoteInfluxService.TestInfluxConnection(AddLog, cancellationToken))
                {
                    AddLog("远程InfluxDB连接失败，无法继续");
                    return;
                }

                AddLog("测试本地InfluxDB连接...");
                using var localInfluxService = new InfluxDBService(localHost, localPort, localDB, localUser, localPassword, writeBatchSize);
                if (!await localInfluxService.TestInfluxConnection(AddLog, cancellationToken))
                {
                    AddLog("本地InfluxDB连接失败，无法继续");
                    return;
                }
                if (!await localInfluxService.EnsureLocalDatabaseExists(AddLog, cancellationToken))
                    return;

                var enteredTags = rtbMeasurements.Text.Split(new[] { '\r', '\n', ';', '|', ',' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                List<string> tagFilters;
                if (enteredTags.Length == 0)
                {
                    AddLog("正在分页获取全部测点...");
                    tagFilters = await remoteInfluxService.GetTagValuesAsync(remoteMeasure, cancellationToken: cancellationToken);
                    if (tagFilters.Count == 0)
                    {
                        AddLog("未找到 tag 测点，无法按测点拆分同步。");
                        return;
                    }
                    AddLog($"已获取 {tagFilters.Count} 个测点，将依次同步。");
                }
                else
                {
                    tagFilters = enteredTags.Distinct(StringComparer.Ordinal).ToList();
                }

                DateTime startTime = dtpStartTime.Value, endTime = dtpEndTime.Value;
                var configuredWindow = TimeSpan.FromHours((double)numSyncWindowHours.Value);
                var minimumWindow = TimeSpan.FromMinutes(1);
                var tagSelectionKey = enteredTags.Length == 0 ? "*" : string.Join('\u001f', tagFilters);
                var syncKey = $"{remoteHost}\u001f{remoteDB}\u001f{remoteMeasure}\u001f{localHost}\u001f{localDB}\u001f{localMeasure}\u001f{startTime:O}\u001f{endTime:O}\u001f{sampleIntervalSeconds}\u001f{configuredWindow.Ticks}\u001f{tagSelectionKey}";
                var startTagIndex = _resumeKey == syncKey && _resumeTag != null ? tagFilters.IndexOf(_resumeTag) : -1;
                if (startTagIndex >= 0)
                    AddLog($"从上次位置继续：测点 {_resumeTag}，时间 {_resumeTime:yyyy-MM-dd HH:mm:ss}");
                else
                    startTagIndex = 0;
                AddLog($"Start sync {tagFilters.Count} tags (window {configuredWindow.TotalHours:g}h, sample {sampleIntervalSeconds}s, concurrency {migrationConcurrency}, write batch {writeBatchSize})...");

                async Task SyncTagAsync(int tagIndex, bool updateResume)
                {
                    var tag = tagFilters[tagIndex];
                    var dtTime = _resumeKey == syncKey && _resumeTag == tag && _resumeTime > startTime ? _resumeTime : startTime;
                    var queryWindow = configuredWindow;
                    if (updateResume)
                    {
                        _resumeKey = syncKey;
                        _resumeTag = tag;
                        _resumeTime = dtTime;
                    }
                    AddLog($"----- [{tagIndex + 1}/{tagFilters.Count}] 开始同步测点: {tag} -----");

                    while (dtTime < endTime)
                    {
                        var et = GetWindowEnd(dtTime, endTime, sampleIntervalSeconds, queryWindow);
                        AddLog($"查询 {dtTime:yyyy-MM-dd HH:mm:ss} - {et:yyyy-MM-dd HH:mm:ss}");
                        string queryResult;
                        try
                        {
                            queryResult = await remoteInfluxService.QueryRemoteData(remoteMeasure, localMeasure, tag, dtTime, et, AddLog, sampleIntervalSeconds, cancellationToken);
                        }
                        catch (Exception ex) when (!cancellationToken.IsCancellationRequested && IsTimeout(ex) && queryWindow > minimumWindow)
                        {
                            queryWindow = TimeSpan.FromTicks(Math.Max(minimumWindow.Ticks, queryWindow.Ticks / 2));
                            AddLog($"查询超时，自动缩小周期为 {queryWindow.TotalMinutes:g} 分钟后重试。");
                            continue;
                        }

                        if (!string.IsNullOrEmpty(queryResult))
                        {
                            AddLog("----- 查询成功，开始写入 -----");
                            await localInfluxService.WritePointsAsync(queryResult, cancellationToken);
                        }
                        else
                        {
                            AddLog($"测量点 {tag} 当前时段无数据");
                        }

                        dtTime = et;
                        if (updateResume) _resumeTime = dtTime;
                    }

                    if (updateResume && tagIndex + 1 < tagFilters.Count)
                    {
                        _resumeTag = tagFilters[tagIndex + 1];
                        _resumeTime = startTime;
                    }
                    await Task.Delay(10, cancellationToken);
                }

                if (migrationConcurrency <= 1)
                {
                    for (var tagIndex = startTagIndex; tagIndex < tagFilters.Count; tagIndex++)
                        await SyncTagAsync(tagIndex, updateResume: true);
                }
                else
                {
                    using var throttle = new SemaphoreSlim(migrationConcurrency);
                    var tasks = Enumerable.Range(startTagIndex, tagFilters.Count - startTagIndex).Select(async tagIndex =>
                    {
                        await throttle.WaitAsync(cancellationToken);
                        try { await SyncTagAsync(tagIndex, updateResume: false); }
                        finally { throttle.Release(); }
                    });
                    await Task.WhenAll(tasks);
                }

                _resumeKey = null;
                _resumeTag = null;
                AddLog("所有测点数据同步完成！");
            }
            catch (OperationCanceledException) when (_syncCancellation?.IsCancellationRequested == true)
            {
                AddLog("同步已取消。");
            }
            catch (Exception ex)
            {
                AddLog($"同步过程中发生错误: {ex.Message}");
            }
            finally
            {
                btnCancelSync.Enabled = false;
                _syncCancellation?.Dispose();
                _syncCancellation = null;
                SetOperationButtonsEnabled(true);
            }
        }

        private async void btnBackupToFile_Click(object sender, EventArgs e)
        {
            try
            {
                string remoteHost = txtRemoteHost.Text, strRemotePort = txtRemotePort.Text,
                    remoteUser = txtRemoteUsername.Text, remotePassword = txtRemotePassword.Text,
                    remoteDB = txtRemoteDatabase.Text, remoteMeasure = txtRemoteMeasure.Text;
                string points = rtbMeasurements.Text;
                SetOperationButtonsEnabled(false);

                string backupFilePath = txtBackupFilePath.Text;
                // 验证输入
                if (string.IsNullOrWhiteSpace(remoteMeasure))
                {
                    AddLog("请输入远程序列名称");
                    return;
                }
                if (string.IsNullOrWhiteSpace(remoteHost))
                {
                    AddLog("请输入远程地址");
                    return;
                }
                if (string.IsNullOrWhiteSpace(remoteDB))
                {
                    AddLog("请输入远程端口");
                    return;
                }
                if (string.IsNullOrWhiteSpace(backupFilePath))
                {
                    AddLog("请选择备份文件保存路径");
                    return;
                }
                if (!int.TryParse(strRemotePort, out int remotePort) || remotePort <= 0 || remotePort > 65535)
                {
                    AddLog("远程端口格式错误");
                    return;
                }
                if (dtpStartTime.Value >= dtpEndTime.Value)
                {
                    AddLog("开始时间必须早于结束时间");
                    return;
                }
                // 测试远程连接
                AddLog("测试远程InfluxDB连接...");
                using var influxDBService = new InfluxDBService(remoteHost, remotePort, remoteDB, remoteUser, remotePassword);
                if (!await influxDBService.TestInfluxConnection(AddLog))
                {
                    AddLog("远程InfluxDB连接失败，无法继续");

                    return;
                }

                var pointList = points.Split(new[] { '\r', '\n', ',', ';' }, StringSplitOptions.RemoveEmptyEntries);
                AddLog($"共获取到 {pointList.Length} 个测量点，开始备份数据...");

                foreach (var point in pointList)
                {
                    AddLog($"开始备份测点：{point}");
                    string backFilePath = Path.Combine(backupFilePath, point + ".bak");
                    await influxDBService.ExportToCompressedFileAsync(remoteMeasure, point, dtpStartTime.Value, dtpEndTime.Value, backFilePath, AddLog);
                }

            }
            catch (Exception ex)
            {
                AddLog($"备份过程中发生错误: {ex.Message}");
            }
            finally
            {
                SetOperationButtonsEnabled(true);
            }
        }

        /// <summary>
        /// 选择备份文件路径
        /// </summary>
        private void btnSelectBackupFile_Click(object sender, EventArgs e)
        {
            using (var saveDialog = new FolderBrowserDialog())
            {
                if (saveDialog.ShowDialog() == DialogResult.OK)
                {
                    txtBackupFilePath.Text = saveDialog.SelectedPath;
                }
            }
        }

        /// <summary>
        /// 选择恢复文件路径
        /// </summary>
        private void btnSelectRestoreFile_Click(object sender, EventArgs e)
        {
            using (var openDialog = new OpenFileDialog())
            {
                openDialog.Title = "选择要恢复的备份文件";

                if (openDialog.ShowDialog() == DialogResult.OK)
                {
                    txtRestoreFilePath.Text = openDialog.FileName;
                }
            }
        }

        private async void btnRestoreFromFile_Click(object sender, EventArgs e)
        {
            try
            {
                string localHost = txtLocalHost.Text, strlocalPort = txtLocalPort.Text,
                    localUser = txtLocalUsername.Text, localPassword = txtLocalPassword.Text,
                    localDB = txtLocalDatabase.Text, localMeasure = txtLocalMeasure.Text;
                SetOperationButtonsEnabled(false);
                string restoreFilePath = txtRestoreFilePath.Text;
                // 验证输入
                if (string.IsNullOrWhiteSpace(restoreFilePath))
                {
                    AddLog("请选择要恢复的备份文件");
                    return;
                }
                if (!File.Exists(restoreFilePath))
                {
                    AddLog("选择的备份文件不存在");
                    return;
                }
                if (!int.TryParse(strlocalPort, out int localPort) || localPort <= 0 || localPort > 65535)
                {
                    AddLog("恢复目标端口格式错误");
                    return;
                }

                // 测试目标InfluxDB连接
                AddLog("测试本地InfluxDB连接...");
                using var influxDBService = new InfluxDBService(localHost, localPort, localDB, localUser, localPassword, (int)numWriteBatchSize.Value);
                if (!await influxDBService.TestInfluxConnection(AddLog))
                {
                    AddLog("本地InfluxDB连接失败，无法继续");
                    return;
                }
                AddLog("检查本地数据库是否存在...");
                bool flag = await influxDBService.EnsureLocalDatabaseExists(AddLog);
                if (!flag)
                {
                    return;
                }

                // 获取备份文件的基本路径和测量点名称
                string basePath = txtRestoreFilePath.Text;
                await influxDBService.ImportFromCompressedFileAsync(basePath, AddLog);

                AddLog("数据恢复成功！");
            }
            catch (Exception ex)
            {
                AddLog($"恢复过程中发生错误: {ex.Message}");
            }
            finally
            {
                SetOperationButtonsEnabled(true);
            }
        }

        private async void btnFillData_Click(object sender, EventArgs e)
        {
            string localHost = txtLocalHost.Text, strlocalPort = txtLocalPort.Text,
                    localUser = txtLocalUsername.Text, localPassword = txtLocalPassword.Text,
                    localDB = txtLocalDatabase.Text, localMeasure = txtLocalMeasure.Text;
            double minNum = (double)numMin.Value, maxNum = (double)numMax.Value;
            try
            {
                SetOperationButtonsEnabled(false);
                if (string.IsNullOrWhiteSpace(localMeasure))
                {
                    AddLog("请输入目标序列名称");
                    return;
                }
                else if (string.IsNullOrWhiteSpace(localHost))
                {
                    AddLog("请输入目标地址");
                    return;
                }
                else if (string.IsNullOrWhiteSpace(localDB))
                {
                    AddLog("请输入目标数据库名称");
                    return;
                }
                if (!int.TryParse(strlocalPort, out int localPort) || localPort <= 0 || localPort > 65535)
                {
                    AddLog("目标端口格式错误");
                    return;
                }
                if (minNum >= maxNum)
                {
                    AddLog("数值范围设置错误");
                    return;
                }
                if (dtpStartTime.Value >= dtpEndTime.Value)
                {
                    AddLog("开始时间必须早于结束时间");
                    return;
                }

                // 测试目标InfluxDB连接
                AddLog("测试本地InfluxDB连接...");
                using var influxDBService = new InfluxDBService(localHost, localPort, localDB, localUser, localPassword, (int)numWriteBatchSize.Value);
                if (!await influxDBService.TestInfluxConnection(AddLog))
                {
                    AddLog("本地InfluxDB连接失败，无法继续");
                    SetOperationButtonsEnabled(true);
                    return;
                }
                AddLog("检查本地数据库是否存在...");
                bool flag = await influxDBService.EnsureLocalDatabaseExists(AddLog);
                if (!flag)
                {
                    return;
                }

                string points = rtbMeasurements.Text;
                var pointList = points.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                if (pointList.Length == 0)
                {
                    AddLog($"测点数据为空，请添加测点");
                    return;
                }
                AddLog($"共获取到 {pointList.Length} 个测量点，开始备份数据...");
                DateTime startTime = dtpStartTime.Value, endTime = dtpEndTime.Value;
                DateTimeOffset dt1 = DateTime.SpecifyKind(startTime, DateTimeKind.Local);
                DateTimeOffset dt2 = DateTime.SpecifyKind(endTime, DateTimeKind.Local);
                long startTimestamp = dt1.ToUnixTimeMilliseconds() / 1000 * 1000, endTimestamp = dt2.ToUnixTimeMilliseconds() / 1000 * 1000;
                StringBuilder sb = new StringBuilder();
                Random rd = new Random();

                double diff = maxNum - minNum;
                foreach (var tag in pointList)
                {
                    AddLog($"----- 开始生成测点数据: {tag} -----");
                    int count = (int)(endTimestamp - startTimestamp) / 1000;
                    AddLog($"----- 开始生成测点: {tag}  时间{startTime:yyyy-MM-dd HH:mm:ss}-----{endTime:yyyy-MM-dd HH:mm:ss}");

                    for (int i = 0; i <= count; i++)
                    {
                        double value = Math.Round(rd.NextDouble() * diff + minNum, 4);
                        long time = startTimestamp + i * 1000;
                        sb.Append($"{localMeasure},tag={tag} value={value} {time}\n");
                        if ((i + 1) % MAX_SIZE == 0)
                        {
                            await influxDBService.WritePointsAsync(sb.ToString());
                            sb.Clear();
                        }

                    }
                    if (sb.Length > 0)
                    {
                        await influxDBService.WritePointsAsync(sb.ToString());
                        sb.Clear();
                    }

                    await Task.Delay(10); // 避免请求过于频繁
                    AddLog($"----- 测点数据已生成 -----");
                }
            }
            catch (Exception ex)
            {
                AddLog("数据生成失败：" + ex.Message);
            }
            finally
            {
                SetOperationButtonsEnabled(true);
            }

        }
    }
}
