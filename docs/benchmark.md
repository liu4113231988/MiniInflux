# Benchmark Results

本文件记录 MiniInflux 与本地 `InfluxDB 1.7.9` 的 HTTP 接口压测历史。复现脚本见 [`scripts/compare-with-influxdb.ps1`](scripts/compare-with-influxdb.ps1)。

## 2026-06-28 压测对比

测试口径：

- MiniInflux 使用 `dotnet run -c Release --no-restore` 本地启动，关闭认证，监听 `127.0.0.1:18086`
- InfluxDB 使用本地路径 `D:\workingfold\Influxdb\influxdb-1.7.9\influxd.exe` 启动，监听 `127.0.0.1:18087`
- 两边都使用独立数据目录，避免历史数据干扰
- 每轮写入 `20,000` 条 Line Protocol 点，`batch size = 2,000`，单连接顺序压测
- 查询 1：`SELECT mean(value),count(value) FROM cpu WHERE host='server00' AND region='cn'`
- 查询 2：`SELECT * FROM cpu WHERE host='server00' AND region='cn' ORDER BY time DESC LIMIT 1000`
- 共跑了两轮，以下表格展示两轮平均值

| Metric | MiniInflux | InfluxDB 1.7.9 | Ratio |
| --- | ---: | ---: | ---: |
| Write throughput (points/s) | 30,007.44 | 139,590.99 | InfluxDB 1.7.9 is `4.65x` |
| Aggregate query latency (ms) | 175.62 | 1.92 | MiniInflux is `91.70x` slower |
| Raw `LIMIT 1000` query latency (ms) | 47.47 | 7.38 | MiniInflux is `6.44x` slower |

两轮原始结果：

- Round 1: MiniInflux `23,541.00 pts/s`, aggregate `198.89 ms`, raw `54.92 ms`; InfluxDB 1.7.9 `138,987.24 pts/s`, aggregate `2.05 ms`, raw `7.58 ms`
- Round 2: MiniInflux `36,473.89 pts/s`, aggregate `152.34 ms`, raw `40.02 ms`; InfluxDB 1.7.9 `140,194.74 pts/s`, aggregate `1.78 ms`, raw `7.17 ms`

结论：

- 在这轮本地单机 HTTP 压测里，MiniInflux 明显慢于 InfluxDB 1.7.9，尤其是简单聚合查询差距最大。
- 写入侧 MiniInflux 已经能稳定跑到约 `3.0 万 pts/s`，说明主链路可用，但距离成熟的 InfluxDB 1.x 仍有明显性能差距。
- 这组数据更适合当“当前基线”，不应直接外推成生产承诺；如果后续要继续优化，优先看查询执行路径、索引过滤、聚合下推和 HTTP 写入批处理开销。

## 2026-06-28 P0 优化后一轮复测

P0 优化内容包括：benchmark 阶段耗时、manifest 索引延迟落盘、segment metadata 缓存、简单聚合 buffer stats fast path、buffer 按 series 候选扫描、关闭压测场景访问日志开销，以及 WAL 写入不再为 tag 排序。

测试口径同样是本机 HTTP、单连接顺序写入；脚本对查询做 warmup 后再计时，并对 MiniInflux 额外采集 query report。

| Metric | MiniInflux | InfluxDB 1.7.9 | Ratio |
| --- | ---: | ---: | ---: |
| Write throughput (points/s) | 32,502.22 | 77,447.22 | InfluxDB 1.7.9 is `2.38x` |
| Aggregate query latency (ms) | 14.18 | 2.25 | MiniInflux is `6.30x` slower |
| Raw `LIMIT 1000` query latency (ms) | 15.64 | 8.65 | MiniInflux is `1.81x` slower |

MiniInflux 聚合查询 report：`ScannedPoints=1250`、`UsedAggregatePushdown=true`、`UsedSeriesIndexPushdown=true`、`DurationMs=0`。这说明 P0 后聚合主路径已经命中索引与统计下推，剩余差距主要在 HTTP 总耗时、响应序列化和更底层 segment metadata / 存储格式演进。

## 2026-06-28 写入专项复测

当前压测脚本默认会在 MiniInflux 写入计时后显式 flush 并生成 segment 文件，同时单独输出 `FlushAfterWriteMs`，避免把 WAL + 内存 buffer 口径误当成完整落盘；如需复现旧的纯写入/WAL 口径，可显式传入 `-BufferOnly`。

| Metric | MiniInflux | InfluxDB 1.7.9 | Ratio |
| --- | ---: | ---: | ---: |
| 100,000 points write throughput (points/s) | 89,660.11 | 114,307.84 | InfluxDB 1.7.9 is `1.27x` |
| 100,000 points aggregate query latency (ms) | 3.82 | 8.00 | MiniInflux is `2.09x` faster |
| 100,000 points raw `LIMIT 1000` query latency (ms) | 26.34 | 13.25 | MiniInflux is `1.99x` slower |

Raw `ORDER BY time DESC LIMIT 1000` 在 buffer-only 单 series 场景已命中倒序 LIMIT 下推，MiniInflux debug report 从扫描 `6,250` points 降到 `1,000` points，内部 `DurationMs=4`；HTTP wall time 仍主要受 JSON/响应输出波动影响。

## 2026-06-29 raw 输出路径结构性优化复测

本轮不再继续做小循环级改动，改为给普通 `/query` 增加 buffer-only raw `ORDER BY time DESC LIMIT` 专用 JSON 输出路径：命中单 series、无 group/function/subquery/field filter、且数据仍在内存 buffer 时，直接生成响应 JSON，绕过 `QueryResponse -> QuerySeries -> Values -> List<List<object?>>` 对象图；同时 fast JSON 将固定 series tag 写入 `Tags`，不再在每行 `values` 中重复输出 tag 列。根据 InfluxDB v1 官方 API 文档，`/query` 支持 `epoch=ns|u|ms|s|m|h` 返回 Unix epoch 时间戳，默认才返回 RFC3339；脚本已补 `-Epoch` 参数用于复测官方轻量时间戳模式。

复测口径：`100,000` points、`batch size = 5,000`、`concurrency = 1`；MiniInflux 仍由脚本以 `dotnet run -c Release --no-restore` 启动。查询延迟改为 5 次采样取 median，避免单次毫秒级请求被本机进程调度噪声误导。

| Metric | MiniInflux | InfluxDB 1.7.9 | Ratio |
| --- | ---: | ---: | ---: |
| 100,000 points write throughput (points/s) | 59,579.21 | 158,393.08 | InfluxDB 1.7.9 is `2.66x` |
| 100,000 points aggregate query latency median (ms) | 5.04 | 8.34 | MiniInflux is `1.65x` faster |
| 100,000 points raw `LIMIT 1000` query latency median (ms) | 5.92 | 9.09 | MiniInflux is `1.54x` faster |
| 100,000 points raw `LIMIT 1000` response bytes | 42,532 | 48,496 | MiniInflux response is `12.30%` smaller |

`epoch=ns` 复测：MiniInflux raw median `2.97 ms`、InfluxDB 1.7.9 raw median `7.84 ms`；MiniInflux raw 响应体 `29,532` bytes，InfluxDB `45,496` bytes。结论：当前 raw 查询方向已经不是最优继续投入点；下一轮应优先转向写入吞吐，raw 侧只保留 median 压测和 `epoch` 兼容维护。

## 2026-06-30 写入主链路复测

参考 InfluxDB v1 官方文档中关于 WAL/cache/TSM 写入路径和批量写入建议，以及 InfluxDB parser 保留 key/field 原始字节片段、减少重复 token 分配的实现思路，本轮停止继续追 raw 查询，转向 `/write` 热路径：WAL 改为同一 HTTP batch 写一条多行 record，减少每点 header/CRC/write 调用；`/write` 入口直接写入 engine，避免 Channel/TCS/后台 worker 的同步等待绕路；移除不再使用的 `WriteQueue` 启动注册；未开启 `MaxBufferPoints` 时不再每批扫描全部 buffer 计数；无重复点写入直接复用 parser 返回的 point list；写入内存 series cardinality 改用 `SeriesKey`，不再为每个点调用 record `ToString()`；simple parser 去掉 tag/field 计数预扫描和整批精确行数预扫描，并在单次 batch 内复用重复的 measurement/tag/field 字符串。

复测口径仍为 `100,000` points、`batch size = 5,000`、`concurrency = 1`，查询延迟为 5 次 median。

| Metric | MiniInflux | InfluxDB 1.7.9 | Ratio |
| --- | ---: | ---: | ---: |
| 100,000 points write throughput (points/s) | 120,418.14 | 159,606.27 | InfluxDB 1.7.9 is `1.33x` |
| 100,000 points aggregate query latency median (ms) | 3.70 | 6.45 | MiniInflux is `1.74x` faster |
| 100,000 points raw `LIMIT 1000` query latency median (ms) | 5.05 | 8.71 | MiniInflux is `1.72x` faster |

结论：当前写入差距已从上一轮 median 口径的 `2.66x` 收敛到约 `1.3x`，最好一轮 MiniInflux 写入达到 `132,987.99 pts/s`。继续优化若要再明显推进，应进入更接近 InfluxDB 的 raw key/field 或列式 batch 写入模型；近期不再继续做小刀式写入优化。

收尾补充：在不大改持久化格式的前提下，先落地 raw key/field 方向中确定收益的一部分：simple parser 对同一 HTTP batch 内重复 series 复用已解析 tags 字典和 canonical tag key，避免每行重复拆 tag；写入内存路径合并 buffer append、buffer series index、全局 series keys 和 manifest index 的遍历，并且 manifest 只接收本批唯一 series。该改动保持现有 `Point` 模型和 WAL/segment 格式不变，作为近期写入优化收口。

收尾复测同口径结果：MiniInflux write `133,971.07 pts/s`、aggregate median `4.53 ms`、raw median `5.95 ms`；InfluxDB 1.7.9 write `94,931.71 pts/s`、aggregate median `30.67 ms`、raw median `9.39 ms`。本轮 InfluxDB 结果较历史波动偏低，单轮结果只说明 MiniInflux 写入路径已推进到 `13 万 pts/s` 级别，后续判断稳定差距仍应看多轮 median。

## 2026-07-04 当前性能状态

压测脚本已修正为默认在 MiniInflux 写入计时后显式 flush，并输出 `FlushAfterWriteMs` 与 segment/WAL 文件统计；`-BufferOnly` 仅用于复现纯 WAL/buffer 口径。当前 `/write` 更接近 InfluxDB 1.x 的 WAL/cache 模型：写请求写入 WAL + 内存 buffer 后返回，segment 落盘由后台或显式 flush 完成。

最新 100,000 points / batch size 5,000 / concurrency 1 复测中，MiniInflux 写请求吞吐约 `87k pts/s`，aggregate median 约 `1.9 ms`，raw `LIMIT 1000` median 约 `12.9 ms`，并在写后 flush 生成 segment。后续若继续优化，优先看 flush/segment 编码成本和更大规模混合 workload，而不是继续小改查询主路径。
