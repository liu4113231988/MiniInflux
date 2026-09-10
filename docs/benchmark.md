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

## 2026-09 优化批次复测

2026-09 批次（正确性修复、Manifest/WAL/compaction/LVC/查询热路径性能优化、备份一致性、磁盘门控、缓存上限、token 分级、CSV、部分过期）落地后按标准口径复测：`100,000` points、`batch size = 5,000`、`concurrency = 1`，查询延迟 5 次 median；MiniInflux 由脚本以 `dotnet run -c Release --no-restore` 启动，写后显式 flush。

| Metric | MiniInflux | InfluxDB 1.7.9 | Ratio |
| --- | ---: | ---: | ---: |
| 100,000 points write throughput (points/s) | 120,940.45 | 276,902.23 | InfluxDB 1.7.9 is `2.29x` |
| 100,000 points aggregate query latency median (ms) | 1.09 | 3.34 | MiniInflux is `3.06x` faster |
| 100,000 points raw `LIMIT 1000` query latency median (ms) | 3.48 | 5.36 | MiniInflux is `1.54x` faster |

结论：查询方向较 2026-07-04（aggregate `1.9ms`、raw `12.9ms`）继续改善，aggregate 与 raw 均稳定快于本机 InfluxDB 1.7.9。写入 `120,940 pts/s` 与 2026-06-30 收口轮（`120,418`）持平——本批次未针对写入主路径做专门优化（WAL CRC 出锁的收益被 HTTP/解析侧噪声掩盖）。注意本轮 InfluxDB 写入读数 `276,902 pts/s` 明显高于其历史区间（`77k-159k`），单轮对比仅作参考；MiniInflux 侧的稳定结论是写入维持 `12-13 万 pts/s` 级别、查询持续领先。

## 2026-09-08 写入基准修正与缓存批量更新

本轮先修正压测方法，再保存优化前 Release 二进制，与最终代码交替测量。旧脚本虽然接受 `Concurrency`，实际逐请求等待；当前脚本由固定数量异步 worker 发起请求，记录峰值并发、成功点数、失败请求、逐请求延迟及写后 `count(value)` 校验。UTF-8 payload 在计时前生成，独立数据库预热 5 个 batch 后删除；服务直接运行已构建 DLL，不将启动或构建计入写吞吐。结果不可与旧脚本历史数字直接比较。

生产代码改动：HTTP 读取返回已有 buffer 和有效长度；LVC 批内聚合最新点，达到 64 个不同 series 后使用直接更新路径以限制临时空间；复用缓存容器和静态委托，淘汰不再复制所有 key。保留默认 `Wal.Fsync=true`、`FsyncIntervalMs=1000`、flush threshold 50,000，未改变持久化格式或确认语义。

### HTTP 对照

同机、Release、16 个 series、batch size 5,000、独立数据目录、关闭访问日志。每组旧/新版本各 5 次，交替先后顺序。表格为各轮指标的 median；p99 列是“每轮 p99 的 median”，不是将所有请求混在一起求 p99。

| 数据量 / 并发 | 指标 | 修改前 | 修改后 | 变化 |
| --- | --- | ---: | ---: | ---: |
| 100,000 / 1 | HTTP ACK points/s | 170,266 | 166,832 | -2.0% |
| 100,000 / 1 | 写入 + 追加 flush points/s | 137,216 | 128,313 | -6.5% |
| 100,000 / 1 | 请求 p99 ms | 68.50 | 56.50 | -17.5% |
| 100,000 / 4 | HTTP ACK points/s | 207,054 | 231,724 | +11.9% |
| 100,000 / 4 | 写入 + 追加 flush points/s | 120,076 | 110,101 | -8.3% |
| 100,000 / 4 | 请求 p99 ms | 158.82 | 125.56 | -20.9% |
| 1,000,000 / 4 | HTTP ACK points/s | 212,731 | 215,263 | +1.2% |
| 1,000,000 / 4 | 写入 + 追加 flush points/s | 170,769 | 169,813 | -0.6% |
| 1,000,000 / 4 | 请求 p99 ms | 238.71 | 213.35 | -10.6% |

30 次正式对照全部请求成功、写后查询点数正确。100,000 点仅 20 个请求，短测受 JIT、调度和后台 flush 时间影响较大；百万点各轮 ACK 区间仍宽：修改前 107,404–229,871，修改后 141,653–235,741 points/s。没有足够证据宣称整库持续吞吐提高，也没有跑满 5–10 分钟的稳态/compaction 测试。短测写后排空指标下降如实保留，不能只用 ACK 增长宣称优化完成。

### 缓存路径独立测量

隔离 `LastValueCache.UpdateMany`，提前生成 80 × 5,000 点，测试 16 和 5,000 个 series；先预热 2 次，再测 5 次取 median，分配统计使用当前线程 allocated bytes，不含输入点构造。为消除分层编译阶段差异，以下一组双方均设置 `DOTNET_TieredCompilation=0`，只说明该方法的开销，不能等同于服务器默认运行模式或整个写入链路的分配量。

| series 数 | 400,000 点耗时：前 → 后 | 分配 bytes/point：前 → 后 |
| --- | ---: | ---: |
| 16 | 81.56 → 36.18 ms | 184.02 → 0.26 |
| 5,000 | 213.94 → 169.62 ms | 186.16 → 3.69 |

保留默认分层编译再次独立测量（同样预热 2 次、采样 5 次），16 series 耗时 median 为 64.88 → 50.64 ms，分配 56.01 → 0.26 bytes/point；5,000 series 为 199.95 → 155.46 ms，分配 58.83 → 3.42 bytes/point。此条件下两场景缓存耗时均下降约 22%，缓存路径分配下降约 94%–99.5%。原始数据为 `.benchmarks/cache-baseline-default-final-20260908.json` 和 `.benchmarks/cache-final-default-20260908.json`。

缓存路径减少分配的收益已经确认；整库下一步应测量 flush 编码、snapshot/cleanup 写锁时间和 backlog，再决定缓冲区改造。不能据本轮将 parser 或磁盘定为已采样确认的首要热点。

### 复现与验证

```powershell
# 默认同时比较本地 InfluxDB 1.7.9；MiniOnly 用于新旧 MiniInflux 二进制对照。
.\scripts\compare-with-influxdb.ps1 -Points 1000000 -BatchSize 5000 -Concurrency 4 -MiniOnly -MiniBinaryPath .\MiniInflux\bin\Release\net10.0\MiniInflux.dll
```

每次运行保存 `.benchmarks/write-<id>/result.json` 和独立数据目录。`-MiniBinaryPath` 省略时先构建当前 Release 程序；`-BufferOnly` 只抬高自动 flush 阈值，写后仍执行显式 flush。`Valid=false` 表示存在失败请求或点数校验不符，该轮不能当作完整成功结果。InfluxDB 没有相同的强制排空步骤，因此其 `WriteAndFlushThroughput` 为 null，不跨产品比较该列。

本地汇总证据：`.benchmarks/write-comparison-final-20260908.json`、`.benchmarks/write-comparison-million-20260908.json`、`.benchmarks/cache-baseline-notier-20260908.json`、`.benchmarks/cache-final-20260908.json`；缓存测量源程序保存在 `.benchmarks/cache-probe/`。中间候选版本数据独立保留，不纳入上表，生成文件不提交。

验证：完整 xUnit 417 项通过；完整解决方案与 Release 服务构建通过；实际 HTTP 冒烟覆盖 gzip、chunked、v1/v2、空 body、无效行及解压超限，最终有效点数为 2。另用 10,000 点 / 并发 4 检查两产品比较流程，两边均峰值并发 4、零失败且点数正确；该轮仅验证工具兼容性，不作产品性能结论。

## 2026-09-08 第二批：按阶段测量优化乱序列构造

对照基线为第一批代码加相同的诊断计时，保存在 `.benchmarks/write-phase2-baseline/`。双方均开启 `-WriteDiagnostics`；配置仍为默认 WAL fsync/间隔和 flush threshold 50,000，Release 服务、16 series、batch 5,000，独立目录与预热方法同上一批。没有修改段文件格式或持久化语义。

初始诊断的百万点运行中，列构造累计 3,794 ms、字段编码 637 ms、时间戳编码 92 ms、文件写出/CRC/fsync 68 ms、写锁等待 155 ms。列构造优先级高于时间戳共享或改变刷盘策略，因此本批针对乱序列的逐点树插入成本优化：普通乱序使用索引排序及原地重排；重复点较多时先哈希去重再排序唯一时间戳。两条路径都保留相同时间戳最后写入的字段值。浮点 adaptive 编码同时复用两个 legacy 候选的基础字节，压缩候选、选择规则不变。

### 百万点 HTTP 前后对照

1,000,000 点、4 并发，旧/新各 5 次交替运行，各指标取轮次 median。10 次全部零失败且写后 `count(value)` 为 1,000,000。

| 指标 | 修改前 | 修改后 | 变化 |
| --- | ---: | ---: | ---: |
| HTTP ACK points/s | 257,361 | 247,018 | -4.0% |
| 写入 + 追加 flush points/s | 207,069 | 232,879 | +12.5% |
| 追加 flush ms | 915.14 | 425.25 | -53.5% |
| 请求 p99 ms（每轮 p99 的 median） | 176.02 | 151.81 | -13.8% |
| 列构造累计 ms | 2,969.75 | 1,575.68 | -46.9% |
| flush 编码阶段累计 ms（含列构造/持久化） | 3,936.72 | 3,083.23 | -21.7% |
| WAL append 累计 ms | 2,270.43 | 2,459.75 | +8.3% |
| flush cleanup 持锁累计 ms | 204.45 | 215.04 | +5.2% |
| 写锁等待累计 ms | 123.36 | 125.92 | +2.1% |

阶段是 wall-clock 累计计时，可能嵌套和并行，不能相加，也不能据此计算 CPU 百分比。更快的 flush 会改变批次大小、完成时点及后台资源竞争；ACK 和 cleanup 未同步改善如实保留。该负载证明排空吞吐改善，尚不代表长时间 steady-state、compaction 混合负载或所有数据分布都有相同收益。

### 固定输入段文件对照

提前生成每组 200,000 点；单进程每场景预热 2 次，测 5 次取 median，默认分层编译，双方开启诊断。分配量只计当前线程执行 `WriteSegment` 的部分，不含输入点构造；耗时包括 fsync。每组前后全部生成文件的 SHA-256 一致。

| 场景 | 总耗时 ms：前 → 后 | 分配 bytes/point：前 → 后 |
| --- | ---: | ---: |
| 顺序 | 183.28 → 136.93 | 308.39 → 279.88 |
| 批次乱序 | 210.99 → 148.32 | 468.43 → 288.31 |
| 大量重复点 | 98.72 → 84.62 | 224.42 → 230.12 |

主要确定收益为乱序列构造的时间和分配下降；重复点路径耗时下降约 14%，分配增加约 2.5%，为本批取舍。顺序路径列构造算法未变，其时间仍受 JIT/调度波动影响，不将整项下降全部归因于代码。采样最多 128 个时间戳，以重复比例约 25% 为策略分界；估计不准会影响性能，但不会跳过去重或改变输出。

复现诊断：

```powershell
.\scripts\compare-with-influxdb.ps1 -Points 1000000 -BatchSize 5000 -Concurrency 4 -MiniOnly -WriteDiagnostics -MiniBinaryPath .\MiniInflux\bin\Release\net10.0\MiniInflux.dll
```

诊断默认关闭，仅在进程启动读取环境变量 `MiniInflux__WriteDiagnostics=true`。`/debug/stats` 输出累计时间和次数，脚本截取正式写前/flush 后增量；数据为进程级，不按数据库划分。`SegmentPersist` 包含文件写出、CRC 和 fsync，不包含最终 rename；`BufferAppend` 包含索引和 LVC 更新；第二批尚未细分 WAL 内部阶段（见下节），parser 和队列等待仍未细分。

证据：`.benchmarks/phase2-million-comparison.json`、`.benchmarks/segment-probe-baseline-final.json`、`.benchmarks/segment-probe-adaptive.json`，以及 `.benchmarks/segment-probe/` 内测量程序；中间策略结果不纳入上表。完整 xUnit 420 项通过，Release 构建通过。保留策略测试修复了整点边界导致输入跨 shard 而不触发 compaction 的夹具问题，未改变运行时保留策略。

## 2026-09-08 WAL 内部优化

在第二批结果上细分 WAL 编码、CRC、文件锁等待、文件写入、轮转及 fsync。首次百万点诊断中，WalEncode 累计 2054.54 ms，WalAppend 2269.22 ms，编码约占 90.5%；CRC 53.89 ms、文件写入 29.37 ms。因此本批优化编码而非调整刷盘频率。

改动：ASCII 分隔符直接写字节，布尔值使用 UTF-8 常量，编码辅助方法使用具体池化缓冲类型，减少接口调用及游标检查。新增 AppendBatch 返回一个共享记录位置，引擎直接复用该位置，消除逐点重复位置数组；保留原 Append 接口供既有调用者使用。没有修改 WAL 格式、CRC 算法、字段枚举顺序、轮转规则或 fsync 策略。

百万点、每请求 5000 点、4 并发，Release 新旧程序各 5 轮，轮间交替并反转先后顺序；双方启用相同诊断，每轮独立目录并预热。所有 10 轮无失败、实际并发为 4，写后验证各 100 万点。下表为每轮指标的中位数：

| 指标 | 优化前 | 优化后 | 变化 |
| --- | ---: | ---: | ---: |
| ACK 吞吐 points/s | 253,735.61 | 291,845.13 | +15.0% |
| 写入加追加 flush 吞吐 points/s | 228,460.19 | 244,910.24 | +7.2% |
| 每轮请求 p99 ms | 159.95 | 171.72 | +7.4% |
| 写后追加 flush ms | 483.51 | 637.48 | +31.8% |
| WalEncode 累计 ms | 1984.34 | 1378.95 | -30.5% |
| WalAppend 累计 ms | 2305.14 | 1673.78 | -27.4% |
| WriteLocked 累计 ms | 2949.07 | 2358.83 | -20.0% |

吞吐改善伴随 p99 与追加 flush 时间增加，不能称为尾延迟优化，也不能从短测推断持续写入稳定性。更快接收写入可能让 flush 更集中，但本次数据不足以证明原因。CRC 中位数 55.12 → 57.20 ms，文件写入 29.51 → 28.30 ms，尚不是主要热点。

WalFsync 包括后台定时、轮转、最终关闭的刷盘计时；脚本增量截止写后 flush，因此不包含随后关闭进程的耗时。阶段嵌套或并行，尤其后台 fsync 不全部属于 Append，不能相加当作 CPU 时间。默认 ACK 仍不保证该批已经 fsync，本次没有放宽或增强持久化语义。

新增测试验证空批次不推进位置，以及 Unicode、混合字段、大字符串缓冲扩容的精确 payload 和轮转后批次位置恢复。全量 422 项测试通过，Release 构建通过。证据保存在本地 `.benchmarks/wal-profile-before.json`、`.benchmarks/wal-comparison.json`，优化前程序为 `.benchmarks/wal-baseline/`；这些生成文件不提交。复现沿用上节命令，旧程序通过 MiniBinaryPath 指定。

## 2026-09-09 持续写入与并发压力测试

本轮使用 scripts/sustained-write.ps1：每批 5,000 点、16 series、单整数 field、唯一时间戳、本机闭环发压。数据生成计入总时间，HTTP 延迟单独计量；与此前浮点等负载不同，不能直接比较吞吐。默认持久化策略保持不变，启用阶段诊断，每 2 秒采集服务进程与存储统计。

| 测试 | 4 并发，300 秒 | 16 并发，120 秒 |
| --- | ---: | ---: |
| HTTP 成功点数 | 150,960,000 | 54,990,000 |
| flush 后首次 COUNT | 150,960,000 | 54,230,000 |
| 请求失败数 | 0 | 0 |
| 校验结果 | 通过 | **失败，少 760,000 点** |
| ACK points/s | 503,024 | 457,506（仅观测，不作为有效成绩） |
| 请求 p95 ms | 128.99 | 378.06 |
| 请求 p99 ms | 215.30 | 620.23 |
| 私有内存采样峰值 MiB | 5,702.6 | 3,777.9 |
| buffer 点数采样峰值 | 545,000 | 450,000 |

4 并发在 30–60 秒窗口为 586,167 points/s、p99 158.10 ms，末 30 秒为 412,333 points/s、p99 249.21 ms。buffer 未持续增长至上限，合并完成 9 次且仍有后台任务运行，不能据此认定所有后台工作已追平。内存随后回落，尚无 GC 或分配分析，不能确认泄漏或具体争用来源。

16 并发首次计数不足时 buffer=0、queue=0，后台 compaction 正在运行。保留原目录，用数据副本重启服务后连续三次 COUNT 均为 54,990,000，提示运行中的查询可见性问题；不能直接认定永久丢失。候选检查点是合并发布后旧段立即删除、读侧文件缺失跳过、元数据索引失效时机。根因需确定性复现，尚未修复。

每种并发仅一轮、时长不同；16 并发首约 30 秒与前一轮汇总短暂重叠，不用于稳定阶段比较。本轮保存请求 JSON 后才发起最终 flush，38.93/98.16 ms 不代表停止发压瞬间的积压；脚本已将保存移到 flush/count 后供下轮使用。默认 MemoryBufferBytes=0 表示未启用字节跟踪。阶段累计时间嵌套且并行，不是 CPU 占比。COUNT 验证不是逐值或崩溃恢复验证。

证据：.benchmarks/sustained-a3d2145cbfd243bbbfb7cab92eb9a47c/ 与 .benchmarks/sustained-99b6fa0ff6d44a84b28f42f075ca83a4/，包含请求、遥测、原始计数和摘要；第二个目录另含副本重启复查。优化顺序和验收条件见 [持续写入优化计划](todo-write-optimization-20260909.md)：先解决计数异常，再补齐观测、控制合并内存、减少 flush 清理持锁、增加队列点数/字节预算。引擎未改动，Release 构建通过。

## 2026-09-10 Compaction 查询一致性修复复测

增加读取凭据保护合并输入文件；编码可继续并行，manifest 发布、缓存失效与旧段删除等待读取完成。覆盖引擎 COUNT、原始读取、降序快路径与流式 SELECT；凭据支持跨线程及重复释放。长查询会延迟合并发布，不代表完整读写事务快照。详细执行记录见 todo-write-optimization-20260909.md。

回归测试覆盖冷/热缓存下暂停读取、合并输出完成但尚未发布、嵌套 COUNT、完整原始值及发布后计数，以及跨线程释放和取消。全量 425 项通过；Release 构建通过，已有编译警告保留。

16 并发、每批 5,000 点、120 秒第一轮复测：成功 68,675,000 点、失败 0，连续三次 COUNT 均一致；约 572,047 points/s，p99 340.42 ms。证据：.benchmarks/sustained-2ed753b37a04473aae7a2d99f17f5eef/。这次用于验证正确性，运行日期/机器状态不同，不将对旧失败轮次的吞吐差异归因于修复。

第二轮（最终代码，16 并发、120 秒）：成功 70,780,000 点，连续三次 COUNT 都一致；但启动第 7.805 秒集中返回 8 次 HTTP 429，本轮 Valid=false，不计作零拒绝吞吐成绩。2 秒采样在相邻窗口看到 buffer 升到 680,000 点；结合固定 16 series、16 请求和约 200 ms 的拒绝延迟，怀疑瞬时 buffer 上限保护，旧日志无法直接确认具体异常，不能将采样峰值当作瞬时峰值。保留原始结果：.benchmarks/sustained-43d4d2a24e294e07886d35dec5629a65/。成功点数没有少计；不通过提高 buffer 上限掩盖拒绝，后续背压优化仍保留在计划中。

第三轮（开启独立警告日志，16 并发、120 秒）：成功 70,720,000 点，失败 0，连续三次 COUNT 均为 70,720,000；约 588,901 points/s，p99 306.30 ms。日志未出现写入拒绝，未复现第二轮的 429。证据：.benchmarks/sustained-95f2e9d7df7b4b7f92803b8c5d1f16bf/。三轮均未再出现成功点数少计，其中两轮严格校验通过、一轮因 429 未通过；不抹去失败轮次，也不承诺任意压力下零拒绝。
