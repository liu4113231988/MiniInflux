# 写入瓶颈检查与优化方案

初始检查日期：2026-09-08。以下瓶颈清单依据优化前源码及 `docs/benchmark.md`，属于静态检查，不能当成 CPU profile。随后实施的变更见本文末尾执行记录，实测结果见 `benchmark.md` 的 2026-09-08 章节。

## 当前结论

写入链路为 HTTP 请求体读取 → UTF-8 parser → WriteQueue → 去重/Schema → db/rp 写锁 → WAL → buffer/series/last-value cache → 后台 segment flush。

首要结构性限制是全局单消费者写队列：所有数据库的 engine 写入在一个 worker 中串行执行。单连接场景更应关注逐点对象分配、重复遍历和 WAL 文本编码；持续写入场景应关注 flush 清理持锁时间及编码/磁盘是否跟得上。当前没有证据能把第一 CPU 热点直接归为 parser、CRC 或磁盘。

历史九月记录约 120,940 points/s，只是旧版本环境下单轮结果，不是本次测量。六月记录的“HTTP 直接调用 engine、移除 WriteQueue”与当前代码不一致，不能继续据此判断当前路径。

## 已确认的问题与影响

| 优先级 | 位置 | 已确认的行为 | 主要影响场景 |
| --- | --- | --- | --- |
| P0 | `scripts/compare-with-influxdb.ps1:188` | 接受 Concurrency 参数，但循环中每次 PostAsync 后立即等待，未实际并发；写计时包含客户端创建内容和进度输出，显式 flush 单独计时 | 现有结果无法回答并发扩展性，也不能将 HTTP 吞吐当作完整 segment 落盘吞吐 |
| P1 | `MiniInflux.Engine/Storage/WriteQueue.cs` | SingleReader + 单 worker；按 db/rp 分组后依次调用同步完成的 WriteInternalAsync；BatchSize 按请求数计算，不是点数 | 多连接、多库共享一个串行执行入口；大批次会阻塞后续请求 |
| P1 | 同上及 `MiniInflux/Program.cs:WritePointsAsync` | 请求完整读取并解析后才入队；默认容量 100,000 个请求，最多合并 10,000 个请求；没有排队点数/字节预算 | 高并发时可积压大量 Point 对象；engine buffer 限制不覆盖这些排队对象；一次合并可能过大，触发整组拒绝 |
| P1 | `MiniInflux.Engine/Storage/TsdbEngine.cs:AddWrittenPoints`、`LastValueCache.cs:Update` | 每点调用缓存更新，重复拼接 db/rp key、查外层字典、执行 AddOrUpdate；同时间戳还复制 Fields；容量淘汰用 inner.Keys 获取键集合 | 低基数大批次重复更新同一 series；接近缓存上限的高基数负载可能产生显著分配和延迟 |
| P1 | `MiniInflux/Program.cs:ReadRequestBodyBytesAsync` | 每请求分配 64 KiB 临时数组及 MemoryStream；容量与长度不等时仍 ToArray，注释声称避免最终复制但实现并未总能避免 | 小请求固定分配、大请求扩容及额外复制、gzip 解压后的分配压力 |
| P1 | `MiniInflux.Engine/Storage/TsdbEngine.cs:TryScheduleAsyncFlush/FlushSnapshotAsync` | 启动时持写锁复制全 buffer；完成时持写锁扫描并压缩整个 list，再为剩余数据重建 series 索引 | 持续写入时周期性 p99 延迟、写队列堆积；后台编码不意味着 flush 对写路径完全无阻塞 |
| P2 | `MiniInflux.Engine/Protocol/LineProtocolParser.cs` | 已有 UTF-8 快路径与字符串复用，但缓存命中的 tags 仍复制 Dictionary；每点构造 Fields；复杂行回退 UTF-16 parser | 标签多、字段多、转义和字符串字段多时分配与解析成本增加 |
| P2 | `MiniInflux.Engine/Storage/TsdbEngine.cs` | 去重、Schema、cardinality、buffer/index、缓存分别遍历；两个阶段分别维护本批 seen-series；全批时间戳非严格递增就进入哈希去重 | 常见的“多个 series 同一采样时间”即使没有重复点，也走较重路径 |
| P2 | `MiniInflux.Engine/Storage/Wal.cs:Append` | 每批已经只写一条记录，采用池化 UTF-8 buffer，CRC 已在 WAL 锁外；但仍把 Point 编回文本，并为所有点生成相同 WalPosition 的列表 | 宽行和大批次的编码、CRC、分配；编码虽在 WAL 锁外，仍处于 engine 的 db/rp 写锁内 |
| P2 | `MiniInflux.Engine/Storage/SegmentWriter.cs` | 列构造、压缩、整段 MemoryStream、CRC、fsync；乱序列使用 SortedDictionary 去重排序 | 长时间写入、乱序补数、宽字段及 compaction 重叠时限制可持续吞吐 |

已存在的优化不重复实施：WAL 批记录、CRC 移出 WAL 锁、UTF-8 解析、仅新增 series 更新 manifest、buffer 增量计数、后台大批 flush、磁盘余量检查 5 秒缓存。

## 实施顺序

### 阶段 0：建立可归因的基线

1. 修正压测客户端为固定数量的并发 worker，提前生成 payload，释放每次请求/响应资源；记录实际并发数、成功点数、失败率、每请求 p50/p95/p99。预热与正式计时分开，禁止把失败请求算入吞吐。
2. 增加低开销批次计时：读 body、parse、queue wait、dedup/schema、engine 锁等待/持有、WAL 编码/CRC/append/fsync、buffer/index/LVC、flush snapshot/encode/publish/cleanup。累计计数与直方图优先，避免逐点日志。
3. 采集进程 CPU、分配 bytes/point、GC 暂停、堆大小、排队 points/bytes、buffer 水位、flush backlog、磁盘写入量及延迟。分开分析 HTTP 短测和持续落盘测试。
4. 固定 Release 构建、机器、数据目录和持久化参数；日志开启/关闭单独对照。每组至少 5 次取 median，保留离散程度和原始证据于 `.benchmarks/`。

测试矩阵：batch 1/100/1000/5000；实际 concurrency 1/4/8/16；单 db/rp 与多 db/rp；低基数重复 series 与高基数新增 series；全局递增时间戳、多 series 同时间戳、乱序和重复点；窄字段与宽字段。先取代表组合定位，再扩大覆盖，避免无意义的全组合测试。

短测至少覆盖原有 100,000 点场景；持续测试建议 5–10 分钟、跨越多轮 WAL rotation/flush/compaction。分别报告 HTTP ACK 吞吐、追加 flush 耗时，以及 `成功点数 / (写入时间 + 追加 flush 时间)`。后者只作为本引擎排空成本指标，跨产品对比需统一完成语义。当前 BufferOnly 仅抬高阈值，脚本仍会在写计时后显式 flush，不代表整个运行绝不生成 segment。

### 阶段 1：小范围降低每批固定成本与逐点重复工作

- 请求体先直接返回已有 backing buffer 和有效长度，去掉无必要 ToArray；再评估池化临时缓冲区。必须在成功、解析失败、取消、gzip 超限等路径正确归还，并验证 Point 不引用已归还内存。
- 增加真正的 LVC 批量入口：每批只解析一次 db/rp 容器，复用已算好的 SeriesKey，按 series 聚合本批最大时间戳，同时间戳按原写入顺序合并 Fields，再更新全局缓存。淘汰避免每次构造全量 Keys 快照。保留现有 last-point 查询语义。
- 单请求队列处理快路径避免多余分组容器；合并同时受 requests、points、bytes 上限约束，限制大请求对小请求的阻塞。增加解析前的在途预算和解析后的排队预算，不以增大队列代替背压。
- WAL 内部提供返回单个批位置的接口供 engine 使用，避免重复 positions 数组；保留旧接口兼容现有调用/测试。每点仍正确引用所属 WAL record。
- 合并批内 series/schema 摘要的生成及复用；所有字段仍要校验，不能仅检查第一行。多 series 同时间戳快路径必须证明不存在相同 series/timestamp 后才跳过去重。

每项独立对照测试。建议验收目标为相应负载分配量或阶段耗时下降至少 15%，或端到端 median 吞吐提升至少 10%，且差异超过基线波动；这是验收门槛，不是收益预测。

### 阶段 2：缩短 flush 与写入互相阻塞

- 将 active buffer 与 immutable flushing buffer 分离，锁内交换引用，锁外编码；查询同时读取两代并维持 last-write-wins。
- 每代持有独立 series 索引、计数和 WAL replay floor，flush 成功后整体退休，避免扫描仍在接收写入的 list 和重建其索引。
- 失败保留可读快照和 WAL，重试必须处理部分 shard/segment 已成功发布的情况；DROP、retention、backup、checkpoint 均需覆盖并发语义。
- 为 flush/compaction 设置有界并发与调度预算；只有 CPU 采样证明压缩热点后才改编码算法，只有磁盘证据支持后才调整写入调度。

验收：稳态 buffer/backlog 不持续上涨，flush 期间 p99 明显改善，数据计数与重启恢复一致。不要只测不足一个 flush 周期的峰值。

### 阶段 3：经过并发安全审计后扩展写入 worker

- 按数据库路由到固定数量 worker，保持同库写入有序。第一步选数据库而非 db/rp，是因为 `_seriesKeys[db]` 的值为普通 HashSet；多个 rp 的不同锁无法保护这个共享集合。
- 修正全局 `_bufferedPointCount/_bufferedByteCount` 的并发更新和“检查后占用”原子性；仅把加减换成 Interlocked 不足以保证全局上限。
- 审计 schema、manifest、DDL、WAL checkpoint、跨库 flush 与直接 WriteAsync 调用。单 worker 队列并不覆盖所有调用方和后台任务，不能把它当成完整线程安全保证。
- WAL 仍是全局单文件序列化点；先允许准备工作并行、追加有序，再据实测考虑更大的 group commit。不要首先按 shard 拆 WAL，避免同时引入恢复格式迁移。

验收：多库 concurrency 4/8 相对单 worker 有稳定提升，单库无明显退化，cardinality/内存限额在压力下仍准确，无丢点和重放遗漏。

### 阶段 4：仅在 CPU/GC 仍占主导时推进数据模型改造

将每点 tags/fields 字典逐步改为批内只读 series 描述及紧凑字段数组，评估二进制 WAL，最终减少“文本 → Point 对象图 → 文本 WAL → 列数据”的转换。不能直接恢复 tags 字典共享：当前对象可变，必须先明确所有权并覆盖外部调用方修改和查询隔离。二进制 WAL 需要版本标识、旧记录恢复、混合版本回放与损坏检测测试。

## 持久化与正确性约束

默认 `Wal.Fsync=true`、`FsyncIntervalMs=1000`：Append 返回不等待该请求被 fsync，定时器与 rotation/dispose 执行 Flush(true)。因此成功响应不是逐请求掉电持久化保证，风险窗口也不能宣称严格不超过 1 秒。`FsyncIntervalMs=0` 当前不会自动变成“每请求同步刷盘”，因为构造器仅在大于零时启动定时器，Append 中没有对应分支。

比较优化前后必须维持同一持久化策略；关闭 fsync 不计为等价优化。如需严格 durable ACK，应独立设计按 WAL durable position 完成等待者的 group commit，并单独报告吞吐与延迟。

每次非平凡改动添加针对性测试，覆盖乱序、同时间戳字段合并、字段冲突、请求合并失败隔离、取消/限流、WAL rotation/replay、flush 失败与恢复、查询可见性；并发阶段增加跨 db/rp 限额与 checkpoint 测试。运行完整 xUnit 套件后再提交性能/存储变更。

推荐先做阶段 0 与阶段 1，再按实测选择阶段 2 或 3。单连接写慢时优先减少每点成本；多库并发不扩展时优先分区 worker；持续写 p99 恶化且 flush backlog 上升时优先 flush 路径。

## 2026-09-08 第一批执行记录

已完成：

- 修正并发基准：固定数量异步 worker，payload UTF-8 编码移出计时；记录峰值并发、成功点数、失败信息、请求延迟样本及 p50/p95/p99，写后查询验证点数。预热独立数据库后删除，不污染正式数据。
- 基准增加 `-MiniOnly` 与 `-MiniBinaryPath`，可用同一工具交替测量保存的旧程序和新程序；独立运行目录、自动保存 result.json、检查端口冲突、退出时等待实际服务进程结束，恢复脚本修改过的环境变量。
- HTTP body 返回 backing buffer 与实际长度，消除容量不等于长度时的最终复制。实际 HTTP 冒烟验证覆盖 gzip、chunked、v1/v2、空请求、无效行和解压超限。
- engine 使用批量 LVC 更新；低基数批次先聚合最新点，再更新共享缓存；达到 64 个不同 series 后转直接更新，避免高基数临时字典无限扩张。统一复用 db/rp 容器，使用不捕获变量的更新委托，淘汰不再构造完整 Keys 快照。
- 增加缓存批量合并、64-series 边界、输入对象隔离、容量与 db 隔离测试，以及基准真实并发和失败点数测试。完整测试 417 项通过，Release 服务构建通过。

本批未实施：engine 内各阶段耗时埋点、解析前在途预算、队列点数/字节预算、WAL 批位置接口、flush 双缓冲、多 worker、二进制 WAL。没有改变 fsync/flush 配置或持久化格式。

后续优先补齐 flush snapshot/encode/cleanup 与写锁等待测量，确定后台编码、数据积压和锁内清理各自占比，再推进阶段 2；当前短测中 ACK 吞吐的变化不足以证明持续落盘能力改善，不直接进入多 worker 改造。

## 2026-09-08 第二批执行记录

- 增加默认关闭的进程级分阶段计时，通过启动环境变量 `MiniInflux__WriteDiagnostics=true` 启用，`/debug/stats` 输出累计毫秒和次数；基准脚本支持 `-WriteDiagnostics`，计算正式写入前到写后 flush 完成的增量。
- 覆盖写锁等待/持有、WAL append、buffer/index/cache 更新、flush snapshot、flush 编码、cleanup 等待/持锁、列构造、时间戳/字段编码、文件写出/CRC/fsync。阶段存在嵌套和并行，累计时间不是 CPU 采样占比。
- 根据百万点测量，优先优化占时最大的列构造：普通乱序列按原始索引排序并原地重排；128 点均匀采样发现较多重复时间戳时，采用哈希去重后排序唯一时间戳。两条路径完整执行去重，保留每字段 last-write-wins；采样只影响性能策略。
- 浮点 adaptive 编码复用 legacy 原始字节，避免为 raw/Brotli 两个候选重复编码。时间戳编码在首轮诊断中成本较低，本批未改动时间戳共享或缓冲区结构。
- 三种段文件对照（顺序、批次乱序、大量重复点）输出 SHA-256 完全一致；补充乱序/极值/重复点、编码候选字节兼容测试。修正保留策略测试跨整点时两个输入分属不同 shard 的夹具问题；全量 420 项通过。
- 百万点、4 并发、前后各 5 轮，全部成功且写后点数一致：含追加 flush 吞吐 median 207,069 → 232,879 points/s（+12.5%），追加 flush 915 → 425 ms；ACK 吞吐 257,361 → 247,018 points/s（-4.0%）。数据与局限见 `benchmark.md`。

下一轮优先细分 WAL 编码、CRC、文件锁等待/写出及 fsync 阻塞，再决定写入热路径改动。队列点数/字节预算、双缓冲、多 worker、二进制 WAL 仍未实施；本批未改变持久化配置或文件格式。

## 2026-09-08 第三批：WAL 内部优化

- 完成 WalEncode/WalCrc/WalLockWait/WalFileWrite/WalRotate/WalFsync 诊断，确认文本编码是 WAL 主要成本。
- 分隔符直接写字节、布尔值使用 UTF-8 常量、池化缓冲调用具体类型；引擎使用 AppendBatch 的单个共享位置，原 Append 接口保留。
- 保持文件格式、CRC、轮转与 fsync 行为；Unicode 和大 payload 扩容、空批次、轮转恢复测试通过，全量 422 项通过。
- 百万点、4 并发、前后各 5 轮：编码累计耗时中位数下降 30.5%，ACK 吞吐提升 15.0%，含追加 flush 吞吐提升 7.2%；请求 p99 增加 7.4%，追加 flush 时间增加 31.8%。详见 benchmark.md，不视为尾延迟改善。
- 后续优先测量持续写入的 flush backlog 与 p99，再决定积压预算和 flush 调度；本批未实施二进制 WAL、多 worker 或严格 durable ACK。
