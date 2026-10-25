# 已完成路线图总览

按月份整理已交付的能力，作为完整历史记录；当前活跃的待办见 [todo-202610.md](todo-202610.md)。每月先给简要摘要，再附详细记录。

---

## 2026-08

InfluxDB 3 对齐第一阶段，全部完成。

### 简要摘要

- **v2 写入 API 兼容**：`POST /api/v2/write` 支持 `bucket`（含 `db/rp` 拆分）、`org` 忽略、gzip 解压与请求体上限、v2 风格错误体、`Authorization: Token`/`Bearer`。
- **Last Value Cache**：写路径增量更新，flush 后按段 footer 校验回填；`last()` 与 `SELECT * … ORDER BY time DESC LIMIT 1` 走缓存，二次命中 <10 ms。
- **Token 认证体系**：Bearer 与 Basic 并存，多命名 token 的创建/列出/吊销，`tokens.json` SHA256 落盘、列表脱敏。
- **v3 查询端点**：`POST /api/v3/query_influxql` 接收 `{db, q, params?, format?, epoch?}`，统一三种参数来源；顺带修复 gzip 响应压缩中间件只设头不包装的缺陷。
- **参数化查询**：WHERE `$name` 占位符解析期提取，解析树级别绑定防注入，模板经有界缓存复用，v1/v3 双入口统一接入。
- **查询功能补齐**：剩余数学函数（`EXP`/`LOG`/`LN`/三角函数/`ATAN2` 子集）、字符串字段等值 WHERE、布尔括号分组、HTTP 响应 gzip 编码、`SHOW TAG VALUES/SERIES WITH MEASUREMENT` 过滤。

### 详细更新说明

更新范围：2026-08-01 至 2026-08-28。本月更新集中在 InfluxQL 兼容性、读写与存储路径性能、v2/v3 API 互操作、安全认证，以及管理台可用性。

#### 查询与兼容性

- 扩展 InfluxQL 查询能力：支持 WHERE 中的 tag `OR` 和括号组合、`DISTINCT`、epoch 时间输出、更多聚合/数学函数、`EXPLAIN` 与查询管理。
- 补齐常用运维查询：`SHOW SHARDS`、`SHOW STATS`、`SHOW DIAGNOSTICS`、基数统计，以及更多 `SHOW` 过滤条件。
- `/query` 与 `/api/v3/query_influxql` 支持 `$name` 参数化 WHERE 条件。参数在解析树中绑定，不会拼接回查询文本；相同查询模板可复用解析缓存。
- 新增 `POST /api/v3/query_influxql`，接受 JSON 请求体 `{ db, q, params?, format?, epoch? }`，并保持 InfluxQL 查询结果信封兼容。

#### 写入、存储与性能

- 新增 `POST /api/v2/write` 兼容端点，支持 `bucket` 到 `db/rp` 的映射、`org` 参数、gzip 请求体、请求大小限制及 v2 风格错误响应。
- 优化读路径：元数据缓存、LIMIT 下推、字段统计跳过、并行读取和流式 k-way 合并，降低大范围查询的扫描与内存开销。
- 优化写路径：批量写入、UTF-8 直接解析、异步双缓冲 flush、并行分 shard 刷盘，以及 WAL 负载估算优化。
- 改进 segment 合并策略：控制小尾段文件数量，提升 compaction 的合并效率；新增 compaction 写入节流配置。
- 新增每序列 Last Value Cache 与 Distinct Value Cache。`last()` 和当前值查询可命中最新值缓存；当时间范围、删除标记或字段缺失可能影响正确性时会自动回退到正常扫描。

#### 安全与 API

- 支持 `Authorization: Token` 与 `Authorization: Bearer`，并与既有 Basic 认证共存。
- 增加命名 token 的创建、查看和吊销能力；服务端仅持久化 SHA-256 哈希，明文 token 只在创建时返回一次。
- HTTP JSON/text 响应支持 gzip，并正确遵守客户端的 `Accept-Encoding` 协商（包括 `gzip;q=0`）。
- 分离应用与系统日志级别，便于在高吞吐环境中降低框架日志噪声。

#### 管理台

- 管理台新增数据库容量、Retention Policy、Continuous Query、token、缓存统计、shard 诊断、备份/恢复预置及在线维护入口。
- 修复数据库、RP 与 CQ 管理操作此前误用只读接口的问题。管理命令现通过独立的受认证白名单接口执行，只允许管理台公开的数据库、RP 与 CQ 创建/删除操作。
- 查询控制台保持只读；普通 `SELECT` 的翻页改为后端分页，切换页面会以 `LIMIT` / `OFFSET` 重新请求服务端。已经显式包含 `LIMIT` 或 `OFFSET` 的查询保持其原有语义。

#### 稳定性修复

- 修复 Last Value Cache 对 `first()` 的错误使用，确保 `first()` 始终走能返回最早点的正常查询路径。
- 修复最新点不含目标字段时 `last(field)` 可能漏掉较早有效值的问题。
- 修复缓冲区变更、原始降序查询分组与响应 gzip 包装等边界问题。

#### 升级说明

本月更新不要求数据迁移。升级后可直接使用新增 v2/v3 API、token 与管理台能力。

生产环境仍建议启用 TLS 和认证，并通过环境变量提供管理员密码；`Auth.AllowQueryCredentials` 仅用于兼容旧客户端，不建议在公网环境开启。

### InfluxDB 1.x 查询功能对比

以 InfluxDB 1.8 (1.x 最后一版) 官方查询语言为基准，逐项对比 MiniInflux 实现状态（更新于 2026-08-23）。标注：`[done]` 已实现且通过测试、`[partial]` 部分支持、`[missing]` 未实现。

#### SELECT 数据探索

| 功能 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `SELECT field` / `SELECT *` / 多字段 | 支持 | 支持 | `[done]` |
| `mean(field)` 等聚合 | 支持 | 支持 | `[done]` |
| 限定名 `db.rp.measurement` | 支持 | 支持 | `[done]` |
| 子查询 `(SELECT ... ) FROM ...` | 支持 | 支持 | `[done]` |
| `SELECT ... INTO ...` | 支持 | 支持 | `[done]` |
| `SELECT DISTINCT field` / `COUNT(DISTINCT field)` | 支持 | 支持 | `[done]` |
| `WHERE time >/>=/</<=`、相对/绝对时间 | 支持 | 支持 | `[done]` |
| `WHERE tag =/!=/=~/!~` | 支持 | 支持 | `[done]` |
| `WHERE field > N`（数值）/ `field = 's'`（字符串） | 支持 | 支持 | `[done]` |
| `AND` / `OR` / 括号分组 | InfluxQL 不支持 OR | 已扩展支持 OR 与括号 | `[done]` |
| `GROUP BY time/tag/*`、混合分组、`fill(...)` | 支持 | 支持 | `[done]` |
| `ORDER BY time ASC/DESC`、`LIMIT`/`OFFSET`、`SLIMIT`/`SOFFSET` | 支持 | 支持 | `[done]` |

#### 聚合与转换函数

| 类别 | 函数 | 状态 |
|------|------|------|
| 基础聚合 | `COUNT`/`COUNT(*)`/`SUM`/`MEAN`/`MEDIAN`/`MIN`/`MAX`/`FIRST`/`LAST`/`SPREAD`/`STDDEV`/`MODE`/`PERCENTILE` | `[done]` |
| 转换 | `DERIVATIVE`/`NON_NEGATIVE_DERIVATIVE`/`DIFFERENCE`/`NON_NEGATIVE_DIFFERENCE`/`MOVING_AVERAGE`/`CUMULATIVE_SUM`/`ELAPSED`/`INTEGRAL` | `[done]` |
| 排名选择 | `TOP`/`BOTTOM`/`SAMPLE` | `[done]` |
| 高级函数 | `HOLT_WINTERS`/`HOLT_WINTERS_WITH_MONTHLY`/`HISTOGRAM` | `[missing]`（见 [todo-202610.md](todo-202610.md)） |
| 数学函数 | `ABS`/`CEIL`/`FLOOR`/`ROUND`/`SQRT`/`EXP`/`LOG`/`LOG2`/`LN`/`SIN`/`COS`/`TAN`/`ATAN2`（第二参数常量子集） | `[done]` |

#### Schema 与系统 SHOW

| 类别 | 状态 |
|------|------|
| `SHOW DATABASES/MEASUREMENTS/SERIES/TAG KEYS/TAG VALUES/FIELD KEYS` 及 `WITH MEASUREMENT`、`WHERE`、`WITH KEY =~`、`CARDINALITY` 系列 | `[done]` |
| `SHOW RETENTION POLICIES`、`SHOW CONTINUOUS QUERIES` | `[done]` |
| `SHOW QUERIES`、`KILL QUERY`、`SHOW SHARDS`、`SHOW SHARD GROUPS`、`SHOW STATS`、`SHOW STATS FOR`、`SHOW DIAGNOSTICS`、`EXPLAIN`、`EXPLAIN ANALYZE` | `[done]` |
| `SHOW SUBSCRIPTIONS` | `[missing]`（单节点架构，非目标） |

#### 数据库 / RP / CQ 管理

| 类别 | 状态 |
|------|------|
| `CREATE/DROP DATABASE`（含 `IF NOT EXISTS`、`WITH DURATION...`）、`DROP MEASUREMENT`、`DROP SERIES`、`DROP SHARD`、`DELETE` | `[done]` |
| `CREATE/ALTER/DROP RETENTION POLICY` | `[done]` |
| `CREATE/DROP CONTINUOUS QUERY`（含 `RESAMPLE`） | `[done]` |

#### 排除项（既定非目标）

- **用户与权限管理**：`CREATE USER`/`GRANT`/`REVOKE`/`SET PASSWORD`/`DROP USER`/`SHOW USERS`/`SHOW GRANTS` — 单管理员模型，不实现多用户与细粒度授权。
- **Subscription 管理**：`CREATE/DROP SUBSCRIPTION`、`SHOW SUBSCRIPTIONS` — 单节点架构，不实现 Kapacitor/Subscription。

#### 时间语法与 Line Protocol

| 类别 | 状态 |
|------|------|
| 纳秒整数、RFC3339、`now()`、相对偏移、epoch 后缀 | `[done]` |
| Line Protocol：多字段/多标签、`i`/`t/f/T/F/true/false`、String 双引号、`precision` | `[done]` |

#### API 端点

| 端点 | 状态 |
|------|------|
| `GET /ping`、`GET /health`、`POST /write`、`GET/POST /query`、`GET /metrics` | `[done]` |
| `GET /debug/vars` | `[partial]`（已有 `/debug/stats`） |
| `GET /debug/pprof`、`GET /debug/requests` | `[missing]`（见 [todo-202610.md](todo-202610.md)） |

补充：`/query` 支持 `chunked`/`chunk_size`/`epoch`/`debug`，请求体支持 gzip 解压；`/query`、`/metrics`、`/debug/stats`、`/debug/benchmark`、`/admin/api` 响应支持 `Accept-Encoding: gzip`。

#### 统计汇总

| 类别 | done | partial | missing | 完成率 |
|------|------|---------|--------|--------|
| SELECT 语法 | 9 | 0 | 0 | 100% |
| WHERE 子句 | 12 | 0 | 0 | 100% |
| GROUP BY / FILL | 6 | 0 | 0 | 100% |
| 排序分页 | 6 | 0 | 0 | 100% |
| 基础聚合 | 11 | 0 | 0 | 100% |
| 转换函数 | 8 | 0 | 0 | 100% |
| 排名选择 | 3 | 0 | 0 | 100% |
| 高级函数 | 0 | 0 | 3 | 0% |
| 数学函数 | 7 | 0 | 0 | 100% |
| Schema SHOW | 21 | 0 | 0 | 100% |
| 系统 SHOW | 9 | 0 | 1 | 90% |
| 数据库管理 | 8 | 0 | 0 | 100% |
| RP 管理 | 3 | 0 | 0 | 100% |
| CQ 管理 | 3 | 0 | 0 | 100% |
| 用户权限 | 0 | 0 | 8 | 0% |
| Subscription | 0 | 0 | 3 | 0% |
| 时间语法 | 5 | 0 | 0 | 100% |
| Line Protocol | 8 | 0 | 0 | 100% |
| API 端点 | 5 | 1 | 2 | 63% |
| **合计** | **124** | **1** | **17** | **87%** |

说明：用户权限与 Subscription 为单节点/单管理员架构下的既定排除项，不计入实际待办。

#### 已知缺陷（非兼容性缺口）

- ~~`Compactor_OutputRespectsMaxSegmentFileBytes` 回归测试失败~~ `[fixed 2026-08-23]`：`EstimateColumnBytes` 未计入 metadata footer 双份字符串/统计与文件级开销，小列估算偏低约 1.7x；已修正为 128 字节 + 字符串双份计长。
- WAL fsync 为定时组提交（默认 1000ms）：进程崩溃安全，断电可能丢失最近一个 fsync 窗口内已确认写入。该取舍已在 README 配置说明中显式记录。

---

## 2026-09

正确性、性能、存储健壮性与功能增强四个方向，全部完成。

### 简要摘要

- **InfluxQL 正确性**：`AS` 别名解析、`time = X` 相等过滤、`time <>` 显式抛错、`DropDatabase` 锁竞态、大段流式 CRC 校验、`StorageHealth.RecordFailure` 隐性崩溃修复。
- **一致性在线备份**：`FlushAll` 后拷贝，备份期间暂停 compaction 与 shard 过期清理，manifest 最后原子 rename，WAL 不再做 SHA256 校验。
- **性能优化**：Manifest 拆分持久化（索引不写盘）、WAL CRC 移出锁与恢复按 (db,rp) 分组加锁、流式 compaction（k 路归并、峰值内存降为一批 + 一块）、LVC 免克隆、查询热路径（tag 正则缓存、时间单次解析、`fill(linear)` 二分、共享 `BuildValuePairs`、分组不分配字典）。
- **存储健壮性**：磁盘空间写入门控（503 + 自愈）、CRC 缓存与 LVC 上限、损坏段 quarantine（`/debug/stats` 报告）。
- **功能增强**：Token 权限分级（`all`/`read`/`write`）、服务端 CSV 导出、部分过期（compaction 丢弃过期点）。

### 详细更新说明（v0.6.0）

版本 v0.6.0，更新范围：2026-08-29 至 2026-09-01。全量测试 407/407 通过。

#### InfluxQL 正确性

- 修复 `AS` 别名解析：`SELECT mean(value) AS m` 此前整串被当作字段名（静默错误），现正确剥离 `AS ident` 作为输出列名；覆盖 `func(field) AS alias`、`field AS alias`、`count(distinct x) AS alias`。
- 修复 `time = X` 相等过滤与以 `time` 开头标签键的判定：`time = X` 现置为 `min=max=X`（此前被静默忽略）；`timer='a'` 等精确非 `time` 键回退到标签过滤；`time <>/!= X` 显式报错而非静默吞掉。
- 修复 `DropDatabase` 锁竞态：drop 时仅从锁表移除 per-key 锁，不再 `Dispose`（避免他线程已持有引用导致崩溃）。
- 修复大 segment CRC 校验空白：>8MB 流式段在首次打开时按块增量校验。
- 修复存储健康失败处理器隐性崩溃：`StorageHealth.RecordFailure` 的 `Interlocked.Exchange(ref DateTimeOffset?)` 会触发泛型约束异常，任何 WAL 失败都会使恢复本身崩溃，现已改为 long 毫秒存储。

#### 备份一致性

- 新增一致性在线备份：`CreateConsistentBackup` 先 flush + WAL checkpoint + manifest 落盘再拷贝；备份期间暂停 compaction 与 shard 过期清理；数据先拷、`meta/manifest.json` 最后原子落盘，并在其后补齐引用段。
- `wal/` 不再做 SHA256 元数据校验（在线追加文件哈希不稳定，WAL 自带逐记录 CRC）。

#### 性能优化

- Manifest 拆分持久化：`SeriesIndex/TagIndex` 等索引改为 `[JsonIgnore]`，恢复时无条件从段元数据重建；`Save()` 从 O(全部 series) 降为 O(shard+CQ)，显著缩小高基数下 manifest 体积。
- WAL 追加与恢复优化：CRC 计算移出全局锁；大批量恢复按 (db,rp) 分组一次加锁，消除逐点三重加锁。
- Compaction 流式合并：新增按列选择器解码，按 (measurement,tags,field) 分组分批 k 路归并，峰值内存从"全部输入解码"降到"一个批次 + 一个输出块"。批预算原为 200 万点（随点宽与输入文件数放大，宽行/多文件仍可达数百 MB）；2026-10 审计后改为 64 MiB 解码字节预算，峰值直接有界（decoded + filtered/merged 副本约为该值的 2–3 倍）。
- Last Value Cache 免克隆：缓存按引用存点，删除 flush 路径的逐点二次更新。
- 查询热路径：tag 正则按模式编译缓存（有界 256）、行排序时间只解析一次、`fill(linear)` 从 O(n²) 降为按列二分找邻、分组值对构建复用、分组键不再逐点分配字典。

#### 存储健壮性

- 磁盘空间写入门控：空闲空间低于 `Storage.MinFreeDiskBytes` 时写入直接返回 503（5s 缓存探测），并在存储故障解除后自动恢复写路径。
- 缓存上限：CRC 校验缓存有界 4096，Last Value Cache 每库支持 `Storage:LastValueCacheMaxEntries`（默认 1,000,000），超限任意逐出并回退全扫保证正确性。
- 损坏段隔离：结构性损坏（magic/尺寸/CRC/元数据）的 segment 进入进程级 quarantine，不再被查询反复重读，`/debug/stats` 报告 `QuarantinedSegments` 计数。

#### 功能增强

- Token 权限分级：命名 token 可指定权限 `all`（默认）/`read`/`write`；`read` 仅查询与只读管理，`write` 仅写入；写端点要求 `write`，管理变更要求 `all`，不满足返回 403；管理台创建表单增加权限下拉。
- 服务器端 CSV 导出：`/query` 增加 `format=csv`、v3 端点 `format:"csv"`，每 series 一块，RFC4180 转义；与 `chunked=true` 互斥。
- 部分过期：compaction 合并时丢弃早于 `now - duration` 的点（有界 RP），shard 内数据不再陪跑到整 shard 老化。

#### 升级说明

本版本不要求数据迁移。升级后可直接使用新增 token 权限分级、CSV 导出与在线一致性备份能力。

生产环境建议启用 TLS 与认证；`Storage.MinFreeDiskBytes` 在 Production 必须设置为非零值；`Wal.Fsync` 组提交的持久性取舍（断电可能丢失最近一个 fsync 窗口内已确认写入）已在 README 配置说明中显式记录。

#### Benchmark（与 InfluxDB 1.7.9 本地对比，100,000 points / batch 5,000 / concurrency 1，查询 5 次 median）

| Metric | MiniInflux | InfluxDB 1.7.9 | 结论 |
| --- | ---: | ---: | --- |
| write throughput (points/s) | 120,940 | 276,902 | 维持 12–13 万 pts/s 级别 |
| aggregate query median (ms) | 1.09 | 3.34 | MiniInflux `3.06x` 快 |
| raw `LIMIT 1000` median (ms) | 3.48 | 5.36 | MiniInflux `1.54x` 快 |

此轮 InfluxDB 写入读数显著高于其历史区间，单轮对比仅供参考；MiniInflux 侧稳定结论是写入 12–13 万 pts/s、查询持续领先。

### 写入优化方案与执行记录（2026-09-08）

初始检查日期 2026-09-08。瓶颈清单依据优化前源码及 `benchmark.md`，属静态检查，非 CPU profile。实测结果见 `benchmark.md` 的 2026-09-08 章节。

#### 当前结论

写入链路：HTTP 请求体读取 → UTF-8 parser → WriteQueue → 去重/Schema → db/rp 写锁 → WAL → buffer/series/last-value cache → 后台 segment flush。

首要结构性限制是全局单消费者写队列：所有数据库的 engine 写入在一个 worker 中串行执行。单连接关注逐点对象分配、重复遍历和 WAL 文本编码；持续写入关注 flush 清理持锁时间及编码/磁盘是否跟得上。

#### 已确认的问题与影响

| 优先级 | 位置 | 已确认的行为 | 主要影响场景 |
| --- | --- | --- | --- |
| P0 | `scripts/compare-with-influxdb.ps1` | 接受 Concurrency 但循环每次 PostAsync 后立即等待，未实际并发 | 现有结果无法回答并发扩展性 |
| P1 | `WriteQueue.cs` | SingleReader + 单 worker；按 db/rp 分组串行；BatchSize 按请求数 | 多连接、多库共享一个串行入口 |
| P1 | `WriteQueue.cs` / `WritePointsAsync` | 请求完整读取解析后才入队；无排队点数/字节预算 | 高并发可积压大量 Point 对象 |
| P1 | `TsdbEngine.AddWrittenPoints` / `LastValueCache.Update` | 每点调用缓存更新，重复拼接 db/rp key、查字典、同时间戳复制 Fields | 低基数大批次重复更新 |
| P1 | `Program.ReadRequestBodyBytesAsync` | 每请求分配 64 KiB 数组及 MemoryStream；容量≠长度时仍 ToArray | 小请求固定分配、大请求扩容复制 |
| P1 | `TsdbEngine.TryScheduleAsyncFlush/FlushSnapshotAsync` | 启动时持写锁复制全 buffer；完成时持写锁扫描压缩整个 list 并重建索引 | 持续写入周期性 p99 延迟 |
| P2 | `LineProtocolParser.cs` | tags 仍复制 Dictionary；每点构造 Fields；复杂行回退 UTF-16 | 标签/字段多时分配成本 |
| P2 | `TsdbEngine.cs` | 去重/Schema/cardinality/buffer/index/缓存分别遍历；非严格递增进入哈希去重 | 多 series 同时间戳走重路径 |
| P2 | `Wal.Append` | 仍把 Point 编回文本，为所有点生成相同 WalPosition 列表 | 宽行和大批次编码分配 |
| P2 | `SegmentWriter.cs` | 列构造、压缩、整段 MemoryStream；乱序列用 SortedDictionary | 长时间写入、乱序补数、宽字段 |

#### 实施顺序

- **阶段 0（已完成）**：建立可归因的基线——修正并发基准、增加低开销批次计时、采集 CPU/分配/GC/队列/flush 指标、固定 Release 构建与 5 次 median。
- **阶段 1（已完成）**：降低每批固定成本——HTTP body 返回 backing buffer、批量 LVC 更新、合并受 points/bytes 约束、WAL 单批位置接口、批内 series/schema 摘要复用。
- **阶段 2（待办，见 [todo-202610.md](todo-202610.md)）**：分离 active buffer 与 immutable flushing buffer，锁内交换引用、锁外编码，查询同时读两代并维持 LWW；每代独立索引/replay floor；flush 成功后整体退休。
- **阶段 3（待办）**：按数据库路由到固定数量 worker；修正 `_bufferedPointCount/_bufferedByteCount` 原子性；审计 schema/manifest/DDL/WAL checkpoint/跨库 flush。
- **阶段 4（待办，仅在 CPU/GC 仍占主导时）**：tags/fields 字典改为批内只读 series 描述与紧凑字段数组，评估二进制 WAL。

#### 持久化与正确性约束

默认 `Wal.Fsync=true`、`FsyncIntervalMs=1000`：Append 返回不等待 fsync，定时器与 rotation/dispose 执行 Flush(true)。成功响应不是逐请求掉电持久化保证。`FsyncIntervalMs=0` 不会自动变成每请求同步刷盘。

比较优化前后必须维持同一持久化策略；关闭 fsync 不计为等价优化。

#### 第一批执行记录（已完成）

- 修正并发基准：固定异步 worker、payload UTF-8 编码移出计时、记录 p50/p95/p99 与写后计数验证、预热独立数据库后删除。
- 基准增加 `-MiniOnly` 与 `-MiniBinaryPath`，独立运行目录、自动保存 result.json、检查端口冲突。
- HTTP body 返回 backing buffer 与实际长度，消除最终复制；冒烟覆盖 gzip、chunked、v1/v2、空请求、无效行、解压超限。
- engine 批量 LVC 更新：低基数先聚合、达 64 series 转直接更新；统一复用 db/rp 容器，淘汰不构造全量 Keys。
- 全量 417 项通过。本批未实施：阶段计时埋点、在途预算、队列点数/字节预算、WAL 批位置接口、flush 双缓冲、多 worker、二进制 WAL。

#### 第二批执行记录（已完成）

- 进程级分阶段计时（`MiniInflux__WriteDiagnostics=true`），`/debug/stats` 输出累计毫秒和次数；覆盖写锁、WAL、buffer、flush snapshot/encode/cleanup、列构造、时间戳/字段编码、文件写出/CRC/fsync。
- 列构造优化：普通乱序列按原始索引排序原地重排；128 点采样发现较多重复时间戳时哈希去重后排序唯一时间戳。浮点 adaptive 编码复用 legacy 原始字节。
- 三种段文件（顺序、批次乱序、大量重复点）输出 SHA-256 一致；全量 420 项通过。
- 百万点、4 并发、各 5 轮：含追加 flush 吞吐 median 207,069 → 232,879 points/s（+12.5%），追加 flush 915 → 425 ms；ACK 吞吐 257,361 → 247,018 points/s（-4.0%）。

#### 第三批：WAL 内部优化（已完成）

- 完成 WalEncode/WalCrc/WalLockWait/WalFileWrite/WalRotate/WalFsync 诊断，确认文本编码是 WAL 主要成本。
- 分隔符直写字节、布尔 UTF-8 常量、池化缓冲；引擎使用 AppendBatch 单批共享位置，原 Append 接口保留。
- 保持文件格式、CRC、轮转与 fsync 行为；全量 422 项通过。
- 百万点、4 并发、各 5 轮：编码累计耗时中位数下降 30.5%，ACK 吞吐提升 15.0%，含追加 flush 吞吐提升 7.2%；请求 p99 增加 7.4%，追加 flush 时间增加 31.8%（不视为尾延迟改善）。

### 持续写入压测与一致性修复（2026-09-09/10）

#### 测量范围

新增压测与汇总脚本，未修改引擎/WAL/flush。Release 构建、独立临时目录、本机 HTTP、每请求 5,000 点、16 series、一个整数 field、唯一时间戳。主测试 300 秒 4 并发；补充 120 秒 16 并发。默认 WAL fsync=true/1000ms、flush 阈值 50,000 点、buffer 上限 1,000,000。

#### 主测试结果与判断

- 300 秒成功 150,960,000 点，count 相同，失败 0；平均 503,024 points/s。
- p50=20.51 ms、p95=128.99 ms、p99=215.30 ms；30–60s 吞吐 586,167、270–300s 412,333（随时间下降，单轮不足以归因）。
- 私有内存峰值 5,702.6 MiB、工作集 5,527.5 MiB，随后回落。
- buffer 峰值 545,000 点，队列峰值 4 请求；结束 9 次 compaction run，backlog=1。
- 累计阶段：FlushEncode=205.15 s、WriteLocked=168.89 s、WalAppend=95.66 s、SegmentColumns=87.34 s、BufferAppend=61.21 s、WalEncode=54.28 s、FlushCleanup=25.29 s、WriteLockWait=18.13 s（嵌套并行，不能相加）。

#### 阶段 0：compaction 读一致性修复（已完成）

16 并发 120 秒首次 count 少 760,000 点（1.38%），重启后连续三次 count 一致，判断为运行中查询可见性问题。

根因：`Compactor.FinalizeCompaction` 替换 manifest 后立即删除旧段，读侧遇 FileNotFoundException 跳过；元数据索引在 CompactAll 完成后才统一失效。

实施：引擎读取及 SELECT/流式查询持有可跨线程释放的读取凭据；合并编码仍在读侧并行，发布 manifest/清缓存/删旧段前等待现有读取完成；缓存清理移入每次发布内部。新增流式读取暂停下的发布/嵌套 COUNT/缓存冷热测试，覆盖跨线程释放、重复释放与取消。全量 425 项通过。

代价：长查询或未释放枚举会延后合并发布；本机制仅协调 compaction 发布，不提供完整读写事务快照。

同时修正 README 中"进程崩溃不会丢已确认写入"的过强保证（当前 ACK 不等待 fsync）。持续基准增加连续三次 COUNT 校验。

复测：三轮 16 并发 120 秒均零失败、连续三次 COUNT 一致（第一轮 68,675,000；第二轮 70,780,000，有 8 次启动期 429；第三轮 70,720,000，未再拒绝）。

#### 阶段 1–5（待办，见 [todo-202610.md](todo-202610.md)）

- **阶段 1**：补齐内存、队列和后台合并的观测（active buffer/flush snapshot/排队请求点数与字节、compaction 输入输出字节、.NET 分配速率/GC）。
- **阶段 2**：限制 compaction 解码与输出缓冲峰值（按字节约束、大 group 分块解码、及时写出释放、有序输入直接拼接）。
- **阶段 3**：减少 flush 清理持锁扫描与索引重建（按冻结批次维护数据与索引生命周期，整体退休）。
- **阶段 4**：请求队列与合批增加点数/字节预算（限制排队请求/点数/字节、解析前在途预算、超载返回与重试）。
- **阶段 5**：再评估 WAL 编码与跨数据库并行（重复 measurement/tags 编码复用、多 db/rp 确认队列瓶颈后再分区 worker）。

---

## 2026-10

- **查询正确性加固**：`SELECT … INTO` 同步写入（消除 sync-over-async）、chunked 查询解析错误改为单错误块返回（不再破坏流式响应）。
- **认证/存储健壮性**：`tokens.json` 损坏时重命名为 `.corrupt-*` 保留现场并告警（不再静默清空）；token 持久化改用 `FileStream + Flush(true) + Move` 实现 fsync；`TokenStore.Validate` 移除伪常时遍历，直接字典查找。
- **错误可见性**：`DropDatabase` 目录删除失败记入 `StorageHealth`；`Manifest` 损坏重命名失败记录日志；请求体读取不再 dispose 服务器持有的 `request.Body`。

### 全库审计修复（2026-10-25）

对引擎与应用层做了一次全量静态审计（构建 0 警告 0 错误，修复前后各跑全量测试）。修复项：

- **C1 查询正确性（P0）**：`LIMIT` 下推与逐点字段过滤的组合会少返行——`readLimit` 按未过滤点数截断后，`ApplyFieldFilters` 才收缩结果。现在存在字段过滤时不下推 limit。补 3 个回归测试（含 limit+offset、匹配点超出原始预算的场景）。
- **C2 存储健壮性（P0）**：瞬时 `IOException`（Windows 共享违例、磁盘抖动）会把健康段写入进程级 quarantine，导致后续查询静默丢数据。现在仅结构校验失败（`InvalidDataException`/`EndOfStreamException`，含新增的长度前缀校验）才隔离；瞬时错误当次跳过并记入 `StorageHealth`，下次读取重试。恢复路径的 `SegmentsCorrupted` 也只统计真正被隔离的文件。补 2 个回归测试（持锁读取不隔离、截断文件仍隔离）。
- **C3 查询健壮性（P1）**：子查询递归无深度上限，深层嵌套会触发不可捕获的 `StackOverflowException`。执行与权限分类均加 16 层深度上限，超限返回明确错误。
- **C4 配置正确性（P1，修正审计初判）**：审计初判"两条路径 limit 语义不一致"不成立——所有路径都是先截断后检查。真实缺陷是 `MaxResponseRows=0`（约定为"不限制"）在缓冲路径塌缩成 `Take(0)` 静默返回空结果。统一为 `EffectiveRowLimit(maxResponseRows, queryLimit)`：`<=0` 表示无限、显式 LIMIT 优先，null 时不截断；覆盖全部 11 处行限制计算点。截断语义（既有测试固定）保持不变。
- **C5 查询健壮性（P1）**：`fill()` 先在内存物化全部桶行、再撞响应行上限（小 step 大区间先 OOM 后报错）。现在按"桶数 × series 数"预算前置拒绝。
- **查询热路径（P1）**：`EstimateGroupingStateBytes` 不再对全量点集重复建分组字典，改为与 `EstimatePointsBytes` 相同的抽样外推；`BuildGroupByTagKey` 从 LINQ + `string.Join` + 双 `Replace` 改为单趟 `StringBuilder` 构建，无需转义的取值（绝大多数）零中间分配。
- **compaction 内存（P2）**：合并批预算从 200 万点改为 64 MiB 解码字节，峰值直接有界（原预算随点宽与输入文件数放大）。
- **文档修订**：`MinSegmentFileBytes`/`MinSegmentFillRatio` 语义、segment 格式版本（v4）、compaction 峰值描述、quarantine 隔离条件、CLI 与 HTTP 备份路径差异，共 6 处与实现不符的描述已更正。

全量测试 541 → 554 项通过（新增 `P0AuditFixTests` 5 项、`P1AuditFixTests` 8 项、修正既有预期 0 项）。
