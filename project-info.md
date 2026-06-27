### 项目说明
***

# MiniInflux.Net10 项目状态

更新时间：2026-06-27

MiniInflux.Net10 是一个基于 .NET 10 / ASP.NET Core Minimal API 的轻量级 InfluxDB 1.x 兼容子集时序数据库。当前项目已经具备单节点时序数据库的完整主链路：HTTP 接口、Line Protocol 写入、写入队列、WAL、segment 存储、schema、manifest/index、shard/retention、compaction、InfluxQL 子集、查询保护、权限、诊断指标、管理 CLI 和回归测试。

当前验证状态：

```text
dotnet test .\MiniInflux.Tests\MiniInflux.Tests.csproj -nologo --no-restore
结果：命令退出码 0
测试用例：源码中共 168 个 [Fact]/[Theory] 测试标记
备注：本次环境没有打印详细 passed 汇总，因此以退出码和测试标记数量记录。
```

***

# 一、项目完整性评估

## 1. 结论

当前项目不再只是 Demo，已经是一个完整可运行的单节点 InfluxDB 1.x 兼容子集。核心数据路径完整，启动恢复、写入持久化、查询执行、删除/压缩、诊断和离线运维都有对应实现。

整体完整性判断：

```text
主写入链路：完整
主查询链路：完整
持久化恢复：完整
单节点运维：基本完整
InfluxDB 1.x 兼容性：子集可用，但不是完整替代
生产级长期运行：具备基础保护，还需要继续打磨边界语义、压测和运维安全性
```

## 2. 当前最需要注意的文档差异

`README.md` 是当前最接近源码状态的说明；旧版 `project-info.md` 中仍保留了一些已经完成的风险项，例如：

```text
WAL 仍是文件粒度 checkpoint
完整多层 LSM compaction 还没有
查询结果未支持 chunked response
regex tag predicate 仍只能读点后过滤
auth 仍是全局用户校验
backup / restore 缺少元信息校验
字节级内存限制未接入
```

这些能力现在都已经至少完成第一版，应从“当前最大风险”中移出。

***

# 二、已完成功能

## 1. HTTP API

已实现 InfluxDB 1.x 风格基础接口：

```text
GET  /ping
GET  /health
GET  /debug/stats
GET  /debug/benchmark
GET  /metrics
POST /write?db=xxx&rp=xxx&precision=...
GET  /query?db=xxx&q=...
POST /query
```

写入接口支持 gzip body。写入队列满、请求体过大、buffer 超限或 series cardinality 超限时会返回受控错误。

## 2. Line Protocol 写入

已支持：

```text
measurement
tag set
field set
timestamp
多行批量写入
precision: ns, u/us, ms, s, m, h
首次写入不存在的 db 时自动创建数据库和默认 RP autogen
```

同一批次内 duplicate point 已按 InfluxDB 语义处理：

```text
same measurement + same tag set + same timestamp
=> field set merge
=> 同名 field 后写覆盖
```

## 3. 写入队列与背压

已实现 `WriteQueue`：

```text
HTTP write -> bounded channel -> batch worker -> TsdbEngine
```

关键配置：

```text
Write.QueueCapacity
Write.BatchSize
Write.MaxRequestBodyBytes
Storage.MaxBufferPoints
Storage.MaxBufferBytes
Storage.MaxSeriesPerDatabase
Storage.MaxFieldsPerMeasurement
```

## 4. WAL 可靠性

已实现：

```text
WAL append
WAL replay/recovery
WAL file rotation
record offset checkpoint
WAL record CRC32
WAL fsync interval / group commit
checkpoint 原子写入
```

启动时会先应用 pending restore，再调用 `engine.Recover()` 从 WAL 和已有 segment 重建状态。

## 5. Segment 存储与压缩

已实现：

```text
内存 buffer
阈值 flush
定时 flush
segment 列式存储
segment metadata
segment CRC 校验
tmp + rename 原子写入
segment reader
进程退出 flush
```

Segment v3 已支持：

```text
timestamp delta-of-delta / Gorilla
integer delta + ZigZag + Varint
double legacy XOR / Gorilla
bool bit-pack
string dictionary
Brotli block compression
adaptive float codec strategy
```

## 6. Schema 管理

已实现 `SchemaRegistry`：

```text
measurement -> fieldKey -> fieldType
field type conflict 校验
MaxFieldsPerMeasurement 限制
schema 持久化
```

写入字段类型冲突会返回 400。

## 7. Manifest / 索引

已实现 `Manifest`，包含：

```text
database metadata
retention policy metadata
shard group metadata
segment manifest
measurement index
series index
tag inverted index
continuous query metadata
```

查询已具备：

```text
time range pushdown
measurement metadata skip
projection pushdown / column pruning
field range predicate skip
tag-series inverted index pushdown
regex tag candidate prefilter
segment 内 measurement/tag/time 列级解压前跳过
```

## 8. Shard 与 Retention Policy

已实现：

```text
time-based shard routing
shard directory management
CREATE RETENTION POLICY
ALTER RETENTION POLICY
DROP RETENTION POLICY
SHOW RETENTION POLICIES
retention expiry background cleanup
```

## 9. Compaction

已实现 `Compactor` 第一版：

```text
L0 -> L1 -> L2
按文件数量触发
按累计大小触发
按 shard 合并 segment
overlap-aware compaction
tombstone 应用
重复点 last-write-wins
重写合并后的 segment
单次 CompactAll 多轮排空 backlog
staged source segment 切换
查询时按层级/时间顺序读取 segment，优先保留更新值
后台定时 compaction
compaction 任务队列和状态指标
```

当前已经具备短期正式使用所需的 compaction 主链路；后续更值得继续投入的是全量索引重建/校验工具、限速策略和长时间 soak test。

## 10. Tombstone / 删除

已实现：

```text
DROP DATABASE
DROP MEASUREMENT
DROP SERIES 第一版
DROP SHARD 第一版
DELETE FROM measurement WHERE time ...
tombstone 持久化
compaction 时清理被删除数据
```

当前 `DELETE` / `DROP SERIES` 已支持 field predicate 第一版；更深的兼容性工作主要在复杂谓词组合和 InfluxDB 1.x 行为对照。

## 11. InfluxQL 子集

已实现：

```sql
CREATE DATABASE metrics;
SHOW DATABASES;
SHOW MEASUREMENTS;
SHOW FIELD KEYS;
SHOW TAG KEYS FROM cpu;
SHOW TAG VALUES FROM cpu WITH KEY = host;
SHOW SERIES;
SHOW SERIES CARDINALITY;
SHOW MEASUREMENT CARDINALITY;
SHOW TAG VALUES CARDINALITY;
CREATE RETENTION POLICY rp ON db DURATION 7d REPLICATION 1 DEFAULT;
ALTER RETENTION POLICY rp ON db DURATION 30d DEFAULT;
DROP RETENTION POLICY rp ON db;
SHOW RETENTION POLICIES ON db;
CREATE CONTINUOUS QUERY ... BEGIN SELECT ... INTO ... END;
SHOW CONTINUOUS QUERIES;
DROP CONTINUOUS QUERY ... ON db;
DROP DATABASE metrics;
DROP MEASUREMENT cpu;
DROP SERIES FROM cpu WHERE host = 's1';
DROP SHARD 1;
DELETE FROM cpu WHERE time < ...;
SELECT * FROM cpu LIMIT 10;
SELECT value,temp FROM cpu WHERE time >= ... AND time <= ...;
SELECT mean(value),max(temp) FROM cpu GROUP BY time(1m);
SELECT mean("max") FROM (SELECT max(value) FROM cpu GROUP BY time(10s)) GROUP BY time(1m);
SELECT mean(value) INTO cpu_1m FROM cpu GROUP BY time(1m),host;
```

## 12. WHERE / GROUP BY / fill

WHERE 已支持：

```sql
WHERE time >= ... AND time <= ...
WHERE host = 'server01'
WHERE region != 'cn'
WHERE host =~ /server.*/
WHERE host !~ /test.*/
WHERE value > 10
WHERE temp <= 40
```

GROUP BY / fill 已支持：

```sql
GROUP BY time(1m)
GROUP BY host
GROUP BY time(1m), host
fill(none)
fill(null)
fill(0)
fill(previous)
fill(linear)
```

同时已支持 `ORDER BY time DESC`、`LIMIT`、`OFFSET`、`SLIMIT`、`SOFFSET` 的基础组合。

## 13. 聚合、序列、采样函数

已实现：

```text
count
sum
mean
min
max
first
last
spread
stddev
median
percentile(field, n)
difference
derivative
non_negative_derivative
moving_average
cumulative_sum
integral
elapsed
top
bottom
sample
```

其中 `derivative`、`difference`、`moving_average`、`integral`、`elapsed` 等高级函数仍是兼容子集实现，不追求完整 InfluxQL 所有边角语义。

## 14. 查询保护与观测

已实现：

```text
chunked query response
MaxQueryDurationMs
MaxQueryPoints
MaxResponseRows
MaxQueryMemoryBytes
query count
query error count
query timeout count
query rows returned
query scanned points
query duration buckets
query memory estimate metrics
```

## 15. 权限与认证

已实现：

```text
HTTP Basic 认证
query 参数认证
配置文件单一超级管理员
Auth.Enabled 认证总开关，Http.AuthEnabled 作为旧配置兼容回退
Auth.Username / Auth.Password 唯一凭据
登录失败审计日志
按客户端地址的失败限流与临时锁定
query / write / admin / diagnostics 路由统一认证
管理台登录与会话状态
```

系统不再维护数据目录用户仓库，不支持运行时用户 CRUD、改密或 database / RP / measurement 级授权。旧 `meta/auth.json` 不再读取。

## 16. Continuous Query

已实现：

```text
CREATE CONTINUOUS QUERY
SHOW CONTINUOUS QUERIES
DROP CONTINUOUS QUERY
后台定时调度
GROUP BY time(...) bucket 调度
SELECT ... INTO ... 自动回写
带已有 WHERE 的 bucket 时间窗口注入
CQ body 使用子查询第一版
执行进度持久化
RESAMPLE EVERY
RESAMPLE FOR 第一版
InitialBackfillDuration
RecomputeRecentBuckets
按 CQ 维度导出运行指标
```

当前限制是 CQ body 必须是 `SELECT ... INTO ...`，并且必须包含 `GROUP BY time(...)`。

## 17. 诊断、日志与配置

已实现：

```text
GET /health
GET /debug/stats
GET /debug/benchmark
GET /metrics
Prometheus text format
应用日志
访问日志
按状态码筛选访问日志
write tracing
query logging
TLS 配置第一版
```

## 18. 管理 CLI

已实现：

```text
benchmark
inspect wal
inspect segment
inspect manifest
inspect schema
inspect tombstone
validate data-dir
repair
compact
backup
backup verify
restore
管理 CLI 稳定 JSON schema（`--format json`）
repair / compact / restore `--dry-run`
```

`backup` / `restore` 已支持备份元信息、文件长度、SHA256 校验和 pending restore 边界；`backup verify` 可离线校验备份完整性；`inspect manifest` / `inspect schema` / `inspect tombstone` / `validate data-dir` 可用于离线查看 manifest、schema、删除标记以及检查 manifest / segment / schema / tombstone 的基本一致性。除 `benchmark` 外，其余管理 CLI 也已支持稳定的 `--format json` 机器可读输出，当前顶层结果统一携带 `SchemaVersion=1`。

## 19. AOT 兼容设计

已实现：

```text
.NET 10
PublishAot=true
PublishTrimmed=true
JsonSerializerContext
Minimal API Slim Builder
不依赖动态代理
```

## 20. 测试体系

已有 xUnit 测试工程，覆盖：

```text
LineProtocolParser
InfluxQlParser
Compression codecs
CRC32
WAL replay / checkpoint / rotation
Segment read/write / atomic write / corruption detection
SchemaRegistry
Index / Manifest
Duplicate point
Aggregation
P0/P1/P2 todo 回归项
Configuration / logging
Management CLI
Continuous Query
配置文件单一超级管理员认证
Backup / Restore
恢复 / 备份 / compaction 故障注入
```

当前源码中共有 168 个 `[Fact]` / `[Theory]` 测试标记。

***

# 三、缺少的比较紧急功能

当前没有发现“阻断项目完整运行”的 P0 主链路缺口。比较紧急的工作主要是生产化和兼容性收敛：

```text
1. chunked raw SELECT 已完成边扫描边输出第一版；GROUP BY / 函数 / 子查询 / SELECT INTO 回写阶段的峰值估算已补强，但复杂查询仍需继续降低物化内存峰值。
2. SELECT INTO / Subquery / Continuous Query 已补到短期可用：quoted db/rp/measurement、subquery string field/tag、CQ 子查询 + WHERE 已有回归；剩余主要是更复杂组合测试。
3. Auth 已补齐单一超级管理员模型、失败审计和基础限流；后续主要是外部 secret provider 和凭据轮换。
4. Compaction 已补到短期可上线：overlap-aware、多轮排空 backlog、staged 切换、读序修正，以及并发查询/并发删除/混合删改 soak 回归已具备；后续重点转为更长期压测和限速策略。
5. Backup / restore 已有校验、pending restore 和故障注入回归测试，但还不是在线一致性快照协议。
6. 内存估算是近似模型，仍需按真实 workload 校准。
7. Benchmark 已有第一版，缺少远端压测、soak test、多 measurement / 高 tag 基数 workload。
```

***

# 四、阶段判断

MiniInflux.Net10 当前已经具备“小型单节点时序数据库”的骨架和可用功能面。下一步最值得优先投入的是：

```text
正确性边界
大查询内存保护
长期运行压测
运维恢复语义
InfluxDB 1.x 兼容性测试矩阵
```

具体拆解、优先级和阶段路线见 [todo-202606.md](D:\workingfold\MiniInflux\todo-202606.md)。
