### 项目说明
***

# MiniInflux.Net10 项目状态

MiniInflux.Net10 是一个基于 .NET 10 / ASP.NET Core Minimal API 的轻量级 InfluxDB 1.x 兼容子集时序数据库。当前项目已经不只是最初的 Demo：核心写入、WAL、segment 存储、轻量索引、基础查询、聚合、删除标记、诊断指标和单元测试都已经具备。

当前验证状态：

```text
dotnet test .\MiniInflux.Tests\MiniInflux.Tests.csproj -nologo
结果：81 passed, 0 failed, 0 skipped
备注：高/中优先级第一轮修复后已无 xUnit1031 analyzer 警告输出。
```

***

# 一、已完成功能

## 1. HTTP API

已实现 InfluxDB 1.x 风格基础接口：

```text
GET  /ping
GET  /health
GET  /debug/stats
GET  /metrics
POST /write?db=xxx&rp=xxx&precision=...
GET  /query?db=xxx&q=...
POST /query
```

写入接口支持 gzip body；写入队列满或 series cardinality 超限时会返回 429。

## 2. Line Protocol 写入

已支持：

```text
measurement
tag set
field set
timestamp
多行批量写入
precision: ns, u/us, ms, s, m, h
```

同一批次内的 duplicate point 已按 InfluxDB 语义处理：

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

配置项包括：

```json
{
  "Write": {
    "QueueCapacity": 100000,
    "BatchSize": 10000
  }
}
```

## 4. WAL 可靠性基础

已实现：

```text
WAL append
WAL replay/recovery
WAL file rotation
WAL checkpoint
WAL record CRC32
WAL fsync interval
```

启动时 `Program.cs` 会调用 `engine.Recover()`，从 WAL 和已有 segment 重建状态。

## 5. Segment 存储

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

## 6. 压缩编码

已实现：

```text
timestamp delta-of-delta
integer delta + ZigZag + Varint
double XOR
bool bit-pack
string dictionary
Brotli block compression
```

## 7. Schema 管理

已实现 `SchemaRegistry`：

```text
measurement -> fieldKey -> fieldType
field type conflict 校验
MaxFieldsPerMeasurement 限制
schema 持久化
```

写入字段类型冲突会返回 400。

## 8. Manifest / 轻量索引

已实现 `Manifest`，包含：

```text
database metadata
retention policy metadata
shard group metadata
segment manifest
measurement index
series index
tag inverted index
```

查询已具备基础的：

```text
time range pushdown
measurement metadata skip
projection pushdown / column pruning
```

## 9. Shard 与 Retention Policy

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

## 10. Compaction

已实现 `Compactor`：

```text
按 shard 合并 segment
处理 tombstone
重复点 last-write-wins
重写合并后的 segment
后台定时 compaction
```

目前属于轻量 L0 合并能力，还不是完整多层 LSM compaction。

## 11. Tombstone / 删除

已实现：

```text
DROP DATABASE
DROP MEASUREMENT
DELETE FROM measurement WHERE time ...
tombstone 持久化
compaction 时清理被删除数据
```

## 12. InfluxQL 子集

已实现：

```sql
CREATE DATABASE metrics;
SHOW DATABASES;
SHOW MEASUREMENTS;
SHOW FIELD KEYS;
SHOW TAG KEYS FROM cpu;
SHOW TAG VALUES FROM cpu WITH KEY = host;
CREATE RETENTION POLICY rp ON db DURATION 7d REPLICATION 1 DEFAULT;
ALTER RETENTION POLICY rp ON db DURATION 30d DEFAULT;
DROP RETENTION POLICY rp ON db;
SHOW RETENTION POLICIES ON db;
DROP DATABASE metrics;
DROP MEASUREMENT cpu;
DELETE FROM cpu WHERE time < ...;
SELECT * FROM cpu LIMIT 10;
SELECT value,temp FROM cpu WHERE time >= ... AND time <= ...;
SELECT mean(value),max(temp) FROM cpu GROUP BY time(1m);
```

## 13. WHERE 过滤

已支持：

```sql
WHERE time >= ... AND time <= ...
WHERE host = 'server01'
WHERE region != 'cn'
WHERE host =~ /server.*/
WHERE host !~ /test.*/
WHERE value > 10
WHERE temp <= 40
```

`=` / `!=` tag predicate 已能通过 tag-series inverted index 先缩小候选 series；regex predicate 仍在读取点之后过滤。

## 14. GROUP BY / fill

已支持：

```sql
GROUP BY time(1m)
GROUP BY host
GROUP BY time(1m), host
fill(null)
fill(0)
fill(previous)
fill(linear)
```

`fill(linear)` 已基于前后有效 bucket 对数值聚合列做线性插值。

## 15. 聚合函数

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
derivative
non_negative_derivative
difference
cumulative_sum
moving_average
```

其中 `derivative`、`difference`、`moving_average` 等高级函数目前是简化实现，更接近按 group 内值计算的基础版本，不是完整 InfluxQL 语义。

## 16. 诊断与指标

已实现：

```text
GET /health
GET /debug/stats
GET /metrics
```

`/metrics` 输出 Prometheus text format。

## 17. 并发控制与限制

已实现：

```text
per db/rp ReaderWriterLockSlim
global lock 管理 buffer/series state
MaxSeriesPerDatabase
MaxFieldsPerMeasurement
bounded write queue
```

## 18. AOT 兼容设计

已实现：

```text
.NET 10
PublishAot=true
PublishTrimmed=true
JsonSerializerContext
Minimal API Slim Builder
不依赖动态代理
```

## 19. 测试体系

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
```

当前测试数量：81。

***

# 二、待办文档

未完成功能、阶段路线和优先级排序已拆分到 [todo-202606.md](D:\workingfold\MiniInflux\todo-202606.md)。

该待办文档只保留仍未完成事项，并按：

```text
P0 高优先级
P1 中优先级
P2 低优先级
路线建议
```

进行整理，便于后续持续推进。

***

# 三、当前最大风险

当前版本最大风险集中在：

```text
1. checkpoint 仍是 WAL 文件粒度，不是 record offset 粒度
2. 完整多层 LSM compaction 还没有
3. block-level aggregation pushdown 还没有接入执行计划
4. 查询结果仍未支持 chunked response
5. regex tag predicate 仍主要在读取点之后过滤
6. auth 仍是全局用户校验，缺少细粒度权限 DDL
7. backup / restore 还是基础目录复制，缺少在线一致性快照协议
8. 字节级内存限制仍需基于实际对象大小继续增强
```

***

# 四、结论

MiniInflux.Net10 当前已经具备“小型单节点时序数据库”的骨架：写入、WAL、segment、schema、manifest、shard、retention、compaction、查询、指标和测试都已经成形。

下一步最值得优先投入的是正确性收敛和查询保护，而不是继续扩新功能。具体拆解、优先级和阶段路线见 [todo-202606.md](D:\workingfold\MiniInflux\todo-202606.md)。
