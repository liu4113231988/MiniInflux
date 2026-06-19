### 项目说明
***

# 一、当前已实现能力概览

当前版本已经包含：

## 1. HTTP API

已实现：

```text
GET  /ping
POST /write?db=xxx&precision=...
GET  /query?db=xxx&q=...
POST /query
```

支持 InfluxDB 1.x 风格的基础写入和查询。

***

## 2. 写入协议

已实现：

* Line Protocol 解析
* measurement
* tag set
* field set
* timestamp
* 多行批量写入
* precision 支持：
  * `ns`
  * `u` / `us`
  * `ms`
  * `s`
  * `m`
  * `h`

***

## 3. 查询能力

已实现 InfluxQL 子集：

```sql
CREATE DATABASE metrics;
SHOW DATABASES;
SHOW MEASUREMENTS;
SHOW FIELD KEYS;
SHOW TAG KEYS FROM cpu;
SHOW TAG VALUES FROM cpu WITH KEY = host;
SELECT * FROM cpu LIMIT 10;
SELECT value,temp FROM cpu WHERE time >= ... AND time <= ...;
SELECT mean(value),max(temp) FROM cpu GROUP BY time(1m);
```

支持聚合函数：

```text
count
sum
mean
min
max
first
last
```

***

## 4. 存储能力

已实现：

* WAL 追加写
* 内存 buffer
* 达到阈值后 flush 成 segment
* segment 列式存储
* 简单 segment reader
* 进程退出时 flush

***

## 5. 压缩能力

已实现：

* timestamp delta-of-delta
* integer delta + ZigZag + Varint
* double XOR
* bool bit-pack
* string dictionary
* Brotli block compression

***

## 6. AOT 兼容设计

已实现：

* `.NET 10`
* `PublishAot=true`
* `PublishTrimmed=true`
* `JsonSerializerContext`
* 不依赖运行时反射 JSON
* 不使用动态代理
* Minimal API Slim Builder

***

# 二、当前还缺少的核心功能

下面是比较系统的缺口列表。

***

# 1. WAL Recovery 还不完整

## 当前状态

当前版本有 WAL 追加写：

```text
data/wal/current.wal
```

但**启动时没有从 WAL 恢复未 flush 的数据**。

也就是说：

* 写入后如果还没有 flush 到 segment；
* 进程异常退出；
* 下次启动时，WAL 中的数据不会自动 replay。

## 需要补充

需要增加：

```text
WalReader
WalCheckpoint
WalRotation
WalReplay
```

推荐设计：

```text
write -> append wal -> memory buffer
flush segment success -> checkpoint wal offset
startup -> read wal after checkpoint -> replay to buffer or segment
```

## 优先级

**P0，必须补。**

这是生产数据库可靠性的底线。

***

# 2. Compaction 还没有

## 当前状态

当前版本每次 flush 都生成新的 `.seg` 文件：

```text
data/db/metrics/autogen/xxx.seg
```

但是没有后台合并。

长期运行后会产生大量小 segment：

```text
segment-1.seg
segment-2.seg
segment-3.seg
...
segment-100000.seg
```

查询时需要扫描大量文件，性能会下降。

## 需要补充

需要增加 LSM 风格 compaction：

```text
Level 0: 小 segment
Level 1: 合并后的中 segment
Level 2: 大 segment
```

建议规则：

```text
L0 超过 N 个文件 -> 合并为 L1
L1 超过 N 个文件 -> 合并为 L2
```

同时处理：

* 同 series 同 field 排序合并
* 重复点覆盖策略
* tombstone 删除标记
* segment 元数据重写
* 压缩重编码

## 优先级

**P0 / P1。**

如果只是 Demo，可以先不做；如果持续写入，必须做。

***

# 3. 持久化索引还没有

## 当前状态

现在的查询主要靠扫描：

```text
ReadAllPoints()
```

也就是：

* 扫 buffer
* 扫所有 segment
* 反解 columns
* 重组 points

这个方式简单，但数据量大后会慢。

## 缺少的索引

需要增加：

### measurement index

```text
measurement -> segment files
```

### series index

```text
seriesId -> measurement + tags
```

### tag inverted index

```text
tagKey=host, tagValue=s1 -> seriesId list
```

### field index

```text
measurement -> field keys/types
```

### time index

```text
segment -> minTime/maxTime
series field block -> minTime/maxTime
```

## 推荐文件

```text
index/
  measurements.idx
  series.idx
  tags.idx
  fields.idx
  segments.manifest
```

## 优先级

**P0 / P1。**

Grafana、Prometheus remote write 场景下尤其重要。

***

# 4. Tag 条件过滤还不完整

## 当前状态

当前 `WHERE` 主要支持：

```sql
WHERE time >= ... AND time <= ...
```

但还没有完整支持：

```sql
WHERE host = 'server01'
WHERE region != 'cn'
WHERE host =~ /server.*/
WHERE host !~ /test.*/
WHERE value > 10
```

## 需要补充

### tag filter

```sql
WHERE host = 's1'
WHERE host != 's2'
WHERE region = 'cn' AND host = 's1'
```

### regex filter

```sql
WHERE host =~ /web.*/
WHERE host !~ /test.*/
```

### field filter

```sql
WHERE value > 80
WHERE temp <= 40
```

### mixed filter

```sql
WHERE time >= ... AND host='s1' AND value > 0.5
```

## 优先级

**P0。**

Grafana 查询基本都会带 tag 条件。

***

# 5. GROUP BY tag 还没有

## 当前状态

当前支持：

```sql
GROUP BY time(1m)
```

但不支持：

```sql
GROUP BY host
GROUP BY region
GROUP BY time(1m), host
GROUP BY time(1m), host, region
```

## 需要补充

需要把聚合 key 从：

```text
bucketTime
```

扩展为：

```text
bucketTime + selectedTagValues
```

输出也需要带 tags：

```json
{
  "name": "cpu",
  "tags": {
    "host": "s1"
  },
  "columns": ["time", "mean"],
  "values": [...]
}
```

## 优先级

**P0。**

这是 InfluxDB + Grafana 最常用能力之一。

***

# 6. fill() 还没有

## 当前状态

当前 `GROUP BY time()` 只返回有数据的 bucket。

不支持：

```sql
fill(null)
fill(0)
fill(previous)
fill(linear)
fill(none)
```

## 需要补充

示例：

```sql
SELECT mean(value)
FROM cpu
WHERE time >= ... AND time <= ...
GROUP BY time(1m)
fill(0)
```

需要生成完整时间窗口：

```text
00:00
00:01
00:02
00:03
...
```

没有数据的点按 fill 规则填充。

## 优先级

**P1。**

Grafana 图表展示需要。

***

# 7. Retention Policy 还不完整

## 当前状态

当前写入支持 `rp` 参数，但内部基本默认：

```text
autogen
```

没有真正实现：

```sql
CREATE RETENTION POLICY
ALTER RETENTION POLICY
DROP RETENTION POLICY
SHOW RETENTION POLICIES
```

也没有自动过期删除。

## 需要补充

Retention Policy 元数据：

```json
{
  "name": "one_week",
  "duration": "7d",
  "replication": 1,
  "default": true
}
```

Shard Group：

```text
db/rp/shard-start-end/
```

后台过期任务：

```text
delete shards where maxTime < now - retentionDuration
```

## 优先级

**P1。**

如果数据量持续增长，必须有。

***

# 8. Shard 管理还没有

## 当前状态

目前目录结构比较简单：

```text
data/db/{db}/{rp}/{segment}.seg
```

所有 segment 都在同一个 RP 目录下。

## 需要补充

应该改为：

```text
data/db/{db}/{rp}/shards/{shardId}/
  manifest.json
  000001.seg
  000002.seg
```

Shard 按时间划分，例如：

```text
1h shard
1d shard
7d shard
```

这样可以快速跳过不相关时间范围。

## 优先级

**P1。**

没有 shard，长期查询和删除都会慢。

***

# 9. Schema 管理还比较弱

## 当前状态

当前字段类型来自写入数据。

但是没有严格处理 InfluxDB 中常见问题：

```text
field type conflict
```

例如：

```text
cpu value=1.0
cpu value="abc"
```

InfluxDB 会认为同一 shard 中 field type 冲突。

## 需要补充

需要维护：

```text
measurement -> fieldKey -> fieldType
```

写入时校验：

```text
cpu.value Float
cpu.temp Integer
cpu.ok Boolean
```

如果类型冲突，返回 HTTP 400。

## 优先级

**P0。**

否则查询和压缩层会出现不一致。

***

# 10. Duplicate Point 处理还不完整

## 当前状态

如果写入相同：

```text
measurement + tags + timestamp
```

当前可能会形成重复 point 或字段覆盖不一致。

InfluxDB 的语义更接近：

```text
same measurement + same tag set + same timestamp
=> field set merge
=> 同名 field 后写覆盖
```

## 需要补充

写入或 compaction 时处理 duplicate point：

```text
seriesKey + timestamp -> fields merge
```

## 优先级

**P1。**

批量重复写入场景常见。

***

# 11. Delete / DROP 系列语句还没有

## 当前状态

当前没有实现：

```sql
DROP DATABASE
DROP MEASUREMENT
DROP SERIES
DELETE FROM
DROP RETENTION POLICY
DROP SHARD
```

## 需要补充

至少需要：

```sql
DROP DATABASE metrics;
DROP MEASUREMENT cpu;
DELETE FROM cpu WHERE time < ...;
```

底层需要 tombstone：

```text
tombstone/
  measurement delete
  series delete
  time range delete
```

然后 compaction 时真正清理数据。

## 优先级

**P1。**

管理数据必需。

***

# 12. Continuous Query 还没有

## 当前状态

没有实现：

```sql
CREATE CONTINUOUS QUERY
SHOW CONTINUOUS QUERIES
DROP CONTINUOUS QUERY
```

## 用途

例如：

```sql
CREATE CONTINUOUS QUERY cq_1m ON metrics
BEGIN
  SELECT mean(value)
  INTO metrics.autogen.cpu_1m
  FROM cpu
  GROUP BY time(1m)
END
```

## 需要补充

需要后台调度器：

```text
query planner
aggregation job
watermark
late data handling
```

## 优先级

**P2。**

不是基础查询必需，但生产降采样很重要。

***

# 13. INTO 查询还没有

## 当前状态

不支持：

```sql
SELECT mean(value) INTO cpu_1m FROM cpu GROUP BY time(1m)
```

## 需要补充

查询结果写回 TSDB：

```text
query result -> points -> write path
```

## 优先级

**P2。**

主要用于降采样和数据迁移。

***

# 14. Subquery 还没有

## 当前状态

不支持：

```sql
SELECT mean("max") FROM (
  SELECT max(value) FROM cpu GROUP BY time(10s)
) GROUP BY time(1m)
```

## 优先级

**P3。**

复杂查询能力，后期再做。

***

# 15. 更多函数还没有

当前只实现了：

```text
count
sum
mean
min
max
first
last
```

InfluxQL 常用函数还包括：

```text
median
mode
spread
stddev
percentile
elapsed
derivative
non_negative_derivative
difference
moving_average
cumulative_sum
integral
sample
top
bottom
```

## 优先级

建议顺序：

### P1

```text
percentile
stddev
spread
derivative
difference
```

### P2

```text
moving_average
cumulative_sum
elapsed
top
bottom
```

***

# 16. 查询优化器还没有

## 当前状态

目前是非常直接的执行方式：

```text
parse query -> scan points -> filter -> aggregate
```

没有 query plan。

## 需要补充

建议增加：

```text
LogicalPlan
PhysicalPlan
Predicate Pushdown
Projection Pushdown
Time Range Pushdown
Aggregate Pushdown
```

例如：

```text
SELECT mean(value) FROM cpu WHERE time >= ... AND host='s1'
```

应该执行为：

```text
1. 根据 time 找 shard
2. 根据 tag index 找 series
3. 根据 field projection 只读 value 列
4. block-level aggregation
```

## 优先级

**P1。**

数据量大后必需。

***

# 17. Block 元数据还不充分

## 当前状态

segment 里每个 column 有：

```text
measurement
tags
field
kind
minTime
maxTime
count
compressed bytes
```

但缺少更多统计信息：

```text
min value
max value
sum
count
null count
first
last
```

## 作用

如果有这些元数据，可以快速执行：

```sql
SELECT count(value)
SELECT min(value)
SELECT max(value)
```

甚至部分聚合可以不解压 block。

## 优先级

**P1。**

对性能提升很大。

***

# 18. 数据库元数据 manifest 还没有

## 当前状态

database 的存在主要通过目录判断。

## 需要补充

建议增加：

```text
meta/
  databases.json
  retention_policies.json
  users.json
  grants.json
```

或者：

```text
data/meta/manifest.json
```

记录：

```text
database
retention policy
default retention policy
shard groups
schema
last segment id
wal checkpoint
```

## 优先级

**P1。**

管理和恢复必需。

***

# 19. 用户认证和权限还没有

## 当前状态

没有实现认证。

InfluxDB 1.x 常见参数：

```text
u=username
p=password
```

以及 Basic Auth。

## 需要补充

SQL：

```sql
CREATE USER admin WITH PASSWORD 'xxx' WITH ALL PRIVILEGES;
CREATE USER readonly WITH PASSWORD 'xxx';
GRANT READ ON metrics TO readonly;
SHOW USERS;
DROP USER readonly;
```

HTTP：

```text
/query?u=admin&p=xxx
/write?u=admin&p=xxx
Authorization: Basic ...
```

## 优先级

**P1。**

内网测试可以不要，生产必须要。

***

# 20. HTTPS / TLS 还没有内置配置

## 当前状态

依赖 ASP.NET Core 外部配置。

## 需要补充

配置项：

```json
{
  "Http": {
    "Bind": "0.0.0.0",
    "Port": 8086,
    "Tls": {
      "Enabled": true,
      "CertPath": "./cert.pfx",
      "Password": "..."
    }
  }
}
```

## 优先级

**P2。**

可由 Nginx / Envoy / YARP 代理解决。

***

# 21. `/debug` 和诊断接口还没有

## 当前状态

没有实现：

```text
/debug/vars
/debug/pprof
/metrics
```

## 建议增加

```text
GET /health
GET /metrics
GET /debug/stats
GET /debug/storage
```

指标包括：

```text
write points/sec
query count
query latency
wal size
segment count
compaction queue length
memory buffer size
compression ratio
```

## 优先级

**P1。**

没有观测能力，生产不可控。

***

# 22. Prometheus Metrics 还没有

## 当前状态

没有暴露 Prometheus 格式指标。

## 需要补充

```text
GET /metrics
```

示例：

```text
mini_influx_write_points_total 123456
mini_influx_query_total 789
mini_influx_wal_bytes 102400
mini_influx_segment_files 42
mini_influx_compaction_running 0
```

## 优先级

**P1。**

生产部署建议必须有。

***

# 23. 写入限流和背压还没有

## 当前状态

当前写入直接进入：

```text
HTTP request -> parse -> WAL -> memory buffer
```

没有队列背压。

## 风险

高并发写入时可能：

* 内存暴涨
* WAL IO 被打满
* flush 阻塞请求
* GC 压力增大

## 需要补充

建议改成：

```text
HTTP write -> bounded channel -> write worker -> WAL group commit -> buffer
```

配置：

```json
{
  "Write": {
    "QueueCapacity": 100000,
    "BatchSize": 10000,
    "FlushIntervalMs": 1000,
    "MaxBodyBytes": 26214400
  }
}
```

## 优先级

**P0 / P1。**

高吞吐写入必须补。

***

# 24. WAL Group Commit 还没有

## 当前状态

每次写入调用：

```csharp
_stream.Flush(false)
```

但没有真正批量 fsync。

## 需要补充

策略：

```text
Flush every N points
Flush every N ms
Flush when WAL bytes > threshold
Optionally fsync
```

配置：

```json
{
  "Wal": {
    "Fsync": true,
    "FsyncIntervalMs": 1000,
    "MaxSegmentBytes": 134217728
  }
}
```

## 优先级

**P0。**

关系到性能和可靠性平衡。

***

# 25. 并发控制还比较简单

## 当前状态

目前主要用：

```csharp
lock (_lock)
```

## 问题

读写互斥粒度比较粗。

## 需要补充

建议：

```text
per database lock
per shard lock
reader-writer lock
immutable memtable
active memtable + flushing memtable
```

架构类似：

```text
active memtable
immutable memtable queue
flush workers
```

## 优先级

**P1。**

并发量上来后必须优化。

***

# 26. Memory 限制和 OOM 保护还没有

## 当前状态

buffer 达到点数阈值 flush。

但没有根据：

```text
内存大小
字段数量
series 数量
压缩前字节数
```

做控制。

## 需要补充

配置：

```json
{
  "Memory": {
    "MaxBufferBytes": 1073741824,
    "FlushHighWatermarkBytes": 805306368,
    "RejectWritesAtBytes": 1200000000
  }
}
```

## 优先级

**P1。**

防止写入峰值把进程打爆。

***

# 27. Cardinality 控制还没有

## 当前状态

没有限制 series cardinality。

## 风险

如果 tag 很多：

```text
cpu,host=uuid-1
cpu,host=uuid-2
cpu,host=uuid-3
...
```

series 数量会爆炸。

## 需要补充

```json
{
  "Limits": {
    "MaxSeriesPerDatabase": 10000000,
    "MaxTagValuesPerKey": 1000000,
    "MaxFieldsPerMeasurement": 1024
  }
}
```

并提供：

```sql
SHOW SERIES CARDINALITY
SHOW TAG VALUES CARDINALITY
```

## 优先级

**P1。**

TSDB 生产事故高发点。

***

# 28. 查询结果分页 / chunked response 还没有

## 当前状态

查询结果一次性构造完整 JSON 返回。

## 风险

大查询会：

* 占用大量内存
* 响应慢
* 可能 OOM

## 需要补充

InfluxDB 1.x 支持类似：

```text
chunked=true
chunk_size=10000
```

返回分块结果。

## 优先级

**P1。**

大结果集必须补。

***

# 29. 压缩算法还可以继续增强

## 当前状态

当前已经有基础压缩。

但 Gorilla double 压缩还不是完整实现，当前是：

```text
double bits XOR previous bits + varint
```

完整 Gorilla 通常会利用：

```text
leading zeros
trailing zeros
significant bits
control bits
```

## 需要补充

可选：

```text
Gorilla timestamp codec
Gorilla float codec
RLE codec
Simple8b
Dictionary + RLE hybrid
ZSTD block compression
```

不过 .NET 内置没有 ZSTD，需要额外依赖或自行实现，不利于 AOT 简洁性。

## 优先级

**P2。**

当前压缩已可用，后续可优化。

***

# 30. 文件格式版本化还没有

## 当前状态

segment magic 有，但没有完整 version。

## 需要补充

segment header：

```text
magic
version
flags
createdAt
minTime
maxTime
columnCount
footerOffset
checksum
```

## 作用

未来升级格式时可以兼容旧数据。

## 优先级

**P1。**

数据库格式演进必须考虑。

***

# 31. Checksum / CRC 还没有

## 当前状态

segment 没有 block checksum。

## 风险

磁盘损坏或写入中断时无法检测局部损坏。

## 需要补充

```text
block crc32c
footer checksum
manifest checksum
wal record checksum
```

## 优先级

**P0 / P1。**

可靠性关键。

***

# 32. 原子文件写入还需要增强

## 当前状态

segment 直接写目标文件：

```csharp
FileMode.Create
```

## 风险

进程崩溃可能留下半文件。

## 需要补充

```text
write xxx.seg.tmp
fsync file
rename xxx.seg.tmp -> xxx.seg
fsync directory
```

## 优先级

**P0。**

存储系统基本要求。

***

# 33. WAL 文件轮转还没有

## 当前状态

只有：

```text
current.wal
```

会一直增长。

## 需要补充

```text
000001.wal
000002.wal
000003.wal
```

配合 checkpoint 删除旧 WAL。

## 优先级

**P0。**

否则长时间运行 WAL 会无限增大。

***

# 34. Snapshot / Backup / Restore 还没有

## 当前状态

没有备份恢复接口。

## 需要补充

```text
mini-influx backup ./backup
mini-influx restore ./backup
```

或者 API：

```text
POST /admin/backup
POST /admin/restore
```

需要保证：

```text
manifest + segment + wal checkpoint
```

一致性。

## 优先级

**P2。**

生产运维需要。

***

# 35. 配置系统还比较简单

## 当前状态

只有：

```json
{
  "MiniInflux": {
    "DataPath": "./data",
    "FlushThreshold": 50000
  }
}
```

## 需要补充

建议扩展为：

```json
{
  "Server": {
    "HttpPort": 8086,
    "MaxRequestBodyBytes": 26214400
  },
  "Storage": {
    "DataPath": "./data",
    "ShardDuration": "1d",
    "FlushThresholdPoints": 50000,
    "FlushIntervalMs": 5000
  },
  "Wal": {
    "Fsync": true,
    "FsyncIntervalMs": 1000,
    "MaxWalFileBytes": 134217728
  },
  "Compaction": {
    "Enabled": true,
    "MaxConcurrency": 2
  },
  "Limits": {
    "MaxSeriesPerDatabase": 10000000,
    "MaxQueryPoints": 10000000
  }
}
```

## 优先级

**P1。**

***

# 36. 管理 CLI 还没有

## 当前状态

只能通过 HTTP 操作。

## 建议增加

```bash
mini-influxd
mini-influx inspect segment
mini-influx repair
mini-influx compact
mini-influx backup
mini-influx restore
mini-influx benchmark
```

## 优先级

**P2。**

***

# 37. Benchmark 还没有

## 当前状态

没有标准压测工具。

## 需要补充

至少要有：

```text
write benchmark
query benchmark
compression benchmark
recovery benchmark
compaction benchmark
```

指标：

```text
points/sec
bytes/point
compression ratio
p50/p95/p99 write latency
p50/p95/p99 query latency
startup recovery time
```

## 优先级

**P1。**

否则无法评估优化效果。

***

# 38. 测试体系还没有

## 当前状态

包里只有 `smoke-test.sh`，没有正式单元测试和集成测试。

## 需要补充

### 单元测试

```text
LineProtocolParserTests
InfluxQlParserTests
CompressionCodecTests
SegmentReaderWriterTests
WalTests
QueryExecutorTests
```

### 集成测试

```text
WriteThenQueryTests
RestartRecoveryTests
CompactionTests
GrafanaCompatibilityTests
```

### 模糊测试

```text
Line protocol fuzzing
InfluxQL fuzzing
Segment corruption fuzzing
```

## 优先级

**P0 / P1。**

代码继续演进前必须补测试。

***

# 三、按优先级整理

## P0：必须优先补

这些是"数据库能否可靠运行"的基础。

```text
1. WAL Recovery                    ✅ 已完成 - WalManager.Replay()
2. WAL 文件轮转                    ✅ 已完成 - WalManager rotation on MaxWalFileBytes
3. WAL checkpoint                  ✅ 已完成 - WalManager.Checkpoint() + checkpoint.dat
4. 原子 segment 写入               ✅ 已完成 - SegmentWriter writes .tmp then rename
5. segment / wal checksum          ✅ 已完成 - CRC32C on both WAL records and segments
6. field type conflict 校验        ✅ 已完成 - SchemaRegistry validates on write
7. tag 条件过滤                    ✅ 已完成 - WHERE host='x', host!='x', host=~/regex/
8. 写入队列和背压                  ✅ 已完成 - WriteQueue with bounded channel, HTTP 429
9. WAL group commit                ✅ 已完成 - fsync interval timer in WalManager
10. 基础测试体系                   ✅ 已完成 - 44 tests in MiniInflux.Tests
```

***

## P1：生产可用核心能力

这些决定性能、稳定性。

```text
1.  Compaction (L0->L1 merge)              ✅ 已完成 - Compactor.cs + background timer
2.  持久化索引 (轻量级 manifest)            ✅ 已完成 - Manifest.cs + segment-level index
3.  shard 管理                              ✅ 已完成 - ShardManager.cs + time-based routing
4.  retention policy                        ✅ 已完成 - DDL + auto-expiry background task
5.  GROUP BY tag                            ✅ 已完成 - tag grouping in QueryExecutor.AggGroupBy()
6.  fill()                                  ✅ 已完成 - fill(null/0/previous/linear) in AggGroupBy()
7.  删除语句和 tombstone                    ✅ 已完成 - TombstoneStore + DROP/DELETE InfluxQL
8.  并发控制优化                            ✅ 已完成 - ReaderWriterLockSlim per db/rp
9.  cardinality 限制                        ✅ 已完成 - series counter + CardinalityLimitExceededException
10. block metadata                          ✅ 已完成 - min/max/sum/count per column in segment v2
11. manifest 元数据                         ✅ 已完成 - Manifest.cs (merged with persistent index)
12. 配置系统增强                            ✅ 已完成 - Storage.RpCheckIntervalMs/MaxSeriesPerDatabase/MaxFieldsPerMeasurement

(Downgraded to P2)
- 查询结果 chunked response                → 使用 LIMIT 替代
- 内存限制                                  → WriteQueue backpressure 已提供基本保护
- benchmark                                 → 使用外部 HTTP 压测工具
```

***

## P2：增强型能力

这些是生产运维和高级使用场景需要。

```text
1.  用户认证和权限
2.  HTTPS/TLS 配置
3.  Continuous Query
4.  SELECT INTO
5.  backup / restore
6.  管理 CLI
7.  更高级压缩算法
8.  查询结果 chunked response
9.  内存限制 / OOM 保护
10. Benchmark 工具
```

***

## P1.5：本轮新完成 (Next Urgent Features)

```text
1.  Duplicate Point 处理                     ✅ 已完成 - TsdbEngine.DeduplicatePoints() + Compactor last-write-wins
2.  /health + /debug/stats 接口              ✅ 已完成 - Program.cs + MetricsCollector.cs
3.  Prometheus /metrics 接口                  ✅ 已完成 - MetricsCollector.FormatPrometheus()
4.  更多聚合函数 (percentile/stddev/spread)    ✅ 已完成 - QueryExecutor + InfluxQlParser multi-arg
5.  Measurement Index                        ✅ 已完成 - ReadAllPoints metadata skip
6.  Series Index                             ✅ 已完成 - Manifest.SeriesIndex
7.  Tag Inverted Index                       ✅ 已完成 - Manifest.TagIndex
8.  Time Range Pushdown (enhanced)           ✅ 已完成 - ReadMetadata + block stats skip
9.  Projection Pushdown (Column Pruning)     ✅ 已完成 - SegmentReader(requestedFields)
```

***

## P3：高级兼容能力

这些可以最后做。

```text
1. Subquery
2. 完整 InfluxQL planner
3. Flux 兼容
4. 多节点副本
5. 分布式 shard
6. 远程读写协议
```

***

# 四、建议下一阶段开发路线

我建议不要一次性补所有功能，而是按版本演进。

***

## v0.2：可靠写入版

目标：数据不丢、文件不坏、重启可恢复。

需要补：

```text
WAL Recovery
WAL Rotation
WAL Checkpoint
Atomic Segment Write
CRC Checksum
Field Type Conflict
基础单元测试
```

这是最关键的一版。

***

## v0.3：查询性能版

目标：避免全量扫描。

需要补：

```text
Manifest
Segment Metadata
Measurement Index
Series Index
Tag Inverted Index
Time Range Pushdown
Projection Pushdown
```

***

## v0.4：Grafana 兼容版

目标：Grafana 基本可用。

需要补：

```text
SHOW SERIES
SHOW TAG VALUES 完整兼容
SHOW FIELD KEYS 完整兼容
WHERE tag filter
GROUP BY time(), tag
fill()
chunked response
```

***

## v0.5：生产运行版

目标：长期运行可控。

需要补：

```text
Compaction
Retention Policy
Shard Group
Delete / Tombstone
Prometheus Metrics
Memory Limit
Cardinality Limit
Write Backpressure
```

***

## v0.6：运维增强版

目标：方便部署、排障和迁移。

需要补：

```text
Auth
Backup / Restore
CLI
Benchmark
Repair Tool
Config Hot Reload
```

***

# 五、当前版本最大风险点

如果你现在直接拿当前包做高负载或生产测试，最大风险是：

```text
1. 异常退出后，未 flush 数据不会自动恢复
2. WAL 会一直增长
3. segment 文件没有 checksum
4. segment 写入不是 tmp + rename 原子写
5. 查询是 scan-heavy，数据大了会慢
6. 没有 compaction，segment 会越来越多
7. 没有 tag index，Grafana 查询会慢
8. field 类型冲突没有强校验
9. 大查询一次性返回，可能占用大量内存
10. 没有认证、限流、内存保护
```

***

# 六、我建议下一步先补哪一块

我建议下一步直接补 **v0.2 可靠写入版**。

也就是我继续给你补：

```text
1. WAL Record 格式
2. WAL Reader
3. 启动 Replay
4. WAL Rotation
5. Checkpoint 文件
6. Segment tmp 写入 + rename
7. Segment CRC32
8. Schema Registry
9. Field Type Conflict 检查
10. 对应测试脚本
```

补完后，这个数据库才具备基本的“可恢复存储引擎”形态。
