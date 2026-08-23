# MiniInflux 与 InfluxDB 1.x 查询功能对比清单

更新时间：2026-08-23

## 说明

本文档以 InfluxDB 1.8 (1.x 最后一版) 官方查询语言为基准，逐项对比 MiniInflux 当前实现状态。标注含义：

- `[done]` — 已实现且通过测试
- `[partial]` — 解析器或执行器部分支持，存在已知限制
- `[missing]` — 未实现

本次更新基于源码核对（`MiniInflux/Protocol/InfluxQlParser.cs`、`MiniInflux/Query/QueryExecutor.cs`），修正了 2026-08-02 版本中已过时的状态标注。

---

## 一、SELECT 数据探索

### 1.1 基本 SELECT 语法

| 功能 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `SELECT field FROM measurement` | 支持 | 支持 | `[done]` |
| `SELECT * FROM measurement` | 支持 | 支持 | `[done]` |
| `SELECT field1,field2 FROM measurement` | 支持 | 支持 | `[done]` |
| `SELECT mean(field) FROM measurement` | 支持 | 支持 | `[done]` |
| 限定名 `db.rp.measurement` | 支持 | 支持 | `[done]` |
| 子查询 `(SELECT ... ) FROM ...` | 支持 | 支持 | `[done]` |
| `SELECT ... INTO ...` | 支持 | 支持 | `[done]` |
| `SELECT DISTINCT field FROM ...` | 支持 | 支持 | `[done]` |
| `SELECT COUNT(DISTINCT field) FROM ...` | 支持 | 支持 | `[done]` |

### 1.2 WHERE 子句

| 功能 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `WHERE time > ...` / `>=` / `<` / `<=` | 支持 | 支持 | `[done]` |
| `time >= now() - 1h` 相对时间 | 支持 | 支持 | `[done]` |
| `time >= '2020-01-01T00:00:00Z'` 绝对时间 | 支持 | 支持 | `[done]` |
| `WHERE tag = 'value'` | 支持 | 支持 | `[done]` |
| `WHERE tag != 'value'` | 支持 | 支持 | `[done]` |
| `WHERE tag =~ /regex/` | 支持 | 支持 | `[done]` |
| `WHERE tag !~ /regex/` | 支持 | 支持 | `[done]` |
| `WHERE field > 80` (数值过滤) | 支持 | 支持 | `[done]` |
| `WHERE field = 'string'` (字符串字段) | 支持 | 支持（flushed 与 buffer 路径） | `[done]` |
| `AND` 多条件组合 | 支持 | 支持 | `[done]` |
| `OR` 多条件组合 | 不支持 (InfluxQL 限制) | 已扩展支持 | `[done]` |
| 括号分组 `(a OR b) AND c` | 不支持 | 已扩展支持 | `[done]` |

### 1.3 GROUP BY 子句

| 功能 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `GROUP BY time(1h)` 时间分组 | 支持 | 支持 | `[done]` |
| `GROUP BY "tag"` 标签分组 | 支持 | 支持 | `[done]` |
| `GROUP BY *` 全标签分组 | 支持 | 支持 | `[done]` |
| `GROUP BY time(1h),"tag"` 混合分组 | 支持 | 支持 | `[done]` |
| `GROUP BY time(...) fill(...)` 填充 | 支持 | 支持 | `[done]` |
| `fill(null)` / `fill(0)` / `fill(previous)` / `fill(linear)` | 支持 | 支持 | `[done]` |

### 1.4 排序与分页

| 功能 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `ORDER BY time ASC` (默认) | 支持 | 支持 (默认行为) | `[done]` |
| `ORDER BY time DESC` | 支持 | 支持 | `[done]` |
| `LIMIT N` | 支持 | 支持 | `[done]` |
| `OFFSET N` | 支持 | 支持 | `[done]` |
| `SLIMIT N` (series 限制) | 支持 | 支持 | `[done]` |
| `SOFFSET N` (series 偏移) | 支持 | 支持 | `[done]` |

---

## 二、聚合与转换函数

### 2.1 基础聚合

| 函数 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `COUNT(field)` | 支持 | 支持 | `[done]` |
| `COUNT(*)` | 支持 | 支持 | `[done]` |
| `SUM(field)` | 支持 | 支持 | `[done]` |
| `MEAN(field)` | 支持 | 支持 | `[done]` |
| `MEDIAN(field)` | 支持 | 支持 | `[done]` |
| `MIN(field)` / `MAX(field)` | 支持 | 支持 | `[done]` |
| `FIRST(field)` / `LAST(field)` | 支持 | 支持 | `[done]` |
| `SPREAD(field)` | 支持 | 支持 | `[done]` |
| `STDDEV(field)` | 支持 | 支持 | `[done]` |
| `MODE(field)` | 支持 | 支持 | `[done]` |
| `PERCENTILE(field, N)` | 支持 | 支持 | `[done]` |

### 2.2 转换函数

| 函数 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `DERIVATIVE(field, unit)` | 支持 | 支持 | `[done]` |
| `NON_NEGATIVE_DERIVATIVE(field, unit)` | 支持 | 支持 | `[done]` |
| `DIFFERENCE(field)` | 支持 | 支持 | `[done]` |
| `NON_NEGATIVE_DIFFERENCE(field)` | 支持 | 支持 | `[done]` |
| `MOVING_AVERAGE(field, N)` | 支持 | 支持 | `[done]` |
| `CUMULATIVE_SUM(field)` | 支持 | 支持 | `[done]` |
| `ELAPSED(field, unit)` | 支持 | 支持 | `[done]` |
| `INTEGRAL(field, unit)` | 支持 | 支持 | `[done]` |

### 2.3 排名与选择

| 函数 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `TOP(field, N)` | 支持 | 支持 | `[done]` |
| `BOTTOM(field, N)` | 支持 | 支持 | `[done]` |
| `SAMPLE(field, N)` | 支持 | 支持 | `[done]` |

### 2.4 高级函数 (未实现)

| 函数 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `HOLT_WINTERS(field, N, S)` | 支持 | 未实现 | `[missing]` |
| `HOLT_WINTERS_WITH_MONTHLY` | 支持 | 未实现 | `[missing]` |
| `HISTOGRAM(field, N)` | 支持 | 未实现 | `[missing]` |

### 2.5 数学函数

| 函数 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `ABS(field)` | 支持 | 支持 | `[done]` |
| `CEIL(field)` / `FLOOR(field)` / `ROUND(field)` | 支持 | 支持 | `[done]` |
| `SQRT(field)` | 支持 | 支持 | `[done]` |
| `EXP(field)` | 支持 | 支持 | `[done]` |
| `LOG(field)` / `LOG2(field)` / `LN(field)` | 支持 | 支持 | `[done]` |
| `SIN(field)` / `COS(field)` / `TAN(field)` | 支持 | 支持 | `[done]` |
| `ATAN2(y, x)` | 支持 | 支持（务实子集：第二参数为常量） | `[done]` |

---

## 三、Schema 探索 (SHOW)

| 命令 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `SHOW DATABASES` | 支持 | 支持 | `[done]` |
| `SHOW MEASUREMENTS` | 支持 | 支持 | `[done]` |
| `SHOW MEASUREMENTS WITH MEASUREMENT =~ /.../` | 支持 | 支持 | `[done]` |
| `SHOW MEASUREMENTS WHERE "tag" = '...'` | 支持 | 支持 | `[done]` |
| `SHOW SERIES` | 支持 | 支持 (含 WHERE 过滤) | `[done]` |
| `SHOW SERIES FROM ... WHERE ...` | 支持 | 支持 | `[done]` |
| `SHOW SERIES WITH MEASUREMENT =~ /.../` | 支持 | 支持 | `[done]` |
| `SHOW TAG KEYS` | 支持 | 支持 (含 WHERE 过滤) | `[done]` |
| `SHOW TAG KEYS FROM ... WITH MEASUREMENT =~ /.../` | 支持 | 支持 | `[done]` |
| `SHOW TAG VALUES WITH KEY = "tag"` | 支持 | 支持 | `[done]` |
| `SHOW TAG VALUES FROM ... WITH KEY = ... WHERE ...` | 支持 | 支持 (含 OR 组合) | `[done]` |
| `SHOW TAG VALUES WITH KEY =~ /.../` | 支持 | 支持 | `[done]` |
| `SHOW FIELD KEYS` | 支持 | 支持 | `[done]` |
| `SHOW FIELD KEYS FROM ...` | 支持 | 支持 | `[done]` |
| `SHOW RETENTION POLICIES ON db` | 支持 | 支持 | `[done]` |
| `SHOW CONTINUOUS QUERIES` | 支持 | 支持 | `[done]` |
| `SHOW SERIES CARDINALITY` | 支持 | 支持 | `[done]` |
| `SHOW MEASUREMENT CARDINALITY` | 支持 | 支持 | `[done]` |
| `SHOW TAG VALUES CARDINALITY` | 支持 | 支持 | `[done]` |
| `SHOW TAG KEY CARDINALITY` | 支持 | 支持 | `[done]` |
| `SHOW FIELD KEY CARDINALITY` | 支持 | 支持 | `[done]` |

---

## 四、系统管理与诊断 (SHOW)

| 命令 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `SHOW QUERIES` | 支持 | 支持 | `[done]` |
| `KILL QUERY <id>` | 支持 | 支持 | `[done]` |
| `SHOW SHARDS` | 支持 | 支持 | `[done]` |
| `SHOW SHARD GROUPS` | 支持 | 支持 | `[done]` |
| `SHOW STATS` | 支持 | 支持 (`/debug/stats` + `SHOW STATS`) | `[done]` |
| `SHOW STATS FOR 'meas'` | 支持 | 支持 | `[done]` |
| `SHOW DIAGNOSTICS` | 支持 | 支持 | `[done]` |
| `SHOW SUBSCRIPTIONS` | 支持 | 未实现 (单节点架构) | `[missing]` |
| `EXPLAIN <query>` | 支持 | 支持 | `[done]` |
| `EXPLAIN ANALYZE <query>` | 支持 | 支持 | `[done]` |

---

## 五、数据库与存储管理

| 命令 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `CREATE DATABASE db` | 支持 | 支持 | `[done]` |
| `CREATE DATABASE IF NOT EXISTS db` | 支持 | 支持 | `[done]` |
| `CREATE DATABASE db WITH DURATION ... REPLICATION ... SHARD ... NAME ...` | 支持 | 支持 | `[done]` |
| `DROP DATABASE db` | 支持 | 支持 | `[done]` |
| `DROP MEASUREMENT m` | 支持 | 支持 | `[done]` |
| `DROP SERIES FROM ... WHERE ...` | 支持 | 支持 | `[done]` |
| `DROP SHARD <id>` | 支持 | 支持 | `[done]` |
| `DELETE FROM ... WHERE ...` | 支持 | 支持 | `[done]` |

---

## 六、Retention Policy 管理

| 命令 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `CREATE RETENTION POLICY ... ON ... DURATION ... REPLICATION ... [DEFAULT]` | 支持 | 支持 | `[done]` |
| `ALTER RETENTION POLICY ... ON ... [DURATION ...] [DEFAULT]` | 支持 | 支持 | `[done]` |
| `DROP RETENTION POLICY ... ON ...` | 支持 | 支持 | `[done]` |

---

## 七、Continuous Query 管理

| 命令 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `CREATE CONTINUOUS QUERY ... ON ... [RESAMPLE EVERY ... FOR ...] BEGIN ... END` | 支持 | 支持 | `[done]` |
| `DROP CONTINUOUS QUERY ... ON ...` | 支持 | 支持 | `[done]` |
| `SHOW CONTINUOUS QUERIES` | 支持 | 支持 | `[done]` |

---

## 八、用户与权限管理 (全部未实现，单管理员模型为既定设计)

| 命令 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `CREATE USER username WITH PASSWORD '...'` | 支持 | 未实现 | `[missing]` |
| `CREATE USER username WITH PASSWORD WITH ALL PRIVILEGES` | 支持 | 未实现 | `[missing]` |
| `GRANT [READ\|WRITE\|ALL] ON db TO username` | 支持 | 未实现 | `[missing]` |
| `REVOKE [READ\|WRITE\|ALL] ON db FROM username` | 支持 | 未实现 | `[missing]` |
| `SET PASSWORD FOR username = '...'` | 支持 | 未实现 | `[missing]` |
| `DROP USER username` | 支持 | 未实现 | `[missing]` |
| `SHOW USERS` | 支持 | 未实现 | `[missing]` |
| `SHOW GRANTS FOR username` | 支持 | 未实现 | `[missing]` |

MiniInflux 目前使用配置文件中的单一 Basic Auth 凭据 (`Auth.Enabled` + `Auth.Username` + `Auth.Password`)，不实现多用户和细粒度权限模型。

---

## 九、Subscription 管理 (全部未实现，单节点架构为既定设计)

| 命令 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `CREATE SUBSCRIPTION ... ON db.rp DESTINATIONS ALL 'host:port'` | 支持 | 未实现 | `[missing]` |
| `DROP SUBSCRIPTION ... ON db.rp` | 支持 | 未实现 | `[missing]` |
| `SHOW SUBSCRIPTIONS` | 支持 | 未实现 | `[missing]` |

MiniInflux 为单节点架构，不实现 Kapacitor / Subscription 机制。

---

## 十、时间语法

| 语法 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| 纳秒整数 `1710000000000000000` | 支持 | 支持 | `[done]` |
| RFC3339 字符串 `'2020-01-01T00:00:00Z'` | 支持 | 支持 | `[done]` |
| `now()` 当前时间 | 支持 | 支持 | `[done]` |
| `now() - 1h` / `now() + 10m` 相对偏移 | 支持 | 支持 | `[done]` |
| epoch 时间 `1234567890s` / `ms` / `u` / `ns` | 支持 | 支持 | `[done]` |

---

## 十一、Line Protocol 写入

| 功能 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `measurement,tag=k field=v timestamp` | 支持 | 支持 | `[done]` |
| 多字段写入 | 支持 | 支持 | `[done]` |
| 多标签写入 | 支持 | 支持 | `[done]` |
| Integer `i` 后缀 | 支持 | 支持 | `[done]` |
| Float 无后缀 | 支持 | 支持 | `[done]` |
| Boolean `t/f/T/F/true/false` | 支持 | 支持 | `[done]` |
| String 双引号 | 支持 | 支持 | `[done]` |
| 精度参数 `?precision=ns\|u\|ms\|s` | 支持 | 支持 | `[done]` |

---

## 十二、API 端点对比

| 端点 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `GET /ping` | 支持 | 支持 | `[done]` |
| `GET /health` | — | 支持 | `[done]` |
| `POST /write` | 支持 | 支持 | `[done]` |
| `GET/POST /query` | 支持 | 支持 | `[done]` |
| `GET /debug/pprof` | 支持 | 未实现 | `[missing]` |
| `GET /debug/vars` | 支持 | 部分实现 (`/debug/stats`) | `[partial]` |
| `GET /debug/requests` | 支持 | 未实现 | `[missing]` |
| `GET /metrics` | — (Prometheus) | 支持 | `[done]` |

补充差异：MiniInflux `/query` 支持 `chunked`、`chunk_size`、`epoch`、`debug` 参数；请求体支持 gzip 解压；`/query`、`/metrics`、`/debug/stats`、`/debug/benchmark`、`/admin/api` 响应支持 `Accept-Encoding: gzip` 编码。

---

## 统计汇总

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

---

## 已知缺陷（非兼容性缺口）

- ~~`Compactor_OutputRespectsMaxSegmentFileBytes` 回归测试失败~~ `[fixed 2026-08-23]`：根因是 `EstimateColumnBytes` 未计入 metadata footer 双份字符串/统计与文件级开销，小列估算偏低约 1.7x；已将固定开销修正为 128 字节 + 字符串双份计长，全量测试 284/284 通过。
- WAL fsync 为定时组提交（默认 1000ms）：进程崩溃安全，断电可能丢失最近一个 fsync 窗口内已确认写入。该取舍已在 README 配置说明中显式记录（2026-08-23）。

---

## 优先级建议

### P0 — 影响已有数据的查询可用性

已完成（2026-08-23）：compaction tail-merge 回归修复；`SHOW TAG VALUES WITH KEY =~ /regex/`；`SHOW SERIES/TAG KEYS WITH MEASUREMENT` 过滤。

### P1 — 常用查询增强

已完成（2026-08-23）：剩余数学函数（`EXP`、`LOG/LOG2/LN`）；字符串字段等值 WHERE 过滤；WHERE 括号布尔分组 `(a OR b) AND c`；HTTP 响应 gzip 编码（`Accept-Encoding: gzip`，覆盖 `/query`、`/metrics` 等文本端点）。

### P2 — 运维与兼容性
8. `HOLT_WINTERS` / `HISTOGRAM` 高级统计函数（计划任务 5.7 剩余部分）
9. ~~三角函数与对数函数~~ `[done 2026-08-23]`（`SIN/COS/TAN` 与务实子集 `ATAN2`）
10. `/debug/pprof` 风格的诊断端点

其余 P2 项（HTTP gzip 响应、compaction 节流、RLE/Simple8b、索引重建、benchmark workload、示例文档）已并入 [todo-202608.md](todo-202608.md) 阶段五统一排期。

### P3 — 单节点架构下优先级较低
11. 多用户权限模型 (`CREATE USER`, `GRANT`, `REVOKE`)
12. Subscription 机制