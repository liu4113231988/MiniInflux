# MiniInflux 与 InfluxDB 1.x 查询功能对比清单

更新时间：2026-08-02

## 说明

本文档以 InfluxDB 1.8 (1.x 最后一版) 官方查询语言为基准，逐项对比 MiniInflux 当前实现状态。标注含义：

- `[done]` — 已实现且通过测试
- `[partial]` — 解析器或执行器部分支持，存在已知限制
- `[missing]` — 未实现

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
| `SELECT DISTINCT field FROM ...` | 支持 | 未实现 | `[missing]` |
| `SELECT COUNT(DISTINCT field) FROM ...` | 支持 | 未实现 | `[missing]` |

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
| `WHERE field = 'string'` (字符串字段) | 支持 | 有限支持 | `[partial]` |
| `AND` 多条件组合 | 支持 | 支持 | `[done]` |
| `OR` 多条件组合 | 不支持 (InfluxQL 限制) | 已扩展支持 | `[done]` |
| 括号分组 `(a OR b) AND c` | 不支持 | 不支持 | — |

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
| `MODE(field)` | 支持 | 未实现 | `[missing]` |
| `PERCENTILE(field, N)` | 支持 | 支持 | `[done]` |

### 2.2 转换函数

| 函数 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `DERIVATIVE(field, unit)` | 支持 | 支持 | `[done]` |
| `NON_NEGATIVE_DERIVATIVE(field, unit)` | 支持 | 支持 | `[done]` |
| `DIFFERENCE(field)` | 支持 | 支持 | `[done]` |
| `NON_NEGATIVE_DIFFERENCE(field)` | 支持 | 未实现 | `[missing]` |
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

### 2.5 数学函数 (未实现)

| 函数 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `ABS(field)` | 支持 | 未实现 | `[missing]` |
| `SIN(field)` / `COS(field)` / `TAN(field)` | 支持 | 未实现 | `[missing]` |
| `LOG(field)` / `LOG2(field)` / `LN(field)` | 支持 | 未实现 | `[missing]` |
| `EXP(field)` / `SQRT(field)` | 支持 | 未实现 | `[missing]` |
| `CEIL(field)` / `FLOOR(field)` / `ROUND(field)` | 支持 | 未实现 | `[missing]` |
| `ATAN2(y, x)` | 支持 | 未实现 | `[missing]` |

---

## 三、Schema 探索 (SHOW)

| 命令 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `SHOW DATABASES` | 支持 | 支持 | `[done]` |
| `SHOW MEASUREMENTS` | 支持 | 支持 (无过滤) | `[partial]` |
| `SHOW MEASUREMENTS WITH MEASUREMENT =~ /.../` | 支持 | 未实现 | `[missing]` |
| `SHOW MEASUREMENTS WHERE "tag" = '...'` | 支持 | 未实现 | `[missing]` |
| `SHOW SERIES` | 支持 | 支持 (基本) | `[partial]` |
| `SHOW SERIES FROM ... WHERE ...` | 支持 | 部分支持 | `[partial]` |
| `SHOW SERIES WITH MEASUREMENT =~ /.../` | 支持 | 未实现 | `[missing]` |
| `SHOW TAG KEYS` | 支持 | 支持 (无过滤) | `[partial]` |
| `SHOW TAG KEYS FROM ... WITH MEASUREMENT =~ /.../` | 支持 | 未实现 | `[missing]` |
| `SHOW TAG VALUES WITH KEY = "tag"` | 支持 | 支持 (基本) | `[partial]` |
| `SHOW TAG VALUES FROM ... WITH KEY = ... WHERE ...` | 支持 | 部分支持 | `[partial]` |
| `SHOW TAG VALUES WITH KEY =~ /.../` | 支持 | 未实现 | `[missing]` |
| `SHOW FIELD KEYS` | 支持 | 支持 | `[done]` |
| `SHOW FIELD KEYS FROM ...` | 支持 | 支持 | `[done]` |
| `SHOW RETENTION POLICIES ON db` | 支持 | 支持 | `[done]` |
| `SHOW CONTINUOUS QUERIES` | 支持 | 支持 | `[done]` |
| `SHOW SERIES CARDINALITY` | 支持 | 支持 | `[done]` |
| `SHOW MEASUREMENT CARDINALITY` | 支持 | 支持 | `[done]` |
| `SHOW TAG VALUES CARDINALITY` | 支持 | 支持 | `[done]` |
| `SHOW TAG KEY CARDINALITY` | 支持 | 未实现 | `[missing]` |
| `SHOW FIELD KEY CARDINALITY` | 支持 | 未实现 | `[missing]` |

---

## 四、系统管理与诊断 (SHOW)

| 命令 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `SHOW QUERIES` | 支持 | 未实现 | `[missing]` |
| `KILL QUERY <id>` | 支持 | 未实现 | `[missing]` |
| `SHOW SHARDS` | 支持 | 未实现 | `[missing]` |
| `SHOW SHARD GROUPS` | 支持 | 未实现 | `[missing]` |
| `SHOW STATS` | 支持 | 部分实现 (`/debug/stats`) | `[partial]` |
| `SHOW STATS FOR 'meas'` | 支持 | 未实现 | `[missing]` |
| `SHOW DIAGNOSTICS` | 支持 | 未实现 | `[missing]` |
| `SHOW SUBSCRIPTIONS` | 支持 | 未实现 | `[missing]` |
| `EXPLAIN <query>` | 支持 | 未实现 | `[missing]` |
| `EXPLAIN ANALYZE <query>` | 支持 | 未实现 | `[missing]` |

---

## 五、数据库与存储管理

| 命令 | InfluxDB 1.x | MiniInflux | 状态 |
|------|-------------|-----------|------|
| `CREATE DATABASE db` | 支持 | 支持 | `[done]` |
| `CREATE DATABASE IF NOT EXISTS db` | 支持 | 支持 | `[done]` |
| `CREATE DATABASE db WITH DURATION ... REPLICATION ... SHARD ... NAME ...` | 支持 | 未实现 | `[missing]` |
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

## 八、用户与权限管理 (全部未实现)

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

## 九、Subscription 管理 (全部未实现)

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
| epoch 时间 `1234567890s` | 支持 | 仅纯数字纳秒 | `[partial]` |

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

---

## 统计汇总

| 类别 | done | partial | missing | 完成率 |
|------|------|---------|--------|--------|
| SELECT 语法 | 10 | 1 | 2 | 77% |
| WHERE 子句 | 10 | 1 | 1 | 83% |
| GROUP BY / FILL | 6 | 0 | 0 | 100% |
| 排序分页 | 6 | 0 | 0 | 100% |
| 基础聚合 | 10 | 0 | 1 | 91% |
| 转换函数 | 7 | 0 | 1 | 88% |
| 排名选择 | 3 | 0 | 0 | 100% |
| 高级函数 | 0 | 0 | 2 | 0% |
| 数学函数 | 0 | 0 | 6 | 0% |
| Schema SHOW | 9 | 5 | 6 | 45% |
| 系统 SHOW | 0 | 1 | 8 | 6% |
| 数据库管理 | 6 | 0 | 1 | 86% |
| RP 管理 | 3 | 0 | 0 | 100% |
| CQ 管理 | 3 | 0 | 0 | 100% |
| 用户权限 | 0 | 0 | 8 | 0% |
| Subscription | 0 | 0 | 3 | 0% |
| 时间语法 | 4 | 1 | 0 | 80% |
| Line Protocol | 8 | 0 | 0 | 100% |
| API 端点 | 4 | 1 | 2 | 57% |
| **合计** | **89** | **10** | **41** | **63%** |

---

## 优先级建议

### P0 — 影响已有数据的查询可用性
1. `SHOW MEASUREMENTS WITH MEASUREMENT =~ /.../` 过滤
2. `SHOW TAG VALUES FROM ... WITH KEY = ... WHERE ...` 过滤
3. `DISTINCT` 和 `COUNT(DISTINCT field)`
4. epoch 时间精度解析 (`1234567890s` 格式)

### P1 — 常用查询增强
5. `NON_NEGATIVE_DIFFERENCE` 函数
6. `MODE` 函数
7. 数学函数 (`ABS`, `CEIL`, `FLOOR`, `ROUND`, `SQRT`)
8. `EXPLAIN` 查询计划
9. `SHOW QUERIES` / `KILL QUERY` 查询管理

### P2 — 运维与兼容性
10. `SHOW SHARDS` / `SHOW SHARD GROUPS`
11. `SHOW STATS FOR '...'` 细粒度统计
12. `SHOW DIAGNOSTICS`
13. `CREATE DATABASE ... WITH DURATION ...`
14. `SHOW TAG KEY CARDINALITY` / `SHOW FIELD KEY CARDINALITY`

### P3 — 单节点架构下优先级较低
15. 多用户权限模型 (`CREATE USER`, `GRANT`, `REVOKE`)
16. Subscription 机制
17. `HOLT_WINTERS` / `HISTOGRAM` 高级统计函数
18. 三角函数和对数函数
