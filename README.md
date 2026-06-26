# MiniInflux.Net10

一个使用 .NET 10 / ASP.NET Core Minimal API 实现的 InfluxDB 1.x 兼容子集时序数据库。

## 功能

- `GET /ping`
- `POST /write?db=metrics&precision=ns|u|ms|s|m|h`
- `GET|POST /query?db=metrics&q=...`
- Line Protocol 写入
  - 首次向不存在的 `db` 写入时会自动创建数据库和默认 RP `autogen`
- InfluxQL 子集：
  - `CREATE DATABASE name`
  - `SHOW DATABASES`
  - `SHOW MEASUREMENTS`
  - `SHOW FIELD KEYS`
  - `SHOW TAG KEYS FROM measurement`
  - `SHOW TAG VALUES FROM measurement WITH KEY = tagKey`
  - `CREATE CONTINUOUS QUERY ... BEGIN SELECT ... INTO ... END`
  - `SHOW CONTINUOUS QUERIES`
  - `DROP CONTINUOUS QUERY ... ON db`
- `SELECT * FROM measurement [WHERE ...] [GROUP BY time(1m)] [ORDER BY time DESC] [LIMIT n]`
  - 支持 `FROM (<select ...>)` 聚合型子查询第一版
  - 聚合函数：`count,sum,mean,min,max,first,last,spread,stddev,median,percentile`
  - 序列函数：`difference,derivative,non_negative_derivative,moving_average,cumulative_sum,integral,elapsed`
  - 采样/排名函数：`top,bottom,sample`
  - 已支持 `GROUP BY time(...)`、`GROUP BY tag`、`GROUP BY time(...),tag`
  - 已支持 `fill(none|null|previous|linear|zero)`、`ORDER BY time DESC`、`SLIMIT`、`SOFFSET`
- `SELECT ... INTO ...`
  - 支持写回 `measurement`、`db.measurement`、`db.rp.measurement`
- 查询保护与观测
  - chunked query response
  - query count / error / timeout / rows / scanned points / duration buckets
  - `MaxQueryDuration`、`MaxQueryPoints`、`MaxResponseRows`、`MaxQueryMemoryBytes`
- 写入保护
  - `MaxRequestBodyBytes`、`MaxBufferPoints`、`MaxBufferBytes`
- 权限与认证
  - HTTP Basic / query 参数认证
  - 配置文件中的单一超级管理员账号
  - 不支持运行时创建用户、改密或细粒度授权
- WAL + Segment 存储
- Segment v3 列编码：时间戳 `delta-of-delta/Gorilla`、浮点 `legacy XOR/Gorilla`、整数 delta、bool bit-pack、string 字典
- 自适应浮点压缩策略：在 `legacy_raw`、`legacy_brotli`、`gorilla_raw` 之间按体积/速度折中选择
- 管理 CLI：`benchmark`、`inspect`、`repair`、`compact`、`backup`、`restore`
- Native AOT 友好：无动态代理、JSON Source Generator

## 运行

```bash
dotnet run -c Release --project MiniInflux.Net10.csproj
```

默认监听 `http://0.0.0.0:8086`。当前配置同时兼容旧版 `MiniInflux:DataPath`，也支持更接近 InfluxDB 1.x 风格的 `Data`、`Http`、`Logging` 段。

## 近期补齐

- `GROUP BY` 查询已补齐到当前项目范围内的可用状态：`time`、`tag`、`time+tag`、`fill`、`SLIMIT/SOFFSET`、`top/bottom/sample` 组合都已支持。
- `SELECT INTO` 已支持查询结果回写，并保留纳秒级时间戳。
- Continuous Query 已支持基础调度、`RESAMPLE EVERY/FOR`、最近 bucket 受控重算，以及按 CQ 维度导出运行指标。
- 查询执行路径已接入聚合下推、字段范围 predicate skip、regex tag 候选预筛选，以及 segment 内按 `measurement/tag/time` 的列级解压前跳过。
- 首次写入不存在的数据库时，会自动创建数据库和默认 RP `autogen`，行为对齐 InfluxDB 1.x 常见使用方式。

## 配置

默认 `appsettings.json`：

```json
{
  "Data": {
    "Dir": "./data",
    "QueryLogEnabled": true
  },
  "Http": {
    "Enabled": true,
    "BindAddress": "0.0.0.0:8086",
    "AuthEnabled": false,
    "LogEnabled": true,
    "SuppressWriteLog": false,
    "AccessLogPath": "",
    "AccessLogStatusFilters": [],
    "WriteTracing": false
  },
  "Auth": {
    "Enabled": false,
    "Username": "admin",
    "Password": "change-this-password",
    "AuditFailures": true,
    "MaxFailedAttempts": 5,
    "FailureWindowMs": 60000,
    "LockoutMs": 300000
  },
  "Logging": {
    "Level": "Information",
    "ConsoleEnabled": true,
    "FileEnabled": false,
    "FilePath": "./logs/miniinflux.log"
  },
  "ContinuousQuery": {
    "Enabled": true,
    "CheckIntervalMs": 5000,
    "MaxCatchUpRunsPerCycle": 8,
    "RecomputeRecentBuckets": 0,
    "InitialBackfillDuration": "0s"
  }
}
```

设计参考了 InfluxDB 官方配置/日志项思路，但当前只实现了与 MiniInflux 现阶段能力匹配的子集：

- `Data.Dir`：数据目录，优先于旧版 `MiniInflux:DataPath`
- `Data.QueryLogEnabled`：是否记录查询语句
- `Http.Enabled`：是否启用 HTTP 服务；关闭后仍可使用管理 CLI
- `Http.BindAddress`：监听地址，会自动映射成 ASP.NET Core 使用的 `Urls`
- `Auth.Enabled`：认证总开关，启用后 `/write`、`/query`、诊断接口和管理台均要求认证
- `Http.AuthEnabled`：旧版兼容开关；仅在未配置 `Auth.Enabled` 时作为回退
- `Auth.Username`：唯一超级管理员用户名
- `Auth.Password`：唯一超级管理员密码；启用认证时不能为空
- `Auth.AuditFailures`：是否记录认证失败和限流审计日志
- `Auth.MaxFailedAttempts`：单个客户端地址在窗口期内允许的最大失败次数，`0` 表示关闭限流
- `Auth.FailureWindowMs`：失败计数窗口，默认 `60000`
- `Auth.LockoutMs`：触发限流后的锁定时长，默认 `300000`
- `Http.LogEnabled`：是否启用访问日志
- `Http.SuppressWriteLog`：是否抑制 `/write` 请求访问日志
- `Http.AccessLogPath`：访问日志文件路径；留空时写到应用 logger
- `Http.AccessLogStatusFilters`：按状态码筛选访问日志，例如 `["4xx","5xx","503"]`
- `Http.WriteTracing`：记录 `/write` 原始请求体，便于排障，默认应关闭
- `Logging.Level`：应用日志级别，支持 `Trace`、`Debug`、`Information`、`Warning`、`Error`、`Critical`
- `Logging.ConsoleEnabled`：是否输出到控制台
- `Logging.FileEnabled`：是否输出到文件
- `Logging.FilePath`：应用日志文件路径
- `ContinuousQuery.Enabled`：是否启用 Continuous Query 后台调度器
- `ContinuousQuery.CheckIntervalMs`：调度扫描周期
- `ContinuousQuery.MaxCatchUpRunsPerCycle`：单次扫描最多补跑多少个 bucket
- `ContinuousQuery.RecomputeRecentBuckets`：每轮额外重算最近多少个已关闭 bucket，默认 `0` 关闭
- `ContinuousQuery.InitialBackfillDuration`：当 CQ 未显式声明 `RESAMPLE FOR` 时，首次创建或长时间停机恢复后默认允许补跑的窗口

环境变量兼容：

- `MINI_INFLUX_DATA` 可继续覆盖数据目录

## 单一超级管理员认证

MiniInflux 使用配置驱动的单一超级管理员模型，不维护数据库内用户表，也不读取数据目录中的旧 `meta/auth.json`。

启用认证：

```json
{
  "Auth": {
    "Enabled": true,
    "Username": "admin",
    "Password": "replace-with-a-strong-password"
  }
}
```

启用后，`/write`、`/query`、诊断接口和 `/admin` 管理台均使用同一账号认证。支持 InfluxDB 1.x 客户端常用的两种传递方式：

```bash
curl -u admin:replace-with-a-strong-password \
  "http://localhost:8086/query?q=SHOW%20DATABASES"

curl -G http://localhost:8086/query \
  --data-urlencode "u=admin" \
  --data-urlencode "p=replace-with-a-strong-password" \
  --data-urlencode "q=SHOW DATABASES"
```

账号或密码修改后需要重启服务。`CREATE USER`、`ALTER USER`、`DROP USER`、`GRANT`、`REVOKE`、`SHOW USERS` 和 `SHOW GRANTS` 不受支持。

当同一客户端地址在 `Auth.FailureWindowMs` 窗口内连续失败达到 `Auth.MaxFailedAttempts` 后，系统会在 `Auth.LockoutMs` 内拒绝新的认证尝试，并在日志中记录失败与锁定事件。管理台登录页会直接提示剩余等待时间。

生产环境建议使用 ASP.NET Core 标准环境变量覆盖明文配置：

```text
Auth__Enabled=true
Auth__Username=admin
Auth__Password=<strong-password>
```

## 日志示例

只输出到控制台：

```json
"Logging": {
  "Level": "Information",
  "ConsoleEnabled": true,
  "FileEnabled": false,
  "FilePath": "./logs/miniinflux.log"
}
```

同时输出到文件：

```json
"Logging": {
  "Level": "Debug",
  "ConsoleEnabled": true,
  "FileEnabled": true,
  "FilePath": "./logs/miniinflux.log"
}
```

只记录异常访问请求：

```json
"Http": {
  "LogEnabled": true,
  "SuppressWriteLog": true,
  "AccessLogPath": "./logs/access.log",
  "AccessLogStatusFilters": ["4xx", "5xx", "503"]
}
```

说明：

- 应用日志和访问日志是分开的；`Logging.*` 控制应用 logger，`Http.*` 控制 HTTP 访问日志
- `SuppressWriteLog=true` 适合高吞吐写入场景，避免 `/write` 访问日志过大
- `WriteTracing=true` 会记录原始写入内容，只建议在短时排障时开启
- `QueryLogEnabled=true` 会记录查询语句，便于排查查询行为和性能问题

## AOT 发布

```bash
dotnet publish -c Release -r linux-x64
# 或
dotnet publish -c Release -r win-x64
```

## 基本使用

```bash
curl -G http://localhost:8086/query --data-urlencode "q=CREATE DATABASE metrics"

curl -i -XPOST "http://localhost:8086/write?db=metrics&precision=ns" \
  --data-binary "cpu,host=s1,region=cn value=0.64,temp=42i,ok=true 1710000000000000000"

curl -G http://localhost:8086/query \
  --data-urlencode "db=metrics" \
  --data-urlencode "q=SELECT * FROM cpu LIMIT 10"

curl -G http://localhost:8086/query \
  --data-urlencode "db=metrics" \
  --data-urlencode "q=SELECT mean(value),max(temp) FROM cpu GROUP BY time(1m)"
```

说明：

- 如果 `metrics` 还不存在，上面的第一次 `/write?db=metrics` 会自动创建数据库和默认 RP `autogen`。
- 也仍然支持显式执行 `CREATE DATABASE metrics`，便于与 InfluxDB 1.x 的常见运维流程保持一致。

## Continuous Query

支持：

- `CREATE CONTINUOUS QUERY`
- `SHOW CONTINUOUS QUERIES`
- `DROP CONTINUOUS QUERY`
- 后台按 `GROUP BY time(...)` 周期调度
- `SELECT ... INTO ...` 结果自动回写

示例：

```sql
CREATE CONTINUOUS QUERY cq_cpu_1m ON metrics
BEGIN
  SELECT mean(value) INTO cpu_1m FROM cpu GROUP BY time(1m),host
END
```

也支持显式指定调度周期：

```sql
CREATE CONTINUOUS QUERY cq_cpu_10s ON metrics
RESAMPLE EVERY 10s
BEGIN
  SELECT max(value) INTO cpu_10s FROM cpu GROUP BY time(10s),host
END
```

也支持基础窗口补跑语义：

```sql
CREATE CONTINUOUS QUERY cq_cpu_10s ON metrics
RESAMPLE EVERY 10s FOR 30s
BEGIN
  SELECT max(value) INTO cpu_10s FROM cpu GROUP BY time(10s),host
END
```

也支持单个 CQ 的独立重算策略：

```sql
CREATE CONTINUOUS QUERY cq_cpu_10s ON metrics
RESAMPLE EVERY 10s FOR 30s RECOMPUTE 2
BEGIN
  SELECT max(value) INTO cpu_10s FROM cpu GROUP BY time(10s),host
END
```

查看与删除：

```sql
SHOW CONTINUOUS QUERIES
DROP CONTINUOUS QUERY cq_cpu_1m ON metrics
```

当前限制：

- CQ body 需要是 `SELECT ... INTO ...`
- CQ body 需要包含 `GROUP BY time(...)`
- 已支持基础 `RESAMPLE FOR` 窗口，但还不是完整 InfluxDB 全语义
- 未显式声明 `FOR` 时，会回退到 `ContinuousQuery.InitialBackfillDuration`
- 可通过 `ContinuousQuery.RecomputeRecentBuckets` 对最近已关闭 bucket 做受控重算
- 已暴露 CQ 运行指标到 `/debug/stats` 与 `/metrics`
- 单个 CQ 也可通过 `RESAMPLE ... RECOMPUTE n` 覆盖全局重算策略
- `/metrics` 已支持按 `db`、`name` label 导出 CQ 明细指标

## 管理 CLI

```bash
dotnet run -- benchmark --points 10000 --concurrency 1 --format text
dotnet run -- inspect wal --data ./data
dotnet run -- inspect segment --path ./data/db/metrics/autogen/shards/000001/xxx.seg
dotnet run -- repair --data ./data
dotnet run -- compact --data ./data
dotnet run -- backup --data ./data --path ./backup
dotnet run -- restore --data ./data --path ./backup
```

说明：

- `benchmark` 会输出写入、查询、恢复、compaction 的基线指标。
- `benchmark` 还会输出 `CodecComparison` 与 `FloatStrategyBenchmarks`，用于比较 `legacy` 和 `Gorilla` 在规则时间序列上的体积、编码耗时、解码耗时，以及 `adaptive` 策略在不同 workload 下的最终选择。
- `inspect wal` 用于查看 WAL 文件数量、checkpoint 位置和可回放记录数。
- `inspect segment` 用于查看 segment 元数据、列数、时间范围、字段统计和 codec 选择。
- `repair` 会离线执行恢复流程并刷新 WAL/内存状态。
- `compact` 会离线触发 compaction。
- `backup` / `restore` 使用带元数据校验的备份恢复流程。

## Benchmark 输出重点

`benchmark --format json` 里与压缩策略相关的关键字段：

- `CodecComparison.Legacy` / `CodecComparison.Gorilla`
- `FloatStrategyBenchmarks[].BestBySize`
- `FloatStrategyBenchmarks[].BestByEncode`
- `FloatStrategyBenchmarks[].BestByDecode`
- `FloatStrategyBenchmarks[].Strategies[]`

当前经验解释：

- `smooth_linear` 往往更偏向 `legacy_brotli`
- `repeating_plateau` 往往更偏向 `gorilla_raw`
- `noisy_sine` 这类高波动序列通常更偏向 `legacy_raw`

## 测试

```bash
dotnet build .\MiniInflux.slnx -nologo --no-restore
dotnet test .\MiniInflux.Tests\MiniInflux.Tests.csproj -nologo --no-restore
```

## 说明

这是一个完整可运行的工程包，但仍是 InfluxDB 1.x 的兼容子集，不是完整替代品。生产增强建议包括：更完整 InfluxQL、LSM compaction、倒排索引持久化、Retention Policy、权限认证、多节点副本等。
