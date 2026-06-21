# MiniInflux.Net10

一个使用 .NET 10 / ASP.NET Core Minimal API 实现的 InfluxDB 1.x 兼容子集时序数据库。

## 功能

- `GET /ping`
- `POST /write?db=metrics&precision=ns|u|ms|s|m|h`
- `GET|POST /query?db=metrics&q=...`
- Line Protocol 写入
- InfluxQL 子集：
  - `CREATE DATABASE name`
  - `SHOW DATABASES`
  - `SHOW MEASUREMENTS`
  - `SHOW FIELD KEYS`
  - `SHOW TAG KEYS FROM measurement`
  - `SHOW TAG VALUES FROM measurement WITH KEY = tagKey`
- `SELECT * FROM measurement [WHERE ...] [GROUP BY time(1m)] [ORDER BY time DESC] [LIMIT n]`
  - 聚合函数：`count,sum,mean,min,max,first,last`
- WAL + Segment 存储
- Segment v3 列编码：时间戳 `delta-of-delta/Gorilla`、浮点 `legacy XOR/Gorilla`、整数 delta、bool bit-pack、string 字典
- 自适应浮点压缩策略：在 `legacy_raw`、`legacy_brotli`、`gorilla_raw` 之间按体积/速度折中选择
- 管理 CLI：`benchmark`、`inspect`、`repair`、`compact`、`backup`、`restore`
- Native AOT 友好：无动态代理、JSON Source Generator

## 运行

```bash
dotnet run -c Release --project MiniInflux.Net10.csproj
```

默认监听 `http://0.0.0.0:8086`。数据目录默认是 `./data`，也可以通过 `appsettings.json` 里的 `MiniInflux:DataPath` 或环境变量 `MINI_INFLUX_DATA` 指定。

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
