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
- 时间戳 delta-of-delta、整数 delta、double XOR、bool bit-pack、string 字典编码、Brotli block 压缩
- Native AOT 友好：无动态代理、JSON Source Generator

## 运行

```bash
dotnet run -c Release --project MiniInflux.Net10.csproj
```

## AOT 发布

```bash
dotnet publish -c Release -r linux-x64
# 或
dotnet publish -c Release -r win-x64
```

## 测试

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

## 说明

这是一个完整可运行的工程包，但仍是 InfluxDB 1.x 的兼容子集，不是完整替代品。生产增强建议包括：更完整 InfluxQL、LSM compaction、倒排索引持久化、Retention Policy、权限认证、多节点副本等。
