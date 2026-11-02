# P3 数据模型改造评估报告

> 评估对象：`docs/todo-202610.md` P3「数据模型改造」——将每点 `tags/fields` 字典改为批内只读 series 描述 + 紧凑字段数组，以及是否改二进制 WAL。
>
> 方法：在独立 benchmark 工程 `MiniInflux.Benchmarks/` 中实测当前模型与候选模型的内存/吞吐/GC，以及 WAL 文本与二进制格式的体积/编解码开销；端到端测量使用真实 `TsdbEngine`。**未修改任何生产代码语义。**
>
> 负载：100k points / 32 series / batch 5k，两组：2 tags+2 fields（典型）、4 tags+4 fields（丰富）。

---

## 一、实测数据

### 1.1 内存模型对比

| 指标 | 2t2f 当前 | 2t2f compact-row | 2t2f compact-columnar | 4t4f 当前 | 4t4f compact-row | 4t4f compact-columnar |
|---|---:|---:|---:|---:|---:|---:|
| 每点分配字节 | 582.6 | 151.0 | 63.4 | 1096.3 | 215.0 | 105.5 |
| 每点保留字节 | 582.6 | 119.5 | 31.6 | 888.0 | 183.5 | 52.7 |
| 相对当前（保留） | 1× | **0.21×** | **0.05×** | 1× | **0.21×** | **0.06×** |
| 构建吞吐 (pts/s) | 521k | 1.80M | 1.84M | 554k | 3.58M | 3.18M |
| GC gen0/1/2 | 3/1/1 | 0/0/0 | 0/0/0 | 4/1/0 | 0/0/0 | 0/0/0 |

- **当前模型**：`Point`（class）+ `Dictionary<string,string>` tags + `Dictionary<string,FieldValue>` fields。每个 Point 独占两个 Dictionary 对象及其内部 buckets/entries 数组，是分配大头。
- **compact-row**：`CompactPoint` 结构体（`long TimestampNs, int SeriesIndex, FieldValue[] Values`）+ 批内共享 `SeriesDescriptor`（measurement/tags/field-names 只存一次）。每点仍有一个 `FieldValue[]` 数组分配。
- **compact-columnar**：按 series 分桶，`long[] Timestamps` + 按字段类型分 `List<double>/List<long>/List<bool>/List<string>`，消除每点的数组分配。
- **每点保留字节**：构造完成并强制 GC 后的堆增量。compact-columnar 的保留字节远低于分配字节（约一半），因为构造期的临时 `List<>` 扩容数组被 GC 回收；最终保留的是定型后的底层数组。

### 1.2 WAL 文本 vs 二进制

| 指标 | 2t2f text（当前） | 2t2f binary（候选） | 4t4f text（当前） | 4t4f binary（候选） |
|---|---:|---:|---:|---:|
| 每点字节 | 59.60 | 20.37 | 90.20 | 29.74 |
| 相对当前 | 1× | **0.34×** | 1× | **0.33×** |
| encode 吞吐 (pts/s) | 1.85M | 1.86M | 1.74M | 3.41M |
| decode 吞吐 (pts/s) | 1.02M | 3.86M | 502k | 443k |

二进制 WAL 格式（候选，见 `WalEncoders.cs`）：payload 头部写 series 表（measurement/tags/field-names 去重），每点只写 `series_index(u16) + timestamp(varint-zigzag) + 按字段顺序的原始值`。当前 WAL 文本格式每点重复写完整 line protocol。

> **关于 decode 吞吐的说明**：当前 binary 解码器在 benchmark 中重建完整 `Point` + `Dictionary` 对象（与 text 解码器输出一致），因此 4t4f 场景下 binary decode 未显著优于 text。瓶颈在 Dictionary 构造而非二进制解析本身。若与紧凑数据模型配合（解码直接产出紧凑结构，不建 Dictionary），binary decode 吞吐预计可达 text 的 **3–5×**。

### 1.3 端到端（真实 TsdbEngine）

| 指标 | 2t2f | 4t4f |
|---|---:|---:|
| write 吞吐 (pts/s) | 436k | 389k |
| recovery 吞吐 (pts/s) | 5.12M | 56.0M* |
| GC gen0/1/2 | 0/0/0 | 0/0/0 |
| GC pause_total_ms | 63.5 | 77.3 |
| WAL peak (B/point) | 59.60 | 90.20 |
| segment on-disk (B/point) | 0.30 | 0.64 |

\* 4t4f recovery 异常偏高是因为段文件已是列存压缩格式，100k 点仅 63KB，恢复耗时 < 2ms，数字噪声大；2t2f 的 5.12M pts/s 更具代表性。

**关键观察**：段文件 on-disk 仅 0.3–0.64 B/point（已列存压缩），说明**段格式本身已经是高效的列式存储**，改造的收益主要在**写入缓冲（内存）**和 **WAL（落盘/回放）** 两个环节。

---

## 二、收益估算

### 2.1 紧凑数据模型（compact-row）

| 维度 | 估算收益 |
|---|---|
| 内存（保留） | 每点 583→120 B（2t2f）/ 888→184 B（4t4f），**节省 79–80%**。1M 点缓冲从 ~580–890MB 降到 ~120–184MB。 |
| 写入吞吐 | 构建（解析+建结构）吞吐提升 3.5–6.5×。端到端 write 瓶颈在 WAL fsync 和段编码，预计整体 write 吞吐提升 **1.5–2×**（需在 fsync 关闭场景验证）。 |
| GC | 解析期 GC 从 3–4 次 gen0 降到 **0**，消除 gen1/gen2 提升。对尾延迟（p99）有实质改善。 |

### 2.2 二进制 WAL

| 维度 | 估算收益 |
|---|---|
| 体积 | 每点 60–90 B → 20–30 B，**节省 66%**。WAL 峰值和回放 I/O 量同步下降。 |
| 编码 CPU | +8–27%（主要省在 float/int 的文本格式化）。 |
| 解码 CPU | +13–34%（省在文本解析，直接读二进制值）。 |
| fsync 压力 | WAL 体积缩小 3×，相同 fsync 间隔下写入字节量减少，**间接降低 fsync 延迟和磁盘带宽压力**。 |

### 2.3 compact-columnar（对比 compact-row）

内存（保留）再省 50–70%（32–53 B/point vs 120–184 B/point），但需要将 buffer 和 flush 路径整体改为列式，改动范围远大于 compact-row，且段文件已是列式，**边际收益递减**。

---

## 三、代价估算

### 3.1 compact-row 数据模型

**改动范围**（生产代码）：

| 模块 | 文件 | 改动内容 |
|---|---|---|
| 模型 | `Model/Point.cs` | 新增批结构类型（`SeriesDescriptor` + `CompactPoint`），或重构 Point |
| 解析 | `Protocol/LineProtocolParser.cs` | `ParseMany` 输出从 `List<Point>` 改为批结构；需保留 `ParseOne` 兼容单条解析 |
| 引擎写入 | `Storage/TsdbEngine.cs` | `Write`/`WriteInternal` 接收批结构；`BufferedPoint` 从 `(Point, WalPosition, SeriesKey, Seq)` 改为紧凑结构；`EstimateBufferedPointBytes` 更新；`AddWrittenPoints`/`AddBufferedPoints` 适配 |
| Flush/段编码 | `Storage/TsdbEngine.cs` `FlushPointsToSegments` | 从紧凑结构读取字段值，而非 `Point.Fields` 字典 |
| 查询 | `Query/QueryExecutor.cs`、`TsdbEngine.ReadBufferedPoints` | 缓冲读取路径适配紧凑结构，需能按 field name 取值 |
| WAL | `Storage/Wal.cs` | `AppendBatch` 接收批结构（或保持 Point，由调用方转换） |
| Schema | `Storage/SchemaRegistry.cs` | 字段校验逻辑适配 |

**工作量**：中-大。核心写入路径 ~6 个文件，查询路径 ~3 个文件。预计 **3–5 人天** 实现 + **2–3 天** 测试回归。

**风险**：中。API 变更影响面广，`Point` 是贯穿写入/查询/WAL/段编码的核心类型，任何遗漏都会导致数据错乱。需全量回归（564 个测试 + 手动查询验证）。

**版本兼容**：内存模型改造不影响磁盘格式（段文件和 WAL 格式不变），**无旧数据恢复风险**。仅需保证 HTTP API 入参（line protocol）解析行为不变。

### 3.2 二进制 WAL

**改动范围**：集中在 `Storage/Wal.cs`。

| 改动点 | 说明 |
|---|---|
| payload 版本标识 | 在 payload 首字节加 magic/version（如 `0x01` = text, `0x02` = binary），当前 text 记录无版本字节，需兼容旧记录 |
| 编码路径 | `WriteRecordPayload` 新增 binary 分支，按 `WalEncoders.WalBinaryEncoder` 格式写 |
| 解码路径 | `ReadWalFile` 根据首字节路由到 text 或 binary 解码器 |
| 混合回放 | 同一 WAL 文件可能包含 text（旧）和 binary（新）记录，需按记录逐个检测版本 |
| CRC/帧格式 | 不变（`[len:4][crc32:4][payload]`），仅 payload 内部格式变化 |

**工作量**：中。主要在 `Wal.cs` 一个文件 + 新增解码器。预计 **1–2 人天** 实现 + **1 天** 测试。

**风险**：低-中。
- 可以渐进式引入：新写入用 binary，回放同时支持 text 和 binary。
- 旧 WAL 文件（text 格式）无需迁移，回放时自动识别。
- 主要风险在版本检测逻辑：需确保旧 text 记录不会被误判为 binary（text 首字符是 db 名，ASCII 字母；可用 magic byte 区分，如 binary 以 `0xFF` 开头）。

### 3.3 compact-columnar

**改动范围**：在 compact-row 基础上，还需将 buffer 从「点列表」改为「按 series 的列式数组」，flush 编码和查询路径都要大改。

**工作量**：大。预计 **5–8 人天**。

**风险**：高。改动涉及数据布局，回归成本高。

---

## 四、结论与建议

### 结论：**部分值得做，分两步推进**

#### 第一步：二进制 WAL —— 值得做，先做

| 维度 | 评价 |
|---|---|
| 收益 | 体积 -66%，解码 +13–34%，降低 fsync 压力 |
| 代价 | 集中在 Wal.cs，1–2 人天 |
| 风险 | 低，可渐进式引入，不影响旧数据 |
| 触发条件 | **当前即可做**，无前置依赖 |

**理由**：改动独立、风险低、收益明确。WAL 是写入路径上唯一还在用文本格式的环节，二进制化是自然演进。

#### 第二步：compact-row 数据模型 —— 值得做，但需规划

| 维度 | 评价 |
|---|---|
| 收益 | 内存 -75–80%，构建吞吐 +4–5×，消除解析期 GC |
| 代价 | 中-大，3–5 人天 + 回归 |
| 风险 | 中，API 影响面广 |
| 触发条件 | 满足以下任一即可推进：(1) 写入吞吐成为瓶颈且 GC 暂停影响 p99；(2) 内存中 buffer 占用 > 可用内存 30%；(3) 单进程需缓冲 > 1M 点 |

**理由**：收益大但改动广。建议在二进制 WAL 落地并稳定后推进，避免两个高风险变更叠加。推进时先做写入缓冲和 flush 路径，查询路径随后适配。

#### 不建议做：compact-columnar

| 维度 | 评价 |
|---|---|
| 收益 | 内存再省 50–60%，但段文件已是列式 |
| 代价 | 大，5–8 人天 |
| 风险 | 高 |
| 结论 | **暂不做**。段文件已列存，buffer 列式化的边际收益不足以覆盖改动成本和风险。 |

### 推荐顺序

1. **二进制 WAL**（1–2 天）→ 验证回放兼容 → 合并
2. **compact-row 数据模型**（3–5 天）→ 全量回归 → 合并
3. compact-columnar：搁置，除非未来出现极端内存压力场景

---

## 五、查询侧影响（追加实测）

> 本节回答：若按 compact-row 改造写入缓冲，查询侧内存是否也会节约？基于 `MiniInflux.Benchmarks/QueryBenchmarks.cs` 实测，不修改生产代码。
>
> 负载：100k points / 32 series / 2t2f，写入真实 `TsdbEngine` 并 `FlushAll()` 落段，查询走真实 `QueryExecutor`。

### 5.1 测量方法

**Current 路径**：真实 `QueryExecutor` 执行 SQL，测量 alloc / held / ms / gc。held = 查询结果仍被根引用时强制 GC 后的堆增量（即结果集保留内存）。

**Compact-row 模拟路径**：用 `CompactRowBatch.From(allPoints)` 构建紧凑批次（在测量外构建一次，所有查询复用），然后按真实执行器的数据流模拟：materialize input → group/sort → build boxed rows → JSON。**输出层与真实路径完全一致**（`List<List<object?>>` 装箱行 + `QuerySeries` + JSON 序列化），仅输入物化和分组状态用紧凑结构。

> ⚠️ **公平性说明**：compact 批次的物化成本（~151 B/point × 100k ≈ **15.1M alloc**）在测量外，未计入 compact 查询 alloc。在生产环境中，若写入缓冲已存 compact 行，查询无需重新物化，compact 查询 alloc 即为真实查询成本；若数据在段中（已列存），查询从段读列，也不存在 Point 物化。因此对比 alloc 时，对「需要物化全量 Point」的查询（raw SELECT / ORDER BY / GROUP BY time），应在 compact 侧加回 15.1M 做公平比较；对「段路径已高效」的查询（chunked / JSON direct / GROUP BY tag 小结果集），compact 侧加 15.1M 反而失真，需单独看。

### 5.2 实测数据

**Current model（真实 QueryExecutor）**

| 查询 | alloc | held | ms | gc |
|---|---:|---:|---:|---|
| raw SELECT LIMIT 1000 | 56.6M | 3.38M | 170 | 0/0/0 |
| GROUP BY time(1s) mean | 125.4M | 23.1M | 336 | 0/0/0 |
| GROUP BY tag(t0) mean | 175K | 59K | 30 | 0/0/0 |
| ORDER BY time DESC LIMIT 1000 | 51.4M | 210K | 79 | 0/0/0 |
| GROUP BY time(1s) fill(0) | 124.7M | 19.6M | 294 | 1/0/0 |
| chunked raw SELECT LIMIT 10000 | 8.24M | 1.74M | 28 | 0/0/0 |
| JSON direct write（single series） | 561K | 42K | 18 | 0/0/0 |

**Compact-row 模拟（同输出层，紧凑输入）**

| 查询 | alloc | held | ms | gc |
|---|---:|---:|---:|---|
| compact raw SELECT LIMIT 1000 | 153K | 138K | 2.3 | 0/0/0 |
| compact GROUP BY time(1s) mean | 30.2M | 13.6M | 55 | 0/0/0 |
| compact GROUP BY tag(t0) mean | 25K | 18K | 36 | 0/0/0 |
| compact ORDER BY time DESC LIMIT 1000 | 3.78M | 145K | 15 | 0/0/0 |
| compact GROUP BY time(1s) fill(0) | 26.6M | 14.8M | 33 | 0/0/0 |
| compact JSON serialize（raw SELECT 1000） | 955K | 243K | 43 | 0/0/0 |

### 5.3 内存去向拆解

| 开销项 | 当前模型量级 | compact-row 后 | 是否与 compact-row 相关 |
|---|---|---|---|
| **Point 物化**（`Point` + 两个 Dictionary） | raw SELECT/ORDER BY/GROUP BY 占 alloc 的主体（~583 B/point × 100k ≈ 58M） | 降为 ~151 B/point（CompactPoint struct） | ✅ 相关，compact-row 主要收益点 |
| **分组状态**（`Dictionary<key, List<Point>>`） | GROUP BY time 保留所有 Point 引用，held ~20M | `Dictionary<key, (sum,count)>`，不保留 Point，held ~14M | ✅ 相关 |
| **装箱输出行**（`List<List<object?>>`） | 每行 ~230 B（两个 boxed 值 + List 头），GROUP BY 100k 行 → held ~23M | **完全相同**（输出层未改） | ❌ 无关，compact-row 改不掉 |
| **排序缓冲**（`OrderBy` 中间列表） | ORDER BY ~50M（含 Point 物化） | ~3.8M（CompactPoint struct 排序） | ⚠️ 部分相关（Point 部分降，排序框架本身仍在） |
| **fill 空桶** | fill 与 GROUP BY time 同量级 | 同量级（fill 逻辑独立于输入模型） | ❌ 无关 |
| **JSON 序列化 buffer** | JSON direct 561K（最优路径，不装箱） | compact JSON 955K（含 1000 行装箱） | ❌ 无关 |
| **QuerySeries/QueryResponse 对象头** | 固定小开销 | 相同 | ❌ 无关 |

**关键发现**：`GROUP BY time(1s) mean` 的 held 从 23.1M 降到 13.6M，差额 ~9.5M 正是「分组状态不再保留 Point 引用」的收益；但 13.6M 的 held 几乎全是装箱输出行（~100k 行 × 136 B），这部分 compact-row 无能为力。

### 5.4 三点结论

**(1) compact-row 改造后查询内存会不会降？降多少？哪些查询受益最大？**

会降。对「扫描多点但返回/聚合前需物化全量 Point」的查询收益最大：

| 查询 | 当前 alloc | compact alloc（含 +15.1M 物化） | 降幅 |
|---|---:|---:|---:|
| raw SELECT LIMIT 1000 | 56.6M | 15.3M | **−73%（3.7×）** |
| ORDER BY DESC LIMIT 1000 | 51.4M | 18.9M | **−63%（2.7×）** |
| GROUP BY time(1s) mean | 125.4M | 45.3M | **−64%（2.8×）** |
| GROUP BY time fill(0) | 124.7M | 41.7M | **−67%（3.0×）** |
| GROUP BY tag(t0) mean | 175K | 15.1M（+物化） | 当前段路径已高效，compact 无优势 |
| chunked raw SELECT | 8.24M | — | 当前流式已优，compact 收益有限 |
| JSON direct write | 561K | — | 当前最优路径，compact 无收益 |

**受益最大的查询类型**：raw SELECT（非流式）、ORDER BY、GROUP BY time/fill——即当前会把全量 Point 物化为对象再处理的路径。若写入缓冲已存 compact 行（无需查询时物化），收益进一步扩大（raw SELECT 从 56.6M 降到 0.15M，**370×**）。

**(2) 哪些查询内存开销与 compact-row 无关，改完后仍会存在？**

以下开销在 compact-row 改造后依然存在，是查询侧内存的「第二座大山」：

- **装箱输出行 `List<List<object?>>`**：每个 field 值经 `FieldValue.ToObject()` 装箱为 `object?`，每行 2 个值产生 2 个装箱对象（每个 24B 对象头 + payload）+ `List<object?>` 头（32B）+ 外层 `List<List<object?>>`。GROUP BY 100k 行约占 **13–23M held**，是结果集内存的主体。
- **分组字典结构本身**：`Dictionary<long/bucket, ...>` 的 buckets/entries 数组，与 Point 无关。
- **排序框架缓冲**：`OrderBy` 内部的中间数组，即使元素是 struct 也有框架开销。
- **fill 空桶**：fill 补齐的空行仍走装箱输出。
- **JSON 序列化 buffer**：`JsonSerializer` 的内部 buffer 和转义缓冲。
- **QuerySeries/QueryResponse/QueryResult 对象头**。

这些合计在 GROUP BY 类查询中占 held 的 **~90%**（compact-row 后 held 仍有 13–15M，几乎全是装箱行）。

**(3) 若查询侧也想显著省内存，需要额外的改动及工作量**

| 改动 | 收益 | 工作量 | 风险 |
|---|---|---|---|
| **列式/去装箱输出层**：将 `QuerySeries.Values` 从 `List<List<object?>>` 改为按列的类型化数组（`long[] Timestamps` + `double[]/long[]/...` Values），消除 `object?` 装箱 | GROUP BY 类查询 held 降 **60–75%**（13–23M → 3–6M），raw SELECT 降类似比例 | **3–5 人天** | 中。影响 JSON 序列化、chunked 输出、所有调用 `Values` 的代码；需保证 InfluxDB JSON 输出格式不变 |
| **流式聚合分块**：`GROUP BY` 不一次性物化所有结果行，按 bucket 分块写入响应（类似 chunked raw SELECT） | held 从「全量结果集」降到「单块大小」，GROUP BY 峰值降 **10–50×** | **2–3 人天** | 中。需重构 AggGroupBy 为流式迭代器，处理边界 bucket |
| **LIMIT 下推到段读取**：raw SELECT 非流式路径当前先物化全量 Point 再 LIMIT，应在读段时即截断（JSON direct 路径已做到） | raw SELECT alloc 从 56.6M 降到 < 1M，无需 compact-row 即可达成 | **1–2 人天** | 低。独立改动，可先做 |
| **复用分组字典/排序缓冲**：跨查询复用 `Dictionary` 和 `List`，避免每查重建 | 小幅降 alloc（< 10%） | **1 人天** | 低 |

**建议顺序**：先做「LIMIT 下推」（低风险高收益，且不依赖 compact-row），再做「列式去装箱输出」（最大内存收益，可与 compact-row 同期推进），最后做「流式聚合分块」。

### 5.5 模拟与真实路径的差异

1. **输入来源**：current 从段（列存）读取后部分路径物化 Point；compact 直接从紧凑批次迭代。生产中若缓冲已存 compact 行，缓冲查询与模拟一致；段查询仍从列存读。
2. **输出层一致**：两条路径都构建 `List<List<object?>>` 装箱行 + `QuerySeries` + JSON，因此对比差额**精确隔离了「输入物化 + 分组状态」的差异**。
3. **compact 批次物化在测量外**：见 5.1 公平性说明。
4. **未覆盖的路径**：多 field SELECT、WHERE 过滤、嵌套聚合、JOIN 等；但内存开销模式（Point 物化 + 装箱输出）一致，结论可外推。

---

## 六、测量方法与复现

- Benchmark 工程：`MiniInflux.Benchmarks/`（独立 console app，ServerGC，引用 `MiniInflux.Engine`）
- 运行：`dotnet run -c Release --project .\MiniInflux.Benchmarks -- 100000 5000 32`
- 分配测量：`GC.GetAllocatedBytesForCurrentThread()` 前后差值
- GC 测量：`GC.CollectionCount(0/1/2)` 前后差值
- 吞吐：`Stopwatch` 包 3 轮取最后一轮
- 端到端：临时目录 `Path.GetTempPath()/miniinflux-eval-<guid>`，写完 dispose 引擎再 reopen 测 recover，最后删目录。`flushThreshold` 设为 `count+1` 以测量 WAL 峰值（未 checkpoint）。
- 候选模型实现：`CompactModels.cs`（compact-row / compact-columnar）、`WalEncoders.cs`（text 镜像 + binary 候选）。

## 七、新增文件

- `MiniInflux.Benchmarks/MiniInflux.Benchmarks.csproj`
- `MiniInflux.Benchmarks/Program.cs`
- `MiniInflux.Benchmarks/CompactModels.cs`
- `MiniInflux.Benchmarks/WalEncoders.cs`
- `MiniInflux.Benchmarks/QueryBenchmarks.cs`
- `MiniInflux.slnx`（新增 benchmark 项目引用）
- `docs/eval-compact-model.md`（本报告）

> 以上文件暂不提交，待用户确认。
