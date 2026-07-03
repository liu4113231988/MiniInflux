# Repository Guidelines

## Project Structure & Module Organization

MiniInflux is a .NET 10 time-series database prototype. The main app lives at the repository root, with `Program.cs` wiring HTTP endpoints and services.

- `Model/`: point, field, and query result models.
- `Protocol/`: line protocol parsing and related protocol code.
- `Query/`: InfluxQL parsing and query execution helpers.
- `Storage/`: WAL, segment, manifest, schema, compaction, and engine code.
- `MiniInflux.Tests/`: xUnit tests for parser, storage, query, WAL, and P0/P1/P2 behaviors.
- `scripts/compare-with-influxdb.ps1`: local benchmark comparison against InfluxDB 1.x.
- `wwwroot/` and `web-admin/`: static/admin UI assets.

Avoid committing generated runtime data from `bin/`, `obj/`, `data/`, or `.benchmarks/`.

## Build, Test, and Development Commands

```powershell
dotnet build .\MiniInflux.slnx -nologo --no-restore
```

Builds the solution using already restored packages.

```powershell
dotnet test .\MiniInflux.Tests\MiniInflux.Tests.csproj -nologo --no-restore
```

Runs the xUnit test suite.

```powershell
dotnet run -c Release --no-restore
```

Starts MiniInflux locally in release mode.

```powershell
.\scripts\compare-with-influxdb.ps1 -Points 100000 -BatchSize 5000 -Concurrency 1 -QueryIterations 5
```

Runs the repeatable local performance comparison. Keep benchmark output as evidence, but do not commit generated `.benchmarks/` files unless explicitly requested.

## Coding Style & Naming Conventions

Use idiomatic C# with 4-space indentation. Prefer small, direct methods over new abstractions. Public types and methods use `PascalCase`; locals and private fields use `camelCase` or `_camelCase` following nearby code. Keep Native AOT friendliness in mind: avoid reflection-heavy patterns and unnecessary dependencies.

## Testing Guidelines

Tests use xUnit. Name tests as `MethodOrScenario_Condition_ExpectedResult`, matching existing files such as `LineProtocolParserTests.cs` and `WalTests.cs`. Add one focused test for parser, WAL, storage, or query behavior when changing non-trivial logic. Run the full test command before committing performance or storage changes.

## Commit & Pull Request Guidelines

Recent commits use short imperative messages, for example `Optimize write batch hot path` or `Optimize raw query benchmark path`. Keep commits focused and include tests/docs with the implementation when relevant.

Pull requests should include a brief summary, test results, and benchmark results for performance-sensitive changes. Note any intentional tradeoffs, especially around WAL durability, buffering, query compatibility, or benchmark methodology.

## Security & Configuration Tips

Local settings live in `appsettings.json` and `MiniInfluxOptions.cs`. Do not commit secrets, local data directories, or machine-specific benchmark artifacts. When changing write-path settings such as WAL fsync, flush thresholds, or buffer limits, document the durability/performance tradeoff in the PR.
