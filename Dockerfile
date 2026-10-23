# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY NuGet.Config MiniInflux.slnx ./
COPY MiniInflux/MiniInflux.csproj MiniInflux/
RUN dotnet restore MiniInflux/MiniInflux.csproj -r linux-x64

COPY MiniInflux/ MiniInflux/
RUN dotnet publish MiniInflux/MiniInflux.csproj \
    -c Release \
    -r linux-x64 \
    --self-contained true \
    --no-restore \
    -o /app/publish

FROM mcr.microsoft.com/dotnet/runtime-deps:10.0
WORKDIR /app

# curl is required for HEALTHCHECK against /health
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --gecos '' appuser \
    && chown -R appuser:appuser /app

ENV MINI_INFLUX_DATA=/app/data
ENV DOTNET_RUNNING_IN_CONTAINER=true
ENV DOTNET_ENVIRONMENT=Production

EXPOSE 8086 8087
VOLUME ["/app/data"]

COPY --from=build --chown=appuser:appuser /app/publish/ ./

USER appuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:8086/health || exit 1

ENTRYPOINT ["./MiniInflux"]
