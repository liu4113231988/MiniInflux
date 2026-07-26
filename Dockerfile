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

ENV MINI_INFLUX_DATA=/app/data
EXPOSE 8086
VOLUME ["/app/data"]

COPY --from=build /app/publish/ ./
ENTRYPOINT ["./MiniInflux"]
