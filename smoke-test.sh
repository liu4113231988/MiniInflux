#!/usr/bin/env bash
set -e
curl -G http://localhost:8086/query --data-urlencode "q=CREATE DATABASE metrics"
curl -i -XPOST "http://localhost:8086/write?db=metrics&precision=ns" --data-binary "cpu,host=s1,region=cn value=0.64,temp=42i,ok=true 1710000000000000000"
curl -G http://localhost:8086/query --data-urlencode "db=metrics" --data-urlencode "q=SELECT * FROM cpu LIMIT 10"
