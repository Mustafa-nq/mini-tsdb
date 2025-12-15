# mini-tsdb

A small in-memory time-series database written in Go. Built to demonstrate Echo API integration and concurrency (sharded storage, background compaction).

## Features

- POST `/metrics` to ingest samples
- GET `/query?metric=<name>&from=<epoch-ms>&to=<epoch-ms>` to retrieve samples
- Sharded in-memory series (configurable shards)
- Background retention job
- Graceful shutdown

## Writing Data
 POST /metrics
`json
{
  "name": "cpu_usage",
  "value": 0.82
}
- What happens internally?
- Echo handler receives the request
- Timestamp is assigned (or accepted from request)
- TSDB hashes the metric name
- Hash decides which shard the metric belongs to
- Shard finds or creates the series
- Sample is appended to memory
- Append → hashKey → shardForKey → shard.Append → series.Append

## Reading Data
- GET /query?metric=cpu_usage&from=1700000000000&to=1700003600000
- What happens internally?
- Query handler validates input
- TSDB hashes metric name
- Correct shard is selected
- Series samples are scanned
- Samples within time range are returned



For simplicity, querying uses a linear scan per series, which is acceptable for a demo TSDB.
## Run

```bash
go run ./cmd/server