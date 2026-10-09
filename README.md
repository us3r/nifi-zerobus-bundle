# NiFi Zerobus Ingest Bundle

Apache NiFi processor for streaming data into Databricks Delta tables via [Zerobus Ingest](https://docs.databricks.com/aws/en/ingestion/zerobus-overview).

## What it does

**PutZerobusIngest** opens a persistent gRPC stream to Databricks Zerobus and pushes JSON FlowFile content directly into a Delta table. No Kafka, no staging files, no intermediate hops.

Features:

- **Persistent gRPC stream** — opened once at processor start, reused across all FlowFiles
- **Batch ingestion** — configurable batch size with offset-based acknowledgment
- **Auto-recovery** — reconnects on transient failures without losing data
- **Three-way routing** — `success` / `failure` (non-retriable) / `retry` (transient)
- **Native performance** — uses the official Databricks Java SDK (JNI → Rust backend)

## Requirements

- Apache NiFi 2.12.0 (for NiFi 1.x use the `main` branch)
- Java 21+
- Databricks workspace with Zerobus Ingest enabled
- Service principal with `MODIFY` + `SELECT` on the target table

## Build

```bash
mvn clean package -DskipTests
```

The NAR file will be at `nifi-zerobus-nar/target/nifi-zerobus-nar-2.12.0-2.nar`.

## Install

Copy the NAR to NiFi's `lib/` directory and restart:

```bash
cp nifi-zerobus-nar/target/nifi-zerobus-nar-2.12.0-2.nar $NIFI_HOME/lib/
$NIFI_HOME/bin/nifi.sh restart
```

On Kubernetes (recommended — bake into image):

```dockerfile
FROM apache/nifi:2.12.0
COPY nifi-zerobus-nar-2.12.0-2.nar /opt/nifi/nifi-current/lib/
```

```bash
docker build -t nifi-zerobus:2.12.0 .
kubectl -n <namespace> set image deployment/nifi nifi=nifi-zerobus:2.12.0
```

> **Arrow needs a JVM flag:** `PutZerobusRecord` uses Apache Arrow, which requires reflective access to `java.nio` on Java 21. Add this line to `conf/bootstrap.conf` (the bundled `Dockerfile.nifi` does it for you), otherwise the processor refuses to start:
>
> ```
> java.arg.arrowAddOpens=--add-opens=java.base/java.nio=ALL-UNNAMED
> ```
> `PutZerobusIngest` works without it.

> **ARM64 (Apple Silicon, Graviton):** Zerobus SDK 1.6.0 ships native libraries for `linux-x86_64` and `linux-aarch64` (glibc and musl), so the image runs natively on both architectures — no `--platform linux/amd64` or Rosetta emulation needed.

## Configuration

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| **Zerobus Server Endpoint** | Yes | — | `<workspace-id>.zerobus.<region>.cloud.databricks.com` |
| **Workspace URL** | Yes | — | `https://dbc-xxxx.cloud.databricks.com` |
| **Target Table** | Yes | — | `catalog.schema.table` |
| **Service Principal Client ID** | Yes | — | OAuth 2.0 client ID |
| **Service Principal Client Secret** | Yes | — | OAuth 2.0 client secret (sensitive) |
| Batch Size | No | 100 | FlowFiles pulled per trigger. Sent in chunks of at most 9 MB (see below). |
| Max Inflight Records | No | 10000 | Records sent but not yet acknowledged. New records wait when this is reached. |
| ACK Wait Timeout | No | 30 sec | Max time to wait for server acknowledgment per batch |
| Delivery Guarantee | No | Guarantee Delivery | `Guarantee Delivery` routes to `success` only after the server acknowledgment. `Best Effort` does not wait (see [Delivery guarantee](#delivery-guarantee)). |
| Max FlowFile Size | No | 1 MB | Oversized FlowFiles are routed to failure. Caps heap usage. |

Zerobus sends a JSON batch as one message and rejects the whole batch if it is larger than 10 MB. The processor therefore splits each trigger's FlowFiles into chunks of at most 9 MB. A chunk is all-or-nothing: one record that fails validation sends its whole chunk to `failure`.

## Data Format

The processor accepts JSON FlowFiles (one JSON object or array per FlowFile). A basic structural check (starts/ends with `{}`/`[]`) catches obviously-not-JSON content before it reaches the server. The Zerobus server validates records against the Delta table schema — schema mismatches are routed to `failure`.

> **Note:** The Zerobus SDK also supports Protocol Buffers via `ZerobusProtoStream`. This processor currently uses the JSON stream. If you need Protobuf ingestion (higher throughput, stricter typing), open an issue or PR.

For non-JSON sources, use **PutZerobusRecord** (below) instead.

## PutZerobusRecord (Arrow)

**PutZerobusRecord** reads FlowFiles with any NiFi Record Reader (Avro, CSV, JSON, Parquet, ...), packs the records into Apache Arrow batches and ingests them over the Zerobus Arrow Flight path. One FlowFile may hold many records.

It shares the five connection properties, `ACK Wait Timeout` and `Delivery Guarantee` with PutZerobusIngest, plus:

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| **Record Reader** | Yes | — | Controller Service that parses the FlowFile and supplies the schema |
| Records Per Batch | No | 10000 | Max records per Arrow batch |
| Max Inflight Batches | No | 1000 | Backpressure threshold |
| IPC Compression | No | NONE | `NONE`, `LZ4_FRAME` or `ZSTD` |

The record schema must match the target table — field names and types are sent as-is:

| NiFi record type | Arrow type sent | Delta type |
|---|---|---|
| BYTE / SHORT / INT / LONG | `Int8` / `Int16` / `Int32` / `Int64` | TINYINT / SMALLINT / INT / BIGINT |
| FLOAT / DOUBLE | `Float32` / `Float64` | FLOAT / DOUBLE |
| BOOLEAN | `Boolean` | BOOLEAN |
| STRING, CHAR, ENUM, UUID, TIME | `LargeUtf8` | STRING |
| DECIMAL | `LargeUtf8` (as text) | DECIMAL |
| DATE | `Date32` | DATE |
| TIMESTAMP | `Timestamp(Microsecond, UTC)` | TIMESTAMP |
| ARRAY\<BYTE\> | `LargeBinary` | BINARY |
| ARRAY / MAP / RECORD | `List` / `Map` / struct | ARRAY / MAP / STRUCT |

Things to know:

- **Use an explicit schema.** Inferred schemas (e.g. JsonTreeReader with "Infer Schema") guess `LONG` for every integer and may produce `CHOICE` types, which are rejected. `TIMESTAMP_NTZ` and `VARIANT` columns are not supported.
- **Zerobus matches fields to columns by name**, and is strict about the rest: fields must appear in the same relative order as the table's columns, with the same type and the same nullability. Nullable columns may be left out of the record schema and are written as `NULL`. A field that is not a column of the table is rejected.
- **Batches are not limited to 10 MB.** The SDK splits an Arrow batch into smaller messages; only a single row has to fit in 10 MB.
- **The stream is opened on the first FlowFile**, because the Arrow schema comes from the Record Reader. Bad credentials therefore show up on the first FlowFile, not at processor start. A schema change reopens the stream.
- **At-least-once.** If a FlowFile fails partway, batches already sent may be ingested and a retry sends them again. See [Delivery guarantee](#delivery-guarantee).
- **Runs single-threaded** (`@TriggerSerially`) — the SDK's Arrow stream is not thread-safe.

## Delivery guarantee

Both processors deliver **at least once**. Duplicates can appear in two ways:

- A FlowFile routed to `retry` is sent again, including any part of it that had already landed.
- The SDK reconnects on its own after a connection loss and replays what was not yet acknowledged. This happens inside the SDK, without the FlowFile leaving the processor, so NiFi does not see it.

If duplicates matter, put a unique ID or a sequence number in every record and deduplicate downstream (`MERGE INTO`, or `ROW_NUMBER()` over the key).

The `Delivery Guarantee` property controls when a FlowFile is routed to `success`:

| Value | Behavior |
|---|---|
| `Guarantee Delivery` (default) | After Zerobus has acknowledged the data as durable. |
| `Best Effort` | As soon as the SDK has accepted the data. Faster, because the processor does not stop for each acknowledgment, but data that is still in flight when NiFi crashes or the stream fails for good is lost, and those FlowFiles are already in `success`. |

In `Best Effort` mode the amount of data at risk is bounded by `Max Inflight Records` (PutZerobusIngest) or `Max Inflight Batches` (PutZerobusRecord). Stopping the processor flushes what is in flight.

Both processors count the records they send in a NiFi counter named `Records Ingested`. Replays done by the SDK are not counted; `system.lakeflow.zerobus_ingest` has the server-side numbers.

## Example Flow

```
GenerateFlowFile / ConsumeKafka / GetSyslog
    → ConvertRecord (to JSON if needed)
    → PutZerobusIngest
        ├── success → LogAttribute
        ├── failure → PutFile (dead letter)
        └── retry   → (back to PutZerobusIngest via retry loop)
```

## Databricks Setup

1. **Create a Delta table:**

```sql
CREATE TABLE catalog.schema.security_events (
    asset_id STRING,
    event_type STRING,
    severity STRING,
    payload STRING,
    event_time TIMESTAMP
) USING DELTA;
```

2. **Create a service principal** with permissions:

```sql
GRANT USE CATALOG ON CATALOG catalog TO `sp-nifi`;
GRANT USE SCHEMA ON SCHEMA catalog.schema TO `sp-nifi`;
GRANT MODIFY, SELECT ON TABLE catalog.schema.security_events TO `sp-nifi`;
```

> **Important:** Zerobus Ingest requires **explicit** `MODIFY` and `SELECT` grants on the target table. `ALL_PRIVILEGES` inherited from a parent catalog or schema is **not sufficient** — the Zerobus OAuth token request uses fine-grained `authorization_details` scoping that only recognizes direct table-level grants. Without them, you'll get a `401: User is not authorized to the requested authorizations` error even if the service principal appears to have full access.

3. **Find your Zerobus endpoint:**

```
https://<workspace-id>.zerobus.<region>.cloud.databricks.com
```

The workspace ID is in your Databricks URL: `https://dbc-xxx.cloud.databricks.com/?o=<workspace-id>`

## Performance

The processor leverages the Zerobus Java SDK's native Rust backend:

- Throughput: up to 100 MB/s per stream (1KB messages)
- Acknowledgment latency: P50 ≤ 200ms, P95 ≤ 500ms
- Time to Delta table: P50 ≤ 5 seconds

NiFi's built-in backpressure and the processor's `Max Inflight Records` setting work together to prevent overwhelming the Zerobus endpoint.

## Running Tests

```bash
mvn test
```

Integration tests (requires Databricks credentials):

```bash
mvn verify -Pit \
  -Dzerobus.endpoint=<endpoint> \
  -Dzerobus.workspace=<url> \
  -Dzerobus.table=<catalog.schema.table> \
  -Dzerobus.clientId=<id> \
  -Dzerobus.clientSecret=<secret>
```

## License

Apache License 2.0
