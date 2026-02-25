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

- Apache NiFi 1.20+
- Java 11+
- Databricks workspace with Zerobus Ingest enabled
- Service principal with `MODIFY` + `SELECT` on the target table

## Build

```bash
mvn clean package -DskipTests
```

The NAR file will be at `nifi-zerobus-nar/target/nifi-zerobus-nar-0.1.0.nar`.

## Install

Copy the NAR to NiFi's `lib/` directory and restart:

```bash
cp nifi-zerobus-nar/target/nifi-zerobus-nar-0.1.0.nar $NIFI_HOME/lib/
$NIFI_HOME/bin/nifi.sh restart
```

On Kubernetes (recommended — bake into image):

```dockerfile
FROM apache/nifi:1.28.1
COPY nifi-zerobus-nar-0.1.0.nar /opt/nifi/nifi-current/lib/
```

```bash
docker build -t nifi-zerobus:1.28.1 .
kubectl -n <namespace> set image deployment/nifi nifi=nifi-zerobus:1.28.1
```

> **Apple Silicon (ARM64):** The Zerobus SDK ships native libraries for `linux-x86_64` only. On ARM64 hosts (OrbStack, Rancher Desktop on Apple Silicon), you must build the image with `--platform linux/amd64` and enable Rosetta emulation in your container runtime. Without this, the processor will fail with `UnsatisfiedLinkError: libzerobus_jni.so`. Production x86_64 clusters are unaffected.

## Configuration

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| **Zerobus Server Endpoint** | Yes | — | `<workspace-id>.zerobus.<region>.cloud.databricks.com` |
| **Workspace URL** | Yes | — | `https://dbc-xxxx.cloud.databricks.com` |
| **Target Table** | Yes | — | `catalog.schema.table` |
| **Service Principal Client ID** | Yes | — | OAuth 2.0 client ID |
| **Service Principal Client Secret** | Yes | — | OAuth 2.0 client secret (sensitive) |
| Batch Size | No | 100 | FlowFiles per ingest call |
| Max Inflight Records | No | 10000 | Backpressure threshold |
| ACK Wait Timeout | No | 30 sec | Max time to wait for server acknowledgment per batch |
| Max FlowFile Size | No | 1 MB | Oversized FlowFiles are routed to failure. Caps heap usage. |

## Data Format

The processor accepts JSON FlowFiles (one JSON object or array per FlowFile). A basic structural check (starts/ends with `{}`/`[]`) catches obviously-not-JSON content before it reaches the server. The Zerobus server validates records against the Delta table schema — schema mismatches are routed to `failure`.

> **Note:** The Zerobus SDK also supports Protocol Buffers via `ZerobusProtoStream`. This processor currently uses the JSON stream. If you need Protobuf ingestion (higher throughput, stricter typing), open an issue or PR.

For non-JSON sources, use NiFi's `ConvertRecord` processor upstream to transform to JSON first.

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
