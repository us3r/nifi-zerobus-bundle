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

On Kubernetes:

```bash
kubectl cp nifi-zerobus-nar/target/nifi-zerobus-nar-0.1.0.nar \
  <namespace>/<nifi-pod>:/opt/nifi/nifi-current/lib/
kubectl -n <namespace> exec <nifi-pod> -- /opt/nifi/nifi-current/bin/nifi.sh restart
```

## Configuration

| Property | Required | Description |
|----------|----------|-------------|
| **Zerobus Server Endpoint** | Yes | `<workspace-id>.zerobus.<region>.cloud.databricks.com` |
| **Workspace URL** | Yes | `https://dbc-xxxx.cloud.databricks.com` |
| **Target Table** | Yes | `catalog.schema.table` |
| **Service Principal Client ID** | Yes | OAuth 2.0 client ID |
| **Service Principal Client Secret** | Yes | OAuth 2.0 client secret (sensitive) |
| Batch Size | No | FlowFiles per ingest call (default: 100) |
| Max Inflight Records | No | Backpressure threshold (default: 10000) |

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
