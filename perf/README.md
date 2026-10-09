# Performance test stack

Local stack for comparing `PutZerobusIngest` (JSON) with `PutZerobusRecord` (Arrow):
NiFi 2.12.0 with the Zerobus NAR, Prometheus, Grafana and a small exporter for NiFi's counters.

```bash
mvn clean package                                  # from the repo root, builds the NAR
docker compose -f perf/docker-compose.yml up -d --build
```

| Service | URL |
|---|---|
| NiFi | http://localhost:8088/nifi |
| Grafana (dashboard "Zerobus Perf Test") | http://localhost:3000/d/zerobus-perf |
| Prometheus | http://localhost:9090 |

NiFi runs over plain HTTP with no login so Prometheus can scrape
`/nifi-api/flow/metrics/prometheus`. All ports are bound to `127.0.0.1`. Do not expose this stack.

## Target table

```sql
CREATE TABLE catalog.schema.zerobus_perf (
    event_ts   BIGINT,   -- epoch milliseconds, set when the FlowFile was generated
    source     STRING,   -- 'json' (PutZerobusIngest) or 'arrow' (PutZerobusRecord)
    asset_id   STRING,
    event_type STRING,
    severity   STRING,
    payload    STRING
) USING DELTA;

GRANT MODIFY, SELECT ON TABLE catalog.schema.zerobus_perf TO `<service-principal>`;
```

## Flow

`zerobus-perf-flow.json` is a NiFi flow definition. Import it by dragging a Process Group onto
the canvas and choosing the file, then fill in the `Zerobus Perf` parameter context
(endpoint, workspace URL, table, client ID, client secret) and enable the
`Perf JSON Reader` controller service.

Lanes A and B send the same synthetic data: records of exactly 1000 bytes, capped at the source
to the Zerobus per-stream quota of 100 000 records/s (100 MB/s). Each lane uses one stream and
one thread.

| | A: PutZerobusIngest | B: PutZerobusRecord |
|---|---|---|
| FlowFile | 1 record, 1 KB | 20 000 records, 20 MB |
| Source rate cap | 1000 FlowFiles / 10 ms | 1 FlowFile / 200 ms |
| Sink setting | Batch Size 10 000, Max Inflight Records 50 000 | Records Per Batch 5000 |
| One ACK per | 10 000 records | 20 000 records |
| Input queue backpressure | 20 000 FlowFiles | 20 FlowFiles |

Both sinks default to `Guarantee Delivery` and no compression. `IPC Compression` (lane B) and
`Delivery Guarantee` are the two settings worth varying between runs. The synthetic payload is
one record repeated, so ZSTD shrinks it by about 99%: use lane C for realistic compression.

## Running a test

1. Reset the counters in NiFi (menu > Counters), start one sink, then its generator. Run one lane at a time.
2. Let it run for at least 10 minutes. The throughput panels show a 5-minute average, so they need 5 minutes to reach the true rate.
3. Stop the generator and wait for the input queue to drain.
4. Check the error queues in front of the funnels: both should be empty.
5. Compare the "Records Ingested" counter in NiFi (menu > Counters) with the table:

```sql
SELECT source, COUNT(*) AS records,
       COUNT(*) / ((MAX(event_ts) - MIN(event_ts)) / 1000) AS avg_records_per_s
FROM catalog.schema.zerobus_perf GROUP BY source;
```

Delivery is at-least-once, so the table may hold more rows than the counter if anything was retried.

## Lane C: real data (NASA NEOWISE)

Lane C sends the dataset used in the Databricks petabyte benchmark through `PutZerobusRecord`
with a `ParquetReader`: 144 columns (99 DOUBLE, 34 BIGINT, 10 STRING), about 1.2 KB per row.
It needs three things that are not in the repo:

1. The target table: run `neowise_table.sql` (edit the table name and the grant) and set the
   `neowise.table` parameter.
2. The Parquet NARs, which the stock NiFi image does not ship. Download both into `perf/nars/`:

   ```bash
   for a in nifi-parquet-nar nifi-hadoop-libraries-nar; do
     curl -o perf/nars/$a-2.12.0.nar https://repo1.maven.org/maven2/org/apache/nifi/$a/2.12.0/$a-2.12.0.nar
   done
   ```
3. Test data in `perf/data/neowise/`. Download one leaf of the public bucket (no AWS credentials
   needed) and split it into 20 000-row files whose schema matches the table:

   ```bash
   curl -o part0.snappy.parquet "https://nasa-irsa-wise.s3.amazonaws.com/wise/neowiser/catalogs/p1bs_psd/healpix_k5/year8/neowiser-healpix_k5-year8.parquet/healpix_k0=0/healpix_k5=526/part0.snappy.parquet"
   pip install pyarrow
   python perf/tools/split_neowise.py part0.snappy.parquet perf/neowise_table.sql perf/data/neowise
   ```

`GetFile` re-reads the same files in a loop, capped at one file per 200 ms (100 000 rows/s).
`client_ts_ms` is sent as NULL.
