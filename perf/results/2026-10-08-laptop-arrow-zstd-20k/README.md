# Run 2026-10-08, laptop, PutZerobusRecord (Arrow) with ZSTD, 20 000 records per FlowFile

2 minutes, one stream, one thread, 1000-byte records, Records Per Batch 5000.

| | 10 000 records / FlowFile, no compression | 10 000 records / FlowFile, ZSTD | 20 000 records / FlowFile, ZSTD |
|---|---|---|---|
| Records/s | 16 700 | about 49 000 | about 94 000 |
| Bytes sent by the container | about 15 MB/s | about 0.5 MB/s | about 0.9 MB/s |
| Container CPU | about 20% of a core | about 30% | about 30% |
| failure / retry | 0 / 0 | 0 / 0 | 0 / 0 |

In the 20 000-record run the sink's input queue stayed near empty: the limit was the generator's
cap of 100 000 records/s (the Zerobus per-stream quota), not the processor.

The test payload is one record repeated, so ZSTD shrinks it by about 99%. Real data compresses far less.
