# Runs of 2026-10-08 as recorded by Zerobus

Laptop (Apple Silicon, Docker, Wi-Fi) in Poland to a workspace in AWS eu-west-1, about 44 ms
round trip. One stream and one thread per run unless noted. Numbers come from
`system.lakeflow.zerobus_ingest`, one row per stream; durations are first to last commit
(Zerobus commits about every 5 seconds), so rates are approximate.

```sql
SELECT table_name, stream_id,
       MIN(commit_time) AS first_commit, MAX(commit_time) AS last_commit,
       COUNT(*) AS commits,
       SUM(committed_records) AS records,
       ROUND(SUM(committed_bytes) / 1e9, 2) AS gb
FROM system.lakeflow.zerobus_ingest
WHERE table_name IN ('demo.default.zerobus_perf', 'demo.default.neowise_perf')
  AND commit_time >= '2026-10-08'
GROUP BY table_name, stream_id
ORDER BY first_commit;
```

## Synthetic data (1000-byte records, 6 columns)

| Run | First commit (UTC) | Commits | Records | GB | Duration | Records/s | MB/s |
|---|---|---|---|---|---|---|---|
| JSON, wait for ACK | 19:34:04 | 61 | 4 161 000 | 4.16 | 302 s | 13 800 | 13.8 |
| Arrow, 10 000 records/FlowFile | 19:39:41 | 62 | 5 190 000 | 4.95 | 307 s | 16 900 | 16.1 |
| Arrow + ZSTD, 10 000 records/FlowFile | 19:50:28 | 26 | 6 010 000 | 5.72 | 128 s | 47 000 | 44.7 |
| Arrow + ZSTD, 20 000 records/FlowFile | 19:57:11 | 24 | 11 460 000 | 10.91 | 120 s | 95 500 | 90.9 |
| Arrow, 20 000 records/FlowFile | 20:02:41 | 28 | 2 220 000 | 2.12 | 136 s | 16 300 | 15.6 |
| Arrow, Best Effort | 20:35:48 | 30 | 3 000 000 | 2.86 | 146 s | 20 500 | 19.6 |
| JSON, Best Effort (before the in-flight fix) | 20:38:29 | 70 | 7 257 000 | 7.26 | 349 s | 20 800 | 20.8 |
| Two lanes at once: Arrow | 20:48:58 | 29 | 2 300 000 | 2.19 | 142 s | 16 200 | 15.4 |
| Two lanes at once: JSON | 20:48:58 | 25 | 1 399 000 | 1.40 | 122 s | 11 500 | 11.5 |

## NASA NEOWISE (144 columns, about 1.2 KB per row)

| Run | First commit (UTC) | Commits | Records | GB | Duration | Records/s | MB/s |
|---|---|---|---|---|---|---|---|
| Smoke test | 21:03:34 | 12 | 740 000 | 0.91 | 55 s | 13 300 | 16.4 |
| Arrow | 21:06:34 | 30 | 2 220 000 | 2.73 | 146 s | 15 200 | 18.7 |
| Arrow + ZSTD, until the laptop went to sleep | 21:09:21 | 13 | 2 168 332 | 2.62 | 61 s | 35 300 | 42.6 |
| Arrow + ZSTD, stream reopened after wake-up | 21:17:42 | 5 | 396 668 | 0.48 | 30 s | | |

## What the numbers show

- Row counts match NiFi's `Records Ingested` counters exactly for every uninterrupted run
  (43 008 000 rows in `zerobus_perf`).
- `committed_bytes` is measured before compression: the ZSTD runs show the same bytes per record
  as the uncompressed ones, although about 30 times fewer bytes were sent.
- Without compression every run plateaus at 14 to 20 MB/s per stream, whatever the format or
  FlowFile size. Two streams at once reach about twice that. The NiFi container used well under
  one core, so the limit is the connection to the region, not the processor.
- With ZSTD on the synthetic payload one stream reaches the documented quota of 100 000 records/s.
  On real data ZSTD compresses about 3.5 times and gives about 2.3 times the rows per second.
- Not waiting for the ACK (Best Effort) gains about 30 to 50% on an uncompressed stream.
- At-least-once in practice: the interrupted NEOWISE run committed 2 565 000 rows for 2 560 000
  counted by NiFi. The SDK replayed one 5000-row Arrow batch when it reopened the stream.
