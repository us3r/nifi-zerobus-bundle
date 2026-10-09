# Run 2026-10-08, laptop (Apple Silicon, Docker) to Databricks over a home uplink

One stream, one thread per lane, 1000-byte records, 5 minutes of generation per lane.

| | PutZerobusIngest (JSON) | PutZerobusRecord (Arrow) |
|---|---|---|
| Records acknowledged | 4 161 000 | 5 190 000 |
| Average rate | 13 900 records/s (13.9 MB/s) | 16 700 records/s (16.7 MB/s) |
| 10-second samples, min to max | 12 000 to 15 000 | 16 000 to 17 000 |
| failure / retry | 0 / 0 | 0 / 0 |
| `event_ts` range (ms) | 1791488041698 to 1791488349200 | 1791488374247 to 1791488691875 |

Row count in the table after the run: 9 362 000 = 4 161 000 + 5 190 000 + 11 000 rows from
earlier connection checks. No loss, no duplicates.

**This run does not compare the processors.** During the Arrow lane the NiFi container used
about 20% of one core and sent a steady 15 MB/s, and both lanes plateaued in the same place.
Later runs showed the limit is per stream on the 44 ms path to eu-west-1, not the laptop's
uplink: two streams at once reached twice the rate. See `../2026-10-08-server-side.md`.

Files: `results.json` and `samples.log` (10-second samples from NiFi's counters),
`dashboard.png` and panel screenshots from Grafana. The throughput panels show a 5-minute
average, so a 5-minute lane appears as a ramp that only reaches the true rate at its end.
