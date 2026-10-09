"""Splits a NEOWISE-R parquet leaf into small parquet files for the NiFi performance test.

Columns are put in the order of neowise_table.sql and a null client_ts_ms column is
appended, so the record schema matches the target table exactly.

    pip install pyarrow
    python split_neowise.py part0.snappy.parquet ../neowise_table.sql ../data/neowise [rows_per_file]
"""
import pathlib
import re
import sys

import pyarrow as pa
import pyarrow.parquet as pq

source, ddl, out_dir = sys.argv[1], sys.argv[2], pathlib.Path(sys.argv[3])
rows_per_file = int(sys.argv[4]) if len(sys.argv) > 4 else 20000

columns = re.findall(r"^\s+(\w+) (?:STRING|BIGINT|DOUBLE)", pathlib.Path(ddl).read_text(), re.M)
table = pq.read_table(source)
table = table.append_column("client_ts_ms", pa.nulls(table.num_rows, pa.int64())).select(columns)

out_dir.mkdir(parents=True, exist_ok=True)
for i, offset in enumerate(range(0, table.num_rows, rows_per_file)):
    pq.write_table(table.slice(offset, rows_per_file), out_dir / f"neowise-{i:04d}.parquet", compression="snappy")
print(f"{table.num_rows} rows, {len(columns)} columns -> {i + 1} files in {out_dir}")
