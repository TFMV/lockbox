# orc2parquet.py
import sys
import pyarrow.orc as orc
import pyarrow.parquet as pq

if len(sys.argv) != 3:
    print("Usage: python orc2parquet.py input.orc output.parquet")
    sys.exit(1)

orc_path = sys.argv[1]
parquet_path = sys.argv[2]

orc_file = orc.ORCFile(orc_path)
table = orc_file.read()
pq.write_table(table, parquet_path)
print(f"Converted {orc_path} to {parquet_path}")
