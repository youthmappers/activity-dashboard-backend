import duckdb

cursor = duckdb.connect()
cursor.execute("INSTALL httpfs;")
cursor.execute("LOAD httpfs;")
cursor.execute("SET s3_region='us-west-2';")
res = cursor.execute("""
SELECT
  *
FROM read_parquet('s3a://overturemaps-us-west-2/release/2024-06-13-beta.0/theme=places/type=place/*', filename=true, hive_partitioning=1)
LIMIT 1;
""").fetchall()

print(res)
