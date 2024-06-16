import duckdb

cursor = duckdb.connect()
cursor.execute("INSTALL httpfs;")
cursor.execute("LOAD httpfs;")
cursor.execute("SET s3_region='us-east-1';")



res = cursor.execute("""
SELECT
  count(1)
FROM read_parquet('s3://youthmappers-internal-us-east1/query_results/parquet/ds=*/*', filename=true, hive_partitioning=1)
WHERE ds = '2024-06-09'
LIMIT 1;
""").fetchall()

print(res)
