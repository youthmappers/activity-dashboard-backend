import duckdb

cursor = duckdb.connect()
cursor.execute("INSTALL httpfs;")
cursor.execute("LOAD httpfs;")
cursor.execute("SET s3_region='us-east-1';")

res1 = cursor.execute("""
SELECT
  count(1)
FROM read_parquet('s3://youthmappers-internal-us-east1/query_results/parquet/ds=*/*', filename=true, hive_partitioning=1)
WHERE ds = '2024-06-09'
LIMIT 1;
""").fetchall()

print(res1)

res2 = cursor.execute("""
  WITH daily_aggregation AS (
      SELECT
          _day,
          CAST(sum(edited_features) + sum(new_features) AS BIGINT) as all_feats,
          CAST(sum(edited_buildings) + sum(new_buildings) AS BIGINT) as buildings,
          CAST(sum(edited_highways) + sum(new_highways) AS BIGINT) as highways,
          CAST(sum(edited_amenities) + sum(new_amenities) AS BIGINT) as amenities,
          COUNT(DISTINCT(uid)) as uids,
          COUNT(DISTINCT(chapter_id)) AS chapters
      FROM read_parquet('s3://youthmappers-internal-us-east1/query_results/parquet/ds=*/*', filename=true, hive_partitioning=1)
  WHERE ds = '2024-06-09' AND centroid IS NOT NULL
  GROUP BY _day ORDER BY _day ASC
  )
  SELECT _day as day, 
      AVG(all_feats) OVER(ORDER BY _day ROWS 7 PRECEDING) AS all_feats_rolling_avg, 
      all_feats,
      AVG(buildings) OVER(ORDER BY _day ROWS 7 PRECEDING) AS buildings_rolling_avg, 
      buildings,
      AVG(highways) OVER(ORDER BY _day ROWS 7 PRECEDING) AS highways_rolling_avg, 
      highways,
      AVG(amenities) OVER(ORDER BY _day ROWS 7 PRECEDING) AS amenities_rolling_avg, 
      amenities,
      AVG(uids) OVER(ORDER BY _day ROWS 7 PRECEDING) AS users_rolling_avg, 
      uids as users,
      AVG(chapters) OVER(ORDER BY _day ROWS 7 PRECEDING) AS chapters_rolling_avg, 
      chapters
  FROM daily_aggregation ORDER BY _day ASC
  LIMIT 100""").fetchall()

print(res2)
