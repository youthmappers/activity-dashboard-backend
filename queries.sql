LOAD spatial;

COPY(
    WITH daily_aggregation AS (
        SELECT
            _day,
            CAST(sum(edited_features) + sum(new_features) AS BIGINT) as all_feats,
            CAST(sum(edited_buildings) + sum(new_buildings) AS BIGINT) as buildings,
            CAST(sum(edited_highways) + sum(new_highways) AS BIGINT) as highways,
            CAST(sum(edited_amenities) + sum(new_amenities) AS BIGINT) as amenities,
            COUNT(DISTINCT(uid)) as uids,
            COUNT(DISTINCT(chapter_id)) AS chapters
        FROM READ_PARQUET('parquet/*/*', hive_partitioning=1)
    WHERE centroid IS NOT NULL
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
) TO 'daily_activity.csv';

COPY(
    SELECT 
        CAST(_day as varchar) as date,
        chapter,
        q8 AS quadkey,
        CAST(amenities AS int) AS amenities,
        CAST(buildings AS int) AS buildings,
        CAST(highways AS int) AS highways,
        CAST(all_feats - amenities - buildings - highways AS int) AS other,
        ST_CENTROID(points) AS geometry,
    FROM (
        SELECT 
            _day,
            chapter,
            substr(quadkey,1,8) as q8,
            ST_Union_Agg(ST_GeomFromWKB(centroid)) as points,
            sum(new_amenities + edited_amenities) as amenities,
            sum(new_buildings + edited_buildings) as buildings,
            sum(new_highways + edited_highways) as highways,
            sum(new_features + edited_features) as all_feats,
            count(distinct(uid)) as mappers
        FROM READ_PARQUET('parquet/*/*', hive_partitioning=1)
            WHERE centroid IS NOT NULL
        GROUP BY _day, substr(quadkey,1,8), chapter
    )
    ORDER BY _day, chapter, q8 ASC
) TO 'daily_chapter_activity.geojson' WITH (FORMAT GDAL, DRIVER "GeoJSON");

--  Create the zoom level 8 tile summaries by week
COPY(
    SELECT
        arbitrary(chapter_id) as chapter_id,
        CAST(epoch(date_trunc('week',_day)) AS int) as timestamp,
        CAST(sum(edited_features) + sum(new_features) AS BIGINT) as all_feats,
        ST_CENTROID(ST_Union_Agg(ST_GeomFromWKB(centroid))) AS geometry
    FROM read_parquet('parquet/*/*', hive_partitioning=1)
    WHERE centroid IS NOT NULL
    GROUP BY date_trunc('week',_day), uid, substr(quadkey,1,8)
) TO 'aggregated_by_zoom/z8_weekly.geojsonseq' WITH (FORMAT GDAL, DRIVER "GeoJSONSeq");

-- Create the zoom level 10 tile summaries
COPY(
    SELECT
        arbitrary(chapter_id) as chapter_id,
        CAST(epoch(_day) AS int) as timestamp,
        CAST(sum(edited_features) + sum(new_features) AS BIGINT) as all_feats,
        ST_CENTROID(ST_Union_Agg(ST_GeomFromWKB(centroid))) AS geometry
    FROM read_parquet('parquet/*/*', hive_partitioning=1)
    WHERE centroid IS NOT NULL
    GROUP BY _day, uid, substr(quadkey,1,10)
) TO 'aggregated_by_zoom/z10_daily.geojsonseq' WITH (FORMAT GDAL, DRIVER "GeoJSONSeq");

-- Write the per-user, per-tile, per-day results:
COPY(
    SELECT
        chapter_id,
        CAST(epoch(_day) AS int) as timestamp,
        edited_features + new_features AS all_feats,
        edited_amenities + new_amenities AS amenities,
        edited_buildings + new_buildings AS buildings,
        edited_highways + new_highways AS highways,
        ST_GeomFromWKB(centroid) AS geometry
    FROM read_parquet('parquet/*/*', hive_partitioning=1)
    WHERE centroid IS NOT NULL
) TO 'aggregated_by_zoom/z15_daily.geojsonseq' WITH (FORMAT GDAL, DRIVER "GeoJSONSeq");

-- Write the per-user, per-tile, per-day bbox results:
COPY(
    SELECT
        chapter_id,
        CAST(epoch(_day) AS int) as timestamp,
        edited_features + new_features AS all_feats,
        edited_amenities + new_amenities AS amenities,
        edited_buildings + new_buildings AS buildings,
        edited_highways + new_highways AS highways,
        ST_Envelope(
            ST_Collect(
                ARRAY[
                    ST_Point(min_lon, min_lat),
                    ST_Point(max_lon, max_lat)
                ]
            )
        ) AS geometry
    FROM read_parquet('parquet/*/*', hive_partitioning=1)
    WHERE centroid IS NOT NULL
) TO 'aggregated_by_zoom/z15_daily_bboxes.geojsonseq' WITH (FORMAT GDAL, DRIVER "GeoJSONSeq");