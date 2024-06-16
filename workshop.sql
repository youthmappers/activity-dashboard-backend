COPY(
    SELECT 
        CAST(_day as varchar) as date,
        chapter,
        -- q8 AS quadkey,
        mappers,
        CAST(amenities AS int) AS amenities,
        CAST(buildings AS int) AS buildings,
        CAST(highways AS int) AS highways,
        CAST(all_feats - amenities - buildings - highways AS int) AS other,
        ST_CENTROID(points) AS geometry,
    FROM (
        SELECT 
            _day,
            chapter,
            count(distinct(uid)) as mappers,
            substr(quadkey,1,8) as q8,
            ST_Union_Agg(ST_GeomFromWKB(centroid)) as points,
            sum(new_amenities + edited_amenities) as amenities,
            sum(new_buildings + edited_buildings) as buildings,
            sum(new_highways + edited_highways) as highways,
            sum(new_features + edited_features) as all_feats,
            count(distinct(uid)) as mappers
        FROM READ_PARQUET('parquet/*/*', hive_partitioning=1)
            WHERE centroid IS NOT NULL
            AND _day >= '2023-01-01'
        GROUP BY _day, substr(quadkey,1,8), chapter
    )
    ORDER BY _day, chapter, q8 ASC
) TO 'daily_chapter_activity_workshop.geojson' WITH (FORMAT GDAL, DRIVER "GeoJSON");