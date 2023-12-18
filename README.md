# activity-dashboard-backend

<!-- Intro about the dashboard and how it works -->

Create the latest daily statistics parquet files: 
```sql
UNLOAD (
    SELECT * FROM youthmappers_daily_rollup
) TO 
    's3://youthmappers-internal-us-east1/query_results/parquet/ds=<DATE>'
WITH (
    format='parquet',
    compression='zstd'
)
```

Add the latest data as the newest partition
```sql
ALTER TABLE daily_ym_stats ADD IF NOT EXISTS 
    PARTITION (ds='<DATE>')
```

For the local DuckDB computation, sync the parquet files locally: 

```bash
aws --profile=ym s3 sync s3://youthmappers-internal-us-east1/query_results/parquet/ parquet/
```

Now run `queries.sql` in duckdb






For reference: 
```sql
CREATE TABLE daily_ym_stats WITH(
    format='parquet',
    write_compression='zstd',
    external_location='s3://youthmappers-internal-us-east1/query_results/parquet/',
    partitioned_by=ARRAY['ds']
) AS (
    SELECT *, '2023-12-11' as ds FROM youthmappers_daily_rollup
)
```


YouthMappers statistics are based on the following 2 views: 

### 1. YouthMappers Daily Rollup
```sql
CREATE OR REPLACE VIEW "youthmappers_daily_rollup" AS 
WITH
    changesets_per_tile_user_day AS (
        SELECT 
            sum(new_highways) AS new_highways, 
            sum(new_buildings) AS new_buildings, 
            sum(new_amenities) AS new_amenities, 
            sum(new_features) AS new_features, 
            sum(edited_highways) AS edited_highways, 
            sum(edited_buildings) AS edited_buildings,
            sum(edited_amenities) AS edited_amenities,
            sum(edited_features) AS edited_features,
            sum(edited_vertices) AS edited_vertices,
            sum(edited_elements) AS edited_elements,
            sum(deleted_elements) AS deleted_elements,
            sum(deleted_nodes) AS deleted_nodes,
            sum(num_changes) AS sum_edits,
            count(DISTINCT id) AS total_changesets,
            arbitrary(id) AS arbitrary_changeset,
            slice(array_agg(id), 1, 5) AS sample_changeset_ids,
            flatten(array_agg(hashtags)) AS hashtags_lists,
            histogram(created_by) AS tools,
            geometry_union_agg(ST_GeometryFromText(center)) AS center_point_agg,
            min(min_lat) AS min_lat,
            min(min_lon) AS min_lon,
            max(max_lat) AS max_lat,
            max(max_lon) AS max_lon,
            sum(area) AS total_area,
            uid,
            quadkey,
            _day
        FROM
            youthmapper_changesets
        GROUP BY uid, quadkey, _day
    ), 
    per_tile_user_day_with_ym AS (
        SELECT
            _day, 
            quadkey, 
            ST_ASBINARY(ST_Centroid(center_point_agg)) AS centroid, 
            ST_ASBINARY(ST_CONVEXHULL(center_point_agg)) AS convex_hull, 
            new_highways, 
            new_buildings, 
            new_amenities, 
            edited_highways, 
            edited_buildings, 
            edited_amenities, 
            new_features, 
            edited_features, 
            edited_vertices, 
            edited_elements, 
            deleted_elements, 
            deleted_nodes, 
            sum_edits, 
            arbitrary_changeset, 
            sample_changeset_ids, 
            total_changesets, 
            min_lon, 
            max_lon, 
            min_lat, 
            max_lat, 
            total_area, 
            tools, 
            hashtags_lists hashtags, 
            youthmappers.*
        FROM (
            changesets_per_tile_user_day INNER JOIN youthmappers ON (changesets_per_tile_user_day.uid = youthmappers.uid)
        )
    )
    SELECT
        *, 
        CASE
            WHEN ((chapter_lon IS NOT NULL) AND (chapter_lat IS NOT NULL)) THEN 
                ST_DISTANCE(
                    to_spherical_geography(
                        ST_Point(chapter_lon, chapter_lat)
                    ), 
                    to_spherical_geography(
                        ST_GEOMFROMBINARY(centroid)
                    )
                ) / 1000
            ELSE null 
        END AS km_to_university
    FROM
        per_tile_user_day_with_ym
    ORDER BY _day DESC, uid DESC
```

### 2. Changesets from known YouthMappers

```sql
CREATE OR REPLACE VIEW "youthmapper_changesets" AS
    WITH ym_changesets AS (
        SELECT id, 
        changesets.uid, 
        split(tags['hashtags'], ';') AS hashtags, 
        split(tags['created_by'], ' ')[1] AS created_by, 
        date(changesets.created_at) AS _day, 
        num_changes, 
        ST_ASTEXT(ST_Point(((min_lon + max_lon) / 2), ((min_lat + max_lat) / 2))) AS center, 
        bing_tile_quadkey(bing_tile_at(((min_lat + max_lat) / 2), ((min_lon + max_lon) / 2), 15)) AS quadkey, 
        min_lat, 
        min_lon, 
        max_lat, 
        max_lon, 
        CASE 
            WHEN ((min_lat <> max_lat) AND (min_lon <> max_lon)) THEN 
                ST_AREA(to_spherical_geography(ST_ENVELOPE(ST_LINESTRING(ARRAY[ST_Point(min_lon, min_lat),ST_Point(max_lon, max_lat)])))) 
            ELSE 
                0 
        END AS area
        FROM changesets INNER JOIN 
            youthmappers ON youthmappers.uid = changesets.uid
        WHERE changesets.created_at > date '2015-01-01'
    ), changeset_totals AS (
        SELECT
            planet_history.changeset, 
            count_if(((tags['highway'] IS NOT NULL) AND (version = 1))) AS new_highways, 
            count_if(((tags['building'] IS NOT NULL) AND (version = 1))) AS new_buildings, 
            count_if(((tags['amenity'] IS NOT NULL) AND (version = 1))) AS new_amenities, 
            count_if(((tags['highway'] IS NOT NULL) AND (version > 1))) AS edited_highways, 
            count_if(((tags['building'] IS NOT NULL) AND (version > 1))) AS edited_buildings, 
            count_if(((tags['amenity'] IS NOT NULL) AND (version > 1))) AS edited_amenities, 
            count_if(((type = 'node') AND (version > 1) AND (cardinality(tags) = 0) AND (visible = true))) AS edited_vertices, 
            count_if((visible = false)) AS deleted_elements, 
            count_if(((visible = false) AND (type = 'node'))) AS deleted_nodes, 
            count_if((version > 1)) AS edited_elements, 
            count_if(((version > 1) AND (cardinality(tags) > 0))) AS edited_features, 
            count_if((version = 1)) AS new_features
    FROM
            planet_history INNER JOIN 
            ym_changesets ON planet_history.changeset = ym_changesets.id
    WHERE 
            cardinality(planet_history.tags) > 0 OR 
            type IN ('way', 'relation') OR 
            (type = 'node' AND version > 1)
    GROUP BY planet_history.changeset
    )
    SELECT
        ym_changesets.*, 
        changeset_totals.*
    FROM
        ym_changesets
        INNER JOIN changeset_totals ON 
            ym_changesets.id = changeset_totals.changeset
```