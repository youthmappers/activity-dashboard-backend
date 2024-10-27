import duckdb 
import argparse
import os.path

def main():

    parser = argparse.ArgumentParser(
        description="CLI "
    )

    parser.add_argument("-ds", 
        "--ds", 
        default="2024-10-20",
        help="ds of latest data from S3"
    )

    parser.add_argument("-o", 
        "--output", 
        default="aggregated_by_zoom",
        help="Output directory"
    )
    args = parser.parse_args()

    print(args)

    ym = YMController(args)

    print("Setting up DuckDB... ")
    ym.load_credentials()
    
    print("Downloading Parquet... ")
    ym.download_parquet()
    
    print("Creating Daily activity CSV for timeline")
    ym.daily_activity()
    
    print("Creating Daily Chapter Activity GeoJSON")
    ym.daily_chapter_activity()

    print("Creating Weekly Chapter Summaries (zoom 8) ")
    ym.tile_level_weekly_summaries(zoom_level=8)

    print("Creating Weekly Chapter Summaries (zoom 10) ")
    ym.tile_level_weekly_summaries(zoom_level=10)
    
    print("Creating Daily Level Tile Summaries")
    ym.daily_level_tile_summaries()

    print("Creating Daily Level bounding box tiles")
    ym.daily_level_bbox()


class YMController():
    def __init__(self, args):
        self.ds = args.ds
        self.output = args.output

        self.parquet_file = f"ym_{self.ds}.parquet"
        self.con = duckdb.connect()

    
    def load_credentials(self):
        self.con.sql("""
            INSTALL spatial;
            LOAD spatial;
            INSTALL aws; 
            LOAD aws;
            -- INSTALL h3 FROM community;
            -- LOAD h3;
            CALL load_aws_credentials('ym');
        """)
                
    def download_parquet(self):
        # Download the parquet from S3 and add h3 cell indexes to the changesets
        if not os.path.isfile(self.parquet_file):
            self.con.sql(f"""
                COPY(
                    SELECT
                        *
                    FROM READ_PARQUET('s3://youthmappers-internal-us-east1/query_results/parquet/ds={self.ds}/*', hive_partitioning=1)
                ) TO '{self.parquet_file}';
            """)
        else:
            print(f"File exists: {self.parquet_file}")

    def daily_activity(self):
        self.con.sql(f"""
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
                    FROM READ_PARQUET('{self.parquet_file}')
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
        """)

    def daily_chapter_activity(self, zoom_level: int=8):
        self.con.sql(f"""
            COPY(
                SELECT 
                    CAST(_day as varchar) as date,
                    chapter,
                    q{zoom_level} AS quadkey,
                    CAST(amenities AS int) AS amenities,
                    CAST(buildings AS int) AS buildings,
                    CAST(highways AS int) AS highways,
                    CAST(all_feats - amenities - buildings - highways AS int) AS other,
                    ST_CENTROID(points) AS geometry,
                FROM (
                    SELECT 
                        _day,
                        chapter,
                        substr(quadkey,1,{zoom_level}) as q{zoom_level},
                        ST_Union_Agg(ST_GeomFromWKB(centroid)) as points,
                        sum(new_amenities + edited_amenities) as amenities,
                        sum(new_buildings + edited_buildings) as buildings,
                        sum(new_highways + edited_highways) as highways,
                        sum(new_features + edited_features) as all_feats,
                        count(distinct(uid)) as mappers
                    FROM READ_PARQUET('{self.parquet_file}')
                        WHERE centroid IS NOT NULL
                    GROUP BY _day, substr(quadkey,1,{zoom_level}), chapter
                )
                ORDER BY _day, chapter, q{zoom_level} ASC
            ) TO 'daily_chapter_activity.geojson' WITH (FORMAT GDAL, DRIVER "GeoJSON");
        """)

    def tile_level_weekly_summaries(self, zoom_level: int=8):
        """
        Create the zoom level summaries by week
        """
        self.con.sql(f"""
            COPY(
                SELECT
                    arbitrary(chapter_id) as chapter_id,
                    CAST(epoch(date_trunc('week',_day)) AS int) as timestamp,
                    CAST(sum(edited_features) + sum(new_features) AS BIGINT) as all_feats,
                    ST_CENTROID(ST_Union_Agg(ST_GeomFromWKB(centroid))) AS geometry
                FROM READ_PARQUET('{self.parquet_file}')
                WHERE centroid IS NOT NULL
                GROUP BY date_trunc('week',_day), uid, substr(quadkey,1,{zoom_level})
            ) TO 'aggregated_by_zoom/z{zoom_level}_weekly.geojsonseq' WITH (FORMAT GDAL, DRIVER "GeoJSONSeq");
        """)

    def daily_level_tile_summaries(self):
        # -- Write the per-user, per-tile, per-day results:
        self.con.sql(f"""
            COPY(
                SELECT
                    chapter_id,
                    CAST(epoch(_day) AS int) as timestamp,
                    edited_features + new_features AS all_feats,
                    edited_amenities + new_amenities AS amenities,
                    edited_buildings + new_buildings AS buildings,
                    edited_highways + new_highways AS highways,
                    ST_GeomFromWKB(centroid) AS geometry
                FROM READ_PARQUET('{self.parquet_file}')
                WHERE centroid IS NOT NULL
            ) TO 'aggregated_by_zoom/z15_daily.geojsonseq' WITH (FORMAT GDAL, DRIVER "GeoJSONSeq");
        """)

    def daily_level_bbox(self):
        self.con.sql(f"""
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
                FROM READ_PARQUET('{self.parquet_file}')
                WHERE centroid IS NOT NULL
            ) TO 'aggregated_by_zoom/z15_daily_bboxes.geojsonseq' WITH (FORMAT GDAL, DRIVER "GeoJSONSeq");
        """)

if __name__=='__main__':

    main()