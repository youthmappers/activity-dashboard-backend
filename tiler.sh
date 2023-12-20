echo ""
echo "Beginning low zoom tile build with z8 aggregation:"
tippecanoe -fo z8.pmtiles -Z2 -z2 -l z8agg -P aggregated_by_zoom/z8_weekly.geojsonseq

echo "Z10 Aggregation: "
tippecanoe -fo z10.pmtiles -Z3 -z5 -l z10agg -P aggregated_by_zoom/z10_daily.geojsonseq

echo "Z15 Centroid Aggregation: "
tippecanoe -fo z15.pmtiles -Z6 -z6 -l z15agg -P aggregated_by_zoom/z15_daily.geojsonseq

echo "Z15 Bounding Box Aggregation: "
tippecanoe -fo z15_polygons.pmtiles -Z6 -z6 -l z15agg_bbox -P aggregated_by_zoom/z15_daily_bboxes.geojsonseq

# echo "Bundling tiles into one pmtiles archive"
# tile-join -fo ym_changesets.pmtiles aggregated_by_zoom/z8.mbtiles aggregated_by_zoom/z10.mbtiles aggregated_by_zoom/z15.mbtiles aggregated_by_zoom/z15_polygons.mbtiles