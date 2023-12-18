echo ""
echo "Beginning low zoom tile build with z8 aggregation:"
tippecanoe -fo aggregated_by_zoom/z8.mbtiles -Z2 -z2 -l z8agg -P aggregated_by_zoom/z8_weekly.geojsonseq

echo "Z10 Aggregation: "
tippecanoe -fo aggregated_by_zoom/z10.mbtiles -Z3 -z5 -l z10agg -P aggregated_by_zoom/z10_daily.geojsonseq

echo "Z15 Aggregation: "
tippecanoe -fo aggregated_by_zoom/z15.mbtiles -Z6 -z6 -l z15agg -P aggregated_by_zoom/z15_daily.geojsonseq

echo "Bundling tiles into one pmtiles archive"
tile-join -fo ym_changesets.pmtiles aggregated_by_zoom/z8.mbtiles aggregated_by_zoom/z10.mbtiles aggregated_by_zoom/z15.mbtiles