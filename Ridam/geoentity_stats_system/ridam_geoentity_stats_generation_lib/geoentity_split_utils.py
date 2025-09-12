import geopandas as gpd # For GeoJSON
import json
from collections import defaultdict


def clip_geoentities_to_bbox(gdf, bbox):
    gdf = gdf.copy()
    gdf = gdf.rename_geometry("geometry")
    grid_gdf = gpd.GeoDataFrame(
        geometry=[bbox],
        crs="EPSG:4326"  # or update to match your dataset
    )
    if gdf.crs != grid_gdf.crs:
        grid_gdf = grid_gdf.to_crs(gdf.crs)
    clipped = gpd.overlay(gdf, grid_gdf, how="intersection")
    polygon_columns = gdf.columns
    if not clipped.empty:
        clipped = clipped[polygon_columns]
    print('clipped box', clipped)
    return clipped


def merge_by_weighted_mean(dicts):
    totals = defaultdict(float)
    weight_sums = defaultdict(float)
    total_counts = 0

    for d in dicts:
        d = json.loads(d)
        w = d.get("count", 1)   # weight from dict
        total_counts += w
        for k, v in d.items():
            if k != "count":
                totals[k] += v * w
                weight_sums[k] += w

    # compute weighted means
    result = {k: totals[k] / weight_sums[k] for k in totals}
    result["count"] = total_counts
    return json.dumps(result)


def merge_cxounts(dicts):
    totals = defaultdict(int)
    for d in dicts:
        d = json.loads(d)
        for k, v in d.items():
            totals[k] += v
    return json.dumps(dict(totals))


def merge_geoentity_stats(df, is_categorical=False):
    print('inital', df)
    if is_categorical is False:
        merged = df.groupby(['geoentity_id', 'geoentity_source_id', 'param_id', 'valtimestamp'], as_index=False).agg({
            'stats_value': lambda x: merge_by_weighted_mean(x)
        })
    else:
        merged = df.groupby(['geoentity_id', 'geoentity_source_id', 'param_id', 'valtimestamp'], as_index=False).agg({
            "stats_value": lambda s: merge_cxounts(s)
        })
    print('final', merged)
    return merged