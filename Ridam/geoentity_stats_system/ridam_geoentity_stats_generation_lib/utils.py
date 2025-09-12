from rasterstats import zonal_stats #To zonal stats
import pandas as pd #To work on pandas dataframe
import numpy as np
import psycopg2 # For PostgreSQL Database Queries
import sys # For Runtime Environment
import json

from vedas_raster_lib.algo import get_raster
import traceback
import numpy as np
from pyproj import CRS


# ---------------- Database Connection ----------------
def get_db_connection(__Config):
    host = __Config["global_param"]["database"]["host"]
    username = __Config["global_param"]["database"]["username"]
    password = __Config["global_param"]["database"]["password"]
    port = __Config["global_param"]["database"]["port"]
    db = __Config["global_param"]["database"]["db"]
    print('before connection')
    try:
        __conn = psycopg2.connect(database=db, user=username, password=password, host=host, port=port)
        __conn.autocommit = True
        print('connection returned')
        return __conn
    except:
        print("Error", "DB Error, Please check DB Configuration once.")
        print('Error', "======Geo Entity Stats Generation Module is failed due to database connection. ======")
        sys.exit(1)


def transform_categorical_data(x, transform_dict):
    y = {}
    try:
        for x_key, x_value in x.items():
            found_bin = False
            for transform_key, transform_value in transform_dict['mappings'].items():
                if x_key in transform_value:
                    y[transform_key] = y.get(transform_key, 0) + x_value
                    found_bin = True
            if not found_bin and "unspecified" in transform_dict:
                y[transform_dict["unspecified"]] = y.get(transform_dict["unspecified"], 0) + x_value
    except Exception as e:
        print(str(e))
    return y


def ingest_df_values_to_DB_table(config, df, table, update_flag=False):
    conn = get_db_connection(config)
    cursor = conn.cursor()
    for stats_data in df.itertuples():
        try:
            insert_qry="INSERT INTO "+table+" (geoentity_id, geoentity_source_id, param_id, stats_value, valtimestamp) VALUES ('"+stats_data.geoentity_id+"',"+ str(stats_data.geoentity_source_id)+","+ str(stats_data.param_id)+",'"+ str(stats_data.stats_value)+"','"+ stats_data.valtimestamp+"');"
            print(insert_qry)
            cursor.execute(insert_qry)
        except psycopg2.Error as e:
            if update_flag and "duplicate" in e.pgerror:
                try:
                    update_qry = f"UPDATE {table} SET stats_value='{stats_data.stats_value}' " \
                                 f"WHERE geoentity_id='{stats_data.geoentity_id}' and geoentity_source_id={stats_data.geoentity_source_id} " \
                                 f"and param_id={stats_data.param_id} and valtimestamp='{stats_data.valtimestamp}'"
                    cursor.execute(update_qry)
                    print("Info", "Duplicate record updated with new values.")
                except (Exception, psycopg2.DatabaseError) as error:
                    print("Error", "Ingest issue %s" % error)
            else:
                print("Warning", "Ingest issue %s" % e)
    cursor.close()
    conn.close()


# ---------------- Pixel Area Computation ----------------
def get_pixel_area(affine, gdf):
    if CRS.from_user_input(gdf.crs).is_geographic:
        # Reproject to UTM automatically based on mean lon
        mean_lon = gdf.geometry.centroid.x.mean()
        utm_zone = int((mean_lon + 180) / 6) + 1
        utm_crs = f"EPSG:{32600 + utm_zone}"  # northern hemisphere, adjust if southern
        subset_proj = gdf.to_crs(utm_crs)
        mean_lat = subset_proj.geometry.centroid.y.mean()
        
        lat = mean_lat if mean_lat is not None else 0
        pixel_width_m = affine.a * (111320 * np.cos(np.deg2rad(lat)))
        pixel_height_m = abs(affine.e * 110540)
        return pixel_width_m * pixel_height_m
    else:
        mean_lat = gdf.geometry.centroid.y.mean()
        return abs(affine.a * affine.e)


# ---------------- Zonal Stats Calculation ----------------
def calc_zonal_stats(geoentity_source_id, param_id, param_unixtimestamp, geoentity_gdf, from_index, to_index, array, affine, nodata, param_stats, skip_null, is_categorical=False, transform_dict=None, convert_to_area=False):
    print('Calculating on ', from_index, to_index)
    subset_gdf = geoentity_gdf[from_index:to_index]
    

    pixel_area = get_pixel_area(affine, subset_gdf)
    
    if nodata is not None:
        array = array.astype('float32')
        array[array == nodata] = np.nan

    stats_list = zonal_stats(
        subset_gdf, array, affine=affine, nodata=nodata,
        all_touched=True, geojson_out=True,
        categorical=is_categorical, stats=param_stats if not is_categorical else None
    )

    param_db_output = []
    for feat in stats_list:
        props = feat['properties']
        geoentity_id = props.pop('geoentity_id', None)

        if not is_categorical and skip_null and props.get(param_stats[0]) is None:
            continue

        if is_categorical:
            if transform_dict is not None:
                props = transform_categorical_data(props, transform_dict)
            if convert_to_area:
                for k, v in list(props.items()):
                    props[f"{k}_area"] = v * pixel_area

        param_db_output.append([geoentity_id, geoentity_source_id, param_id, json.dumps(props), param_unixtimestamp])

    if len(param_db_output) > 0:
        print('Num Stats calculated', len(param_db_output))
        final_result = pd.DataFrame(param_db_output,columns=["geoentity_id", "geoentity_source_id", "param_id", "stats_value", "valtimestamp"])
        return final_result
    else:
        print('No Valid Stats')
        return None


# ---------------- Generate Stats for Bounding Box ----------------
def gen_stats_for_bbox(config, algo_id, algo_args, bbox, projection, gdf, geoentity_source_id, param_id, target_timestamp, param_stats, skip_null, is_categorical=False, transform_dict=None, convert_to_area=False):
    raster = None
    try:
        raster = get_raster(algo_id, algo_args, bbox=bbox, projection=projection)
    except Exception as e:
        print(e)
        traceback.print_exc()
        return

    if raster is None:
        return

    nodata = raster.nodata if raster.nodata is not None else -9999
    array = raster.read(1)
    affine = raster.transform

    record_chunk = config["global_param"]["database"]["processing_record_chunk"]
    record_batch = int(len(gdf) / record_chunk)
    record_remainder = len(gdf) % record_chunk
    print(record_chunk, record_batch, record_remainder)
    
    final_df = pd.DataFrame()  # Initialize final_df outside the loop
    for i in range(record_batch):
        print('Calculating stats for i =', i)
        result = calc_zonal_stats(geoentity_source_id, param_id, target_timestamp,
                    gdf, i*record_chunk, (i + 1) * record_chunk if record_batch > 0 else len(gdf), array, affine, nodata, param_stats, skip_null, is_categorical, transform_dict, convert_to_area)
        if result is not None:
            final_df = pd.concat([final_df, result], ignore_index=True)
        
    if record_remainder > 0:
        print('Calculating stats for rem =', record_remainder)   
        result = calc_zonal_stats(geoentity_source_id, param_id, target_timestamp,
                                gdf, record_batch * record_chunk, len(gdf), array, affine, nodata, param_stats, skip_null, is_categorical, transform_dict, convert_to_area)
        if result is not None:
            final_df = pd.concat([final_df, result], ignore_index=True)
    return final_df
        
        
