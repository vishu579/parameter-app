# -*- coding: utf-8 -*-
#------------------------------------#

#------------------------------------#
# Module Import                      #
#------------------------------------#
import json # To read config json

import geopandas as gpd # For GeoJSON
import psycopg2 # For PostgreSQL Database Queries
import sys # For Runtime Environment
import math
from sqlalchemy import create_engine,text
from urllib.parse import quote_plus
import pandas as pd
from shapely.errors import ShapelyDeprecationWarning
from shapely.geometry import box
# from concurrent.futures import ProcessPoolExecutor, wait
import warnings
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

from ridam_geoentity_stats_generation_lib.utils import gen_stats_for_bbox, ingest_df_values_to_DB_table
from ridam_geoentity_stats_generation_lib.geoentity_split_utils import clip_geoentities_to_bbox, merge_geoentity_stats



class GeoEntity_Stats_Generation:
    #------------------------------------#
    # Private Members: Config and Database                    #
    #------------------------------------#    
    __config_file_path=None
    __Config=None
    __processing_record_chunk=None
    __exit_status=0
       
    #------------------------------------#
    # Aux Methods                        #
    #------------------------------------#
    def sameVal(self, x):
        return x
    
    def __printMsg(self,opt,text):
        if opt=="Warning":
            print("\n[Warning]: "+text+"\n")
        elif opt=="Info":
            print("[Info]: "+text)
        elif opt=="Error":
            print("\n<Error> "+text+"\n")
        else:
            print("Unsupported option "+opt+" for prinitng.")
    
    
    def __getDataFrameFromGeoEntitySource(self,geo_entity_table,source_id,region_prefix_filter):
        """
        Purpose
        ----------
        This method will read Database config and will return Geopanda Data Frame.
        
        Parameters
        ----------
        geo_entity_table: name of Geoentity table
        source_id : geoentity_source_id

        Returns
        -------
        GDF : Geo Panda Data Frame
        """
        self.__printMsg("Info","---GeoEntity fetching for GeoEntity_Source_ID:"+str(source_id)+" is started---")
        DataFrame=None
        __DB = self.get_db_connection(return_connection=False)   
        try:
            entity_filter='|'.join(region_prefix_filter)    
            sql=text("SELECT geoentity_id,geom FROM "+geo_entity_table+" where geoentity_source_id="+str(source_id) +" and geoentity_id similar to '%("+entity_filter+")%'")            
            DataFrame=gpd.read_postgis(sql, __DB.connect())
        except:
            self.__exit_status=1
            self.__printMsg("Error", "Sorry unable to give frame from database, plese check DB parameters.")
            sys.exc_info()
            return # sys.exit(self.__exit_status)
        self.__printMsg("Info","---GeoEntity fetching for GeoEntity_Source_ID:"+str(source_id)+" is completed---")       
        if __DB is not None:
            __DB.dispose()
        return DataFrame
      

    def __get_geoentities_difference(self, geoentity_source_id, geoentity_ids, param_id, ts, stats_table, total_list):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            query = "SELECT geoentity_id FROM " +stats_table+" WHERE param_id='"+str(param_id)+"' and geoentity_source_id='" + str(geoentity_source_id) + "' "
            query += "and valtimestamp = '" + ts + "'::timestamp and geoentity_id in %(geoentity_id_tuple)s"
            cursor.execute(query, {'geoentity_id_tuple' : tuple(geoentity_ids)})
            geoentities=cursor.fetchall()
            geoentity_list = [geo[0] for geo in geoentities]
            print('Data present for ', geoentity_list)
            cursor.close()
            conn.close()
            total = set(total_list)
            present = set(geoentity_list)
            remaining = total.difference(present)
            remaining_list = list(remaining)
            return remaining_list
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.__exit_status=1
            cursor.close()
            conn.close()
            return None
    
    def __get_ParamID(self,param_name,param_table):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            param_id_qry="SELECT id FROM " +param_table+" WHERE param_name='"+param_name+"'"
            cursor.execute(param_id_qry)
            param_id=cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return param_id
        except (Exception, psycopg2.DatabaseError) as error:
            self.__exit_status=1
            cursor.close()
            conn.close()
            return None

    
    def __gen_geoentity_stats_from_raster_system(self, geoentity_param_time_stat_table,geoenity_source_id,geoentity_gdf,param,param_table, algo_args, target_timestamp):
        param_name=param["param_name"]
        param_id=self.__get_ParamID(param_name,param_table)
        config = self.__Config
        
        update_flag = param["update_flag"]

        
        algo_id = param.get('algo_id')
        param_stats = param["stats"]
        bbox = param['bbox']
        grid_size = param['gride_size']
        projection = param['projection']
        skip_null=param.get('skip_null',False)
        is_categorical = param.get('is_categorical', False)
        transform_dict = param.get('transforms', None)
        convert_to_area=param.get('convert_to_area', False)
        split_geoentities = param.get('split_geoentities', 0)
        
        x1, y1, x2, y2 = bbox
        x_count = math.ceil((x2-x1)/grid_size)
        y_count = math.ceil((y2-y1)/grid_size)
        
        if split_geoentities == 1:
            if 'count' not in param_stats and is_categorical is False:
                param_stats.append('count')
        #with ProcessPoolExecutor(max_workers=16) as exe:
        futures = []
        final_df = pd.DataFrame()

        for i in range(x_count):
            for j in range(y_count):
                x_start = x1 + i*grid_size
                y_start = y1 + j*grid_size
                x_end = x_start + grid_size if x_start + grid_size < x2 else x2
                y_end = y_start + grid_size if y_start + grid_size < y2 else y2
                grid_box = [x_start, y_start, x_end, y_end]
                geoentities_in_grid = geoentity_gdf.cx[x_start:x_end, y_start:y_end]
                if geoentities_in_grid.empty:
                    print('No geoentitties in', str(grid_box))
                    continue
                print(geoentities_in_grid)
                if split_geoentities == 1:
                    current_box = box(x_start, y_start, x_end, y_end)
                    geoentities_in_grid = clip_geoentities_to_bbox(geoentities_in_grid, current_box)
                    if geoentities_in_grid.empty:
                        print('No geoentitties afer intersection', str(grid_box))
                        continue

                geoentity_ids = list(geoentities_in_grid.get('geoentity_id'))
                geoentities = self.__get_geoentities_difference(geoenity_source_id, geoentity_ids, param_id, target_timestamp, geoentity_param_time_stat_table, geoentity_ids)
                if not geoentities:
                    print('Data generated for all these geoentities ...')
                    continue
                
                geoentities_for_calculation = geoentities_in_grid[geoentities_in_grid['geoentity_id'].isin(geoentities)]
                relaxed_bounds = list(geoentities_for_calculation.total_bounds)
                print('Relaxed', grid_box, relaxed_bounds)
                
                #f = exe.submit(
                df = gen_stats_for_bbox( config, algo_id, algo_args, relaxed_bounds, projection, geoentities_for_calculation, geoenity_source_id, param_id, target_timestamp,
                                            param_stats, skip_null, is_categorical, transform_dict, convert_to_area)
                if split_geoentities == 0 and df is not None and not df.empty:
                    ingest_df_values_to_DB_table(config, df, geoentity_param_time_stat_table, update_flag)
                elif split_geoentities == 1 and df is not None and not df.empty:
                    final_df = pd.concat([final_df, df], ignore_index=True)
                    # print(f.result())
                    #futures.append(f)
                #wait(futures)
        if split_geoentities == 1 and not final_df.empty:
            print('Ingesting final df of size ', len(final_df))
            merged_stats_df = merge_geoentity_stats(final_df, is_categorical)
            print('After merging, df size is ', len(merged_stats_df))
            print(merged_stats_df)
            ingest_df_values_to_DB_table(config, merged_stats_df, geoentity_param_time_stat_table, update_flag) 
        # exe.shutdown()
                
                
        # bbox = [71, 21.5, 73.5, 24] # [67, 7.5, 98, 38]
        
    
    def __gen_geoentity_stats(self,geoentity_param_time_stat_table,geoenity_source_id,geoentity_gdf,param,param_table, algo_args=None, target_timestamp=None):
        """
        Purpose
        ----------
        This method will generate stats for given geo_entity.
        
        Parameters
        ----------
        geoentity_gdf: GeoDataFrame which contain geo_entity information.
        param: Information for parameters, for which stats have to be generated.
       
        
        Returns
        -------
        Verbose
        """

        
        if param.get('source_type') is not None and param.get('source_type') == 'vedas_raster_system':
            self.__gen_geoentity_stats_from_raster_system(geoentity_param_time_stat_table,geoenity_source_id,geoentity_gdf,param,param_table, algo_args, target_timestamp)
            return
        


    def get_db_connection(self, return_connection=True):
        __Config = self.__Config
         #Global Param Loading      
        host=__Config["global_param"]["database"]["host"]
        username=__Config["global_param"]["database"]["username"]
        password=__Config["global_param"]["database"]["password"]
        port=__Config["global_param"]["database"]["port"]
        db=__Config["global_param"]["database"]["db"]        
        print('before connection')
        #Database Connection
        try:
            conn_url="postgresql://"+username+":"+quote_plus(password)+"@"+host+":"+str(port)+"/"+db
            if not return_connection:
                __DB = create_engine(conn_url)
                return __DB
            else:
                __conn=psycopg2.connect(database=db, user=username, password=password, host=host, port=port)
                __conn.autocommit = True
                print('connection returned')
                return __conn  
            
        except:
            self.__printMsg("Error"," DB Error, Please check DB Configuration once.")
            self.__exit_status=1
            self.__printMsg('Error',"======Geo Entity Stats Generation Module is failed due to database connection. ======")
            sys.exit(self.__exit_status)
    
   
    
    #------------------------------------#
    # Execution of Main Methods          #
    #------------------------------------#
    def generate_stats(self,config='config.json', algo_args=None, target_timestamp=None):    
        self.__printMsg("Info","======Geo Entity Stats Generation Module is Started.======")
        
        self.__config_file_path=config
        
        #Configuration File Loading
        try:
            config_file=open(self.__config_file_path)
        except:
            self.__exit_status=1
            self.__printMsg('Error', "Sorry config file doesn't exist.")
            # sys.exit(self.__exit_status)
        __Config = json.load(config_file)
        self.__Config = __Config
        config_file.close()
        
        geoentity_table=__Config["global_param"]["database"]["geoentity_table"]
        geoentity_stats_table=__Config["global_param"]["database"]["geoentity_stats_table"]
        param_table=__Config["global_param"]["database"]["param_table"]
        self.__processing_record_chunk = __Config["global_param"]["database"]["processing_record_chunk"]
    
        __GeoEntityStatsConfig=__Config["config"]
        
        mapping_type=__GeoEntityStatsConfig["mapping_type"]
        for mapping_key in __GeoEntityStatsConfig["mapping_keys_for_stats_gen"]:            
            mapping_obj=__GeoEntityStatsConfig["mapping"][mapping_type][mapping_key]
            if mapping_type=="entity_mapping":
                source_id=mapping_obj["source_id"]
                region_prefix_filter=mapping_obj["region_prefix_filter"]                
                mapping_gdf=self.__getDataFrameFromGeoEntitySource(geoentity_table,source_id,region_prefix_filter)            
                parameters=None
                if(type(mapping_obj["params"]) == str):
                    parameters=__GeoEntityStatsConfig["param_template"][mapping_obj["params"]]
                elif (type(mapping_obj["params"])== list):
                    parameters=mapping_obj["params"]
                for param in parameters:
                    self.__gen_geoentity_stats(geoentity_stats_table,source_id,mapping_gdf,param,param_table, algo_args, target_timestamp)
        
        self.__printMsg("Info","======Geo Entity Stats Generation Module is Completed.======")
        # sys.exit(self.__exit_status)
 
if __name__ == "__main__":
    obj=GeoEntity_Stats_Generation()    
    obj.generate_stats(sys.argv[1], {"dataset_id":"T3S1P1",  "from_time" : "20241116", "to_time" : "20241130", "merge_method" : "max", "indexes" : [1]}, "2024-11-23 00:00:00+05:30")

