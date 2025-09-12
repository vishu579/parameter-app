import sys
sys.path.append('/opt/vedas_env/lib/geoentity_stats_system')
sys.path.append('/opt/vedas_env/lib/raster_data_system')
sys.path.append('/opt/vedas_env/script/raster_data_system')
from ridam_geoentity_stats_generation_lib.stats_generation import GeoEntity_Stats_Generation
import os
from datetime import datetime, timedelta, date,timezone
import sys
import pytz
import time


def generate_stats(config_path, algo_args, target_ts):
    obj = GeoEntity_Stats_Generation()
    obj.generate_stats(config_path, algo_args, target_ts)

'''
def generate_time_stats(config_path, start_date=None, end_date=None, delta=5):
    if not os.path.isfile(config_path):
        print('Config file not present', config_path)
        return

    if start_date is None or end_date is None:
        current_date = date.today()
        start, end, target_ts = generate_previous_date_range(current_date, delta)
        start_str = start.strftime('%Y%m%d')
        end_str = end.strftime('%Y%m%d')
        target_ts_str = target_ts.strftime('%Y-%m-%d') + " 00:00:00+05:30"
        print(start_str, end_str, target_ts_str)
        algo_args = {"from_time" : start , "to_time" : end}
        generate_stats(config_path, algo_args, target_ts_str)
    else:
        dt = end_date
        while start_date < dt:
            start, end, target_ts = generate_previous_date_range(dt, delta)
            start_str = start.strftime('%Y%m%d')
            end_str =end.strftime('%Y%m%d')
            target_ts_str =target_ts.strftime('%Y-%m-%d') + " 00:00:00+05:30"
            dt = start
            algo_args = {"from_time" : start : "to_time" : end}
            generate_stats(config_path, algo_args, target_ts_str)
            
'''
if __name__ == '__main__':
# 	generate_stats('/home/sac/hydro_model_forecast_config.json', {'from_time':'20250102', 'to_time' : '20250102', 'dataset_id' : 'T1S2P1'}, '20250102 00:00:00+05:30')
    # from_time_str='20250101'
    # to_time_str='20250207'
    
    # Get today's date
    date_series = ['20230203', '20230222','20230509']
    # today = datetime.today()
    # print("today:",today)

    # # Subtract 2 days from today
    # two_days_ago = today - timedelta(days=2)

    # # Format the date as YYYYMMDD
    # from_time_str = two_days_ago.strftime('%Y%m%d')
    # print("from_time_str:",from_time_str)
    
    # to_time_str = datetime.today().strftime('%Y%m%d')
    # print("to_time_str:",to_time_str)
    
    # timezone=pytz.timezone('Asia/Kolkata')
    
    # from_time=datetime.strptime(from_time_str, "%Y%m%d")
    # to_time=datetime.strptime(to_time_str,"%Y%m%d")
    
    # from_time=timezone.localize(from_time)
    # to_time=timezone.localize(to_time)
    
    # current_time=from_time

    # for i in date_series:
    #     date_obj_from = datetime.strptime(i, "%Y%m%d")
    #     date_obj_from = date_obj_from.replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)
    #     formatted = date_obj_from.strftime("%Y%m%d %H:%M:%S%z")
    #
    #     date_obj_to = datetime.strptime(i, "%Y%m%d")
    #     date_obj_to = date_obj_to.replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)
    #     formatted = date_obj_to.strftime("%Y%m%d %H:%M:%S%z")
    #
    #     from_time = date_obj_from #- timedelta(days=1)
    #     to_time = date_obj_to #+ timedelta(days=1)
    #     current_time_str = formatted
    #     from_time = from_time.strftime("%Y%m%d")
    #     to_time = to_time.strftime("%Y%m%d")
    #     # while current_time <=to_time:
    #         # current_time_str=current_time.strftime("%Y%m%d %H:%M:%S%z")
    #         # current_date_str=current_time.strftime("%Y%m%d")
    #         # print('>>>>>>>', current_time_str,current_date_str)
    #         # current_time += timedelta(days=1)
    #     # while True:
    #     time.sleep(5)
    try:generate_stats('/home/sac/tintoda_maxar_tree_building_combine.json', {'from_time':"20230203", 'to_time' : "20230509", 'dataset_id' : 'T6S1P7'},"20230511" )
    except Exception as e:print("Error is ", e);pass

    # time.sleep(1800)
    # print("sleep 1800")
    
    
'''
    start = None
    end = None
    delta=5
    argv = sys.argv
    # Valid formats
    # 1) file_name config_path [from_date] [to_date] [delta]
    # 2) file_name config_path [delta]
    print(argv)
    config_path = argv[1]
    if len(argv) >= 4:
        start_dt = argv[2]
        end_dt = argv[3]
        start = datetime.strptime(start_dt, "%Y%m%d")
        end = datetime.strptime(end_dt, "%Y%m%d")
        if len(argv) == 5:
            delta = int(argv[4])
    print(start, end)
    if len(argv) == 3:
        delta = int(argv[2])
    if start is None or end is None:
        generate_stats(config_path, delta=delta)
    else:
        generate_stats(config_path, start_date=start, end_date=end, delta=delta)
'''
