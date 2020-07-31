import pandas as pd
import numpy as np

import geohash as gh
from collections import Counter

from pyspark.sql import functions as F
from pyspark.sql.functions import collect_list,struct,col,substring,lit,udf

def load_basic_location_data(period,spark):
    '''
    get location raw data given a period
    inputs:
    * data_config: dict containing table name, cols, period
    * spark: spark obj
    output:
    * location_df: spark df, containing location info
    '''
    location_table = 'ddmr_stge.stge_lctn'
    keep_cols = ','.join(['msisdn_no','lat_id','long_id','srce_file_ts'])
    #from 0 o'clock, to last hour of end date
    
    start_time = str(period[0])+'000000'
    end_time = str(period[1])+'240000'
    
    location_sql = '''select {0} from {1} where srce_file_ts between {2} and {3}'''.format(keep_cols,location_table,start_time,end_time)
    location_df = spark.sql(location_sql)
    #substring to average out each hour's location data
    location_df = location_df.select(col("*"), substring(col("srce_file_ts"), 0,10).alias("timestamp"))
    
    #lat keet 5, lon keep 7, due to singapore special location
    location_df = location_df.select(col("*"), substring(col("lat_id"), 0,5).alias("lat"))
    location_df = location_df.select(col("*"), substring(col("long_id"), 0,7).alias("lon"))
    cols_output = ['msisdn_no','timestamp','lat','lon']
    
    location_df_output = location_df.select(*cols_output).dropna(how='any')

    #hourly aggregated
    df_grouped_hourly = location_df_output.groupBy('timestamp','msisdn_no').agg(struct(F.mean('lat'),F.mean('lon')).alias('points'))

    return df_grouped_hourly

def agg_geohash(period,spark):
    '''
    aggregate location points to period
    '''
    hourly_agg_sdf = load_basic_location_data(period,spark)
    #below is mobility without timestamp
    period_agg_loc_points = hourly_agg_sdf.groupBy('msisdn_no').agg(collect_list('points').alias('mobility_gene_points'))
    return period_agg_loc_points

def agg_geohash_w_time(period,spark):
    '''
    aggregate location points to period
    list of points with timestamp
    '''
    hourly_agg_sdf = load_basic_location_data(period,spark)
    #below is mobility with timestamp
    period_agg_loc_points_w_time = hourly_agg_sdf.groupBy('msisdn_no').agg(collect_list(struct('timestamp','points')).alias('mobility_gene_points_w_time'))
    
    return period_agg_loc_points_w_time

def row_processor_geohash(row):
    '''
    geohashing function to encode location points into
    hashed str values, geohash precision set to 6
    input: 
    * row: list of tuples
    output:
    * encoded_points: set of str
    '''
    #using set, to store unique points
    encoded_points = []
    for tup in row:
        encoded = gh.encode(tup[0],tup[1],precision=6)
        encoded_points.append(encoded)
        
    d = Counter(encoded_points)
    
    d_list = list(map(list, d.items()))

    return d_list

def generate_location_dicts(sampling_dates,spark):
    '''
    function to generate dictionary
    storing each person's unique encoded location
    input:
    * sampling_dates: list of int
    * spark: spark obj
    output:
    * list_of_d: list of dict, each dict is 1 day's mobility pattern
    '''
    list_of_d = []
    
    for date in sampling_dates:
        # create period
        period = [date,date]
        print("processing date %s"%date)
        agg_geohash_df = agg_geohash(period,spark)
        pdf = agg_geohash_df.toPandas()
        pdf['points']=pdf['mobility_gene_points'].apply(lambda x:row_processor_geohash(x))
        
        d = dict(zip(pdf.msisdn_no,pdf.points))
        list_of_d.append(d)
    
    return list_of_d

def row_processor_geohash_w_time(row,day_hours_list,night_hours_list):
    '''
    geohashing function to encode location points into
    hashed str values, geohash precision set to 6
    input: 
    * row: list of tuples
    output:
    * encoded_points: set of str
    '''
    #using set, to store unique points
    daytime_locs = []
    nighttime_locs = []
    
    for tup in row:
        time = tup[0]
        encoded = gh.encode(tup[1][0],tup[1][1],precision=6)
        
        if time[-2:] in day_hours_list:
            daytime_locs.append(encoded)
            
        elif time[-2:] in night_hours_list:
            nighttime_locs.append(encoded)
        
        else:
            #not in special hours, ignore
            continue
        
    daytime_dict = Counter(daytime_locs)
    nighttime_dcit = Counter(nighttime_locs)
    
    daytime_points = list(map(list, daytime_dict.items()))
    nighttime_points = list(map(list, nighttime_dcit.items()))

    return daytime_points,nighttime_points

def retrieve_major_stationary_points(row):
    '''
    row: tuple of lists
    '''
    daytime_stationary = row[0]
    nighttime_stationary = row[1]
    
    #only select the most voted point
    major_daytime_pt, major_nighttime_pt = 'none','none'
    
    if len(daytime_stationary) > 0:
        #str values
        major_daytime_pt = sorted(daytime_stationary, key=lambda x: x[1], reverse = True)[0][0]
    if len(nighttime_stationary) > 0:
        major_nighttime_pt = sorted(nighttime_stationary, key=lambda x: x[1], reverse = True)[0][0]
    
    return major_daytime_pt,major_nighttime_pt

def generate_stationary_points(period,spark,day_hours_list,night_hours_list):
    '''
    based on the special hours defined
    find daytime nighttime stay points
    '''
    period_agg_special_hours = agg_geohash_w_time(period,spark)
    
    period_agg_special_hours_pdf = period_agg_special_hours.toPandas()
    
    period_agg_special_hours_pdf['day_night_locations'] = period_agg_special_hours_pdf['mobility_gene_points_w_time'].apply(lambda x: row_processor_geohash_w_time(x,day_hours_list,night_hours_list))
    
    period_agg_special_hours_pdf['major_day_night_location'] = period_agg_special_hours_pdf['day_night_locations'].apply(lambda x:retrieve_major_stationary_points(x))
    
    return period_agg_special_hours_pdf