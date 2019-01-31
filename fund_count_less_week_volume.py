## import required python packages
from __future__ import print_function
import sys, re
from pyspark.sql import SparkSession,HiveContext
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql import Window
import pandas as pd
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import trim
import datetime
import numpy as np
from pyspark import SparkConf

sc_conf = SparkConf()
sc_conf.set('spark.executor.memory', '4g')
sc_conf.set('spark.num.executors', '6')
sc_conf.set('spark.executor.cores', '2')
sc_conf.set('spark.cores.max', '10')
sc_conf.set('spark.logConf', True)

## creating spark session
spark = SparkSession\
        .builder.config(conf=sc_conf)\
        .appName("pricing_Quantiles")\
        .getOrCreate()\
        
#stop here


## creating spark context and hive context
sc = spark.sparkContext
sqlctxt = HiveContext(sc)


## reading data from hive table
data = sqlctxt.sql("SELECT * from h011gtcsandbox.nds_week_less")

seconds = 300
seconds_window = f.from_unixtime(f.unix_timestamp('predicted_time') - f.unix_timestamp('predicted_time') % seconds)
data2= data.withColumn('5_minutes_window', seconds_window)

Fund_Medcount = data.groupBy('predicted_time',f.window('predicted_time',"5 minutes")).agg({'predicted_time':'count'}).withColumnRenamed('count(predicted_time)','PredictedT_Count')
Fund_Medcount

#
#select * from h011gtcsandbox.nds_week_less;



#Calculating the count for every 5 minutes
#data = data.groupBy('nav_date','client_identifier','Bin',f.window("datetime", "5 minutes")).agg({'datetime':'count'}).withColumnRenamed('count(datetime)','Count')
#data = data.drop('window')


def VolumeCount(df,Fund_Medcount):
    '''Adding extra time for smooth Time series data,resample will start  from 16:00:00
    to 19:00:00'''
    #df['MaxTime(before 18:05)'].fillna('00:00:00')
    #df['MaxTime(before 18:05)']=df['MaxTime(before 18:05)']
    #'Quantile20','MaxTime(before 18:05)',  
Fund_cols=['Quantile01(Fund_MinimumTime)', 'Quantile05',
       'Quantile10', 'Quantile15',  
       'Quantile60', 'Quantile70', 'Quantile80', 'Quantile95',
       'Fund_Max_PredictedTime', 'Alert Time',  
       'MaxTime']
    Fund_col=colnm[~colnm.isin(['Quantile50(Predicted_Time)','Ticker','Client Name', 'FUND_ID','Totaldays', 'NoofMissdays'])]
    TempVolume=Fund_Medcount[['Time','Bin_Volume_Predict']]  
    for cl in Fund_col:   
        #print(cl)
        try:
            #df[cl] = df[cl].apply(lambda x:convert_to_time(x) if not isinstance(x, type(datetime.time(16,5,3))) else x)
            df[cl]= pd.to_timedelta(df[cl])#.astype(str))
            df.set_index([cl])
            Temp_count= df[[cl]].resample('5T',on=cl).count()
            '''removing extra count added'''
            Temp_count[cl].iloc[0]=0
            Temp_count[cl].iloc[-1]=0 
            Cnt_nm=cl+'_Count'                             
            Temp_count.rename(columns={cl:Cnt_nm},inplace=True)
            Temp_count.reset_index(inplace=True)
            Vol_nm='Bin_Volume_'+cl
            Temp_count[Vol_nm]=Temp_count[Cnt_nm].cumsum()
            Temp_count.rename(columns={cl:'Time'},inplace=True)
            TempVolume=pd.merge(TempVolume,Temp_count[['Time',Vol_nm]],on='Time',how='left')
            TempVolume.reset_index(inplace=True)
            del TempVolume['index']
            Temp_count.reset_index(inplace=True)
            del Temp_count['index']
            Fund_Medcount=pd.merge(Fund_Medcount,Temp_count,on='Time',how='left')
        except:
            print(cl)
    return(TempVolume)