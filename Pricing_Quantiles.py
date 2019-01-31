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
data = sqlctxt.sql("SELECT * from h011pxo.pxo_myview_pricing_1")

#data.show()

## Changing date column to required format
data = data.withColumn('nav_date', f.from_unixtime(f.unix_timestamp('nav_date', 'MM/dd/yyy')).alias('nav_date'))
data = data.withColumn('nav_date',data.nav_date.cast(DateType()))

## Changing Datetime format 
data = data.withColumn('date_time',f.from_unixtime(f.unix_timestamp('date_time','MM/dd/yy HH:mm')))
data = data.withColumn('date_time',data.date_time.cast(TimestampType()))

data = data.filter((data.nav_date >= '2018-06-01') & (data.nav_date <= '2018-08-31'))

#Remove extra space from FundIds
#data = data.withColumn('ticker', regexp_replace('ticker',' ',''))
data = data.withColumn('fund_id', regexp_replace('fund_id',' ',''))


data_trainnew = data.select('client_name','fund_id','date_time','event_time_numeric')
#data_trainnew = data_trainnew.dropDuplicates()

ClientFund = data_trainnew.select('client_name','fund_id','event_time_numeric')
ClientFund = ClientFund.dropDuplicates()


# fund and ticker list
#TickerList = data_trainnew.select("ticker").distinct()
FundList = data_trainnew.select("fund_id").distinct()


# splitting date and time form datetime column
data_trainnew = data_trainnew.withColumn('Date',f.split('date_time'," ").getItem(0))
data_trainnew = data_trainnew.withColumn('time',f.split('date_time'," ").getItem(1))


df_stats = data_trainnew


data_trainnew_DT = data_trainnew.groupby('client_name','fund_id','Date','event_time_numeric').agg(f.count('fund_id').alias("fund_count"))
data_trainnew_pivot = data_trainnew_DT.fillna(0)


df_fund_Count  = data_trainnew_pivot.groupby('fund_id').agg(f.count('fund_count'))
df_uniqfund = data_trainnew.dropDuplicates(['FUND_ID','Date','event_time_numeric'])


# count of dates 
fund_count = df_uniqfund.groupby(df_uniqfund.fund_id).agg(f.countDistinct('Date').alias('fund_count'))

df_fundCount = df_uniqfund.groupby('fund_id','Date').agg(f.count('fund_id').alias('fund_count'))
df_fundCount = df_fundCount.withColumn('fund_count',df_fundCount['fund_count'].cast(StringType()))
df_uniqfund_pivot = df_fundCount.fillna(0)


df_fund_Count  = df_uniqfund_pivot.groupby('fund_id').agg(f.sum('fund_count').alias('fund_count'))
df_fundCount = df_fund_Count.withColumn('fund_count',df_fund_Count['fund_count'].cast(IntegerType()))


def percent_calculator(values,rank):
  values = [float(i) for i in values]
  value = []
  for i in range(len(values)):
    value.append(values[i])
    value.sort()
  if (rank == 50):
    length = len(value)
    if (length % 2 == 0):
      med = (value[int(length/2) -1] + value[int(length/2)])/ float(2)
    else:
      med = value[length/2]
    return med
  else:
    values.sort()
    import math
    R = (rank / float(100 )) * (len(value) - 1)
    if (type(R) is int):
      IR = int(R)
      return IR
    else:
      FR = math.modf(R)[0]
      high = int(math.ceil(R))
      low = int(math.floor(R))
      IR = value[low] + (value[high] - value[low]) * FR
    return IR
udf_percent_rank = f.udf(percent_calculator,FloatType())

def get_time(decimal_time):
    sec = decimal_time * 86400
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    return str(datetime.time(hour = int(h), minute= int(m), second = int(s)))
udf_string_time = f.udf(get_time,StringType())


#fundss less than week
fund_count = df_uniqfund.groupby(df_uniqfund.fund_id).agg(f.countDistinct('Date').alias('fund_count'))
less_than_week_funds = fund_count.filter(fund_count['fund_count'] <= 5)

less_than_week_funds = less_than_week_funds.select(f.collect_set('fund_id')).collect()[0][0]
less_than_week_funds = [str(less_than_week_funds) for less_than_week_funds in less_than_week_funds]

#df_uniqTicker30 = df_uniqTicker.where(df_uniqTicker['ticker'].isin(df_Ticker30['ticker']))


df_less = df_uniqfund.where(df_uniqfund.fund_id.isin(less_than_week_funds))

#df_less = df_uniqTicker30

df_less_quantiles = df_less.groupby('client_name','fund_id').agg(f.collect_list("event_time_numeric").alias("lis_2_event_time_numeric"))
df_less_quantiles = df_less_quantiles.withColumn('Quantile01(fund_MinimumTime)',udf_percent_rank('lis_2_event_time_numeric',f.lit(1)))
df_less_quantiles = df_less_quantiles.withColumn('Predicted_Time',udf_percent_rank('lis_2_event_time_numeric',f.lit(50)))

#df_less_quantiles = df_less_quantiles.withColumn('Quantile01(Ticker_MinimumTime)', df_less_quantiles['Quantile01(Ticker_MinimumTime)'].cast(IntegerType()))
#df_less_quantiles = df_less_quantiles.withColumn('Predicted_Time',df_less_quantiles['Predicted_Time'].cast(IntegerType()))

df_less_quantiles = df_less_quantiles.withColumn('Quantile01(fund_MinimumTime)',udf_string_time('Quantile01(fund_MinimumTime)'))
df_less_quantiles = df_less_quantiles.withColumn('Predicted_Time',udf_string_time('Predicted_Time'))

df_less_week = df_less_quantiles.select('client_name','fund_id','Quantile01(fund_MinimumTime)','Predicted_Time')


#Stats
df_new_1 = df_stats.select(['client_name','fund_id','date_time','date'])
df_new_1 = df_new_1.withColumn('time',f.split('date_time'," ").getItem(1))

beforetime = df_new_1.filter((df_new_1.time <'18:05:00'))
aftertime = df_new_1.filter((df_new_1.time >'18:05:00'))

TOtal = df_new_1.groupby(df_new_1.fund_id).agg(f.countDistinct('date')).alias('date')
df_total = TOtal.withColumnRenamed('count(DISTINCT date)','Totaldays')

maxtimebefore = beforetime.groupBy('fund_id').agg(f.max('time').alias('time'))
maxtimebefore = maxtimebefore.withColumnRenamed('time','MaxTime(before 18:05)')

maxtime = df_new_1.groupBy('fund_id').agg(f.max('time').alias('time'))
maxtime = maxtime.withColumnRenamed('time','MaxTime')

df_miss = aftertime.groupby(aftertime.fund_id).agg(f.countDistinct('date')).alias('date')
df_miss = df_miss.withColumnRenamed('count(DISTINCT date)','NoofMissdays')



df_merged1 = df_less_week.join(df_total, on = ['fund_id'], how = 'left')
df_merged2 = df_merged1.join(df_miss, on = ['fund_id'], how = 'left')
df_merged3 = df_merged2.join(maxtimebefore, on = ['fund_id'], how = 'left')
df_merged4 = df_merged3.join(maxtime, on = ['fund_id'], how = 'left')
df_merged4_final = df_merged4.fillna(0)
#df_merged4 is the dataframe for less than week and stas (5 days)


# Volume Code(< week)
df_merged4_final = df_merged4_final.withColumnRenamed('Predicted_Time','predicted_time')
df_merged4_final  = df_merged4_final.select('client_name','predicted_time','MaxTime')

df_merged4_final = df_merged4_final.withColumn("predicted_time",df_merged4_final.predicted_time.cast(TimestampType()))
df_merged4_final = df_merged4_final.withColumn("predicted_time",df_merged4_final.predicted_time - f.expr("interval 1 second"))
df_merged4_final = df_merged4_final.withColumn("predicted_time",(f.unix_timestamp(f.col("predicted_time")) - (f.unix_timestamp(f.col("predicted_time")) % f.lit(60)) + f.lit(60)).cast(TimestampType()))

df_merged4_final = df_merged4_final.withColumn("MaxTime",df_merged4_final['MaxTime'].cast(TimestampType()))
df_merged4_final = df_merged4_final.withColumn("MaxTime",df_merged4_final['MaxTime'] - f.expr("interval 1 second"))
df_merged4_final = df_merged4_final.withColumn("MaxTime",(f.unix_timestamp(f.col("MaxTime")) - (f.unix_timestamp(f.col("MaxTime")) % f.lit(60)) + f.lit(60)).cast(TimestampType()))
df_merged4_final = df_merged4_final.withColumn('weekday',f.lit(1))

#Client volume for pricing > week (>5days)
# For predicted Time
data_1_less = df_merged4_final.groupBy('client_name','weekday','Predicted_Time',f.window("Predicted_Time", "1 minutes")).agg({'Predicted_Time':'count'}).withColumnRenamed('count(Predicted_Time)','PredictedT_Count')
union_df_predicted_v_less = data_1_less.drop('window')
union_df_predicted_v_less = union_df_predicted_v_less.withColumn('predicted_time',f.split("predicted_time"," ").getItem(1)) 

# For Maxtime Time
union_df_predicted_2_v_less = df_merged4_final.groupBy('client_name','weekday','MaxTime',f.window("MaxTime", "1 minutes")).agg({'MaxTime':'count'}).withColumnRenamed('count(MaxTime)','MaxTime_Count')
union_df_predicted_2_v_less = union_df_predicted_2_v_less.drop('window')
union_df_predicted_2_v_less = union_df_predicted_2_v_less.withColumn('MaxTime',f.split("MaxTime"," ").getItem(1)) 
union_df_predicted_2_v_less = union_df_predicted_2_v_less.withColumnRenamed('MaxTime','predicted_time')

temp_less = df_merged4_final.groupBy('client_name','weekday').max('weekday')
testing_less = temp_less.drop('max(weekday)')
temp_less = sc.parallelize([i for i in range(0,181)])
temp_less = temp_less.map(lambda x: (x, )).toDF(['bins'])

initial_time_less = '16:00:00'
temp_less = temp_less.withColumn('bin',f.lit(initial_time_less).cast(TimestampType()))

def bin_time(bins,bin):
  import pandas as pd
  import datetime
  return bin + datetime.timedelta(minutes = int(bins)) 
udf_bin_time = f.udf(bin_time,TimestampType())


new_df_less = temp_less.withColumn('bin',udf_bin_time(temp_less.bins,temp_less.bin)).drop('bins')
new_df_less = new_df_less.withColumn('predicted_time',f.split("bin"," ").getItem(1)).drop('bin') 


temp_less = testing_less.crossJoin(new_df_less)

union_df_predicted_v_less = union_df_predicted_v_less.join(temp_less, on= ['client_name','predicted_time','weekday'],how= 'outer')
union_df_predicted_v_less = union_df_predicted_v_less.fillna(0)
union_df_predicted_2_v_less = union_df_predicted_2_v_less.join(temp_less, on= ['client_name','predicted_time','weekday'],how= 'outer')
union_df_predicted_2_v_less = union_df_predicted_2_v_less.fillna(0)

final_predicted_less = union_df_predicted_v_less.join(union_df_predicted_2_v_less,on = ['client_name','weekday','predicted_time'])

final_predicted_less = final_predicted_less.withColumn('predicted_time',f.regexp_replace('predicted_time','15:59:00','16:00:00'))


## Using window function to calculate cumulative sum
windowval_4 = Window.partitionBy('client_name').orderBy('predicted_time').rangeBetween(Window.unboundedPreceding, 0)

final_predicted_less = final_predicted_less.withColumn('cumsum', f.sum('PredictedT_Count').over(windowval_4))
final_predicted_less = final_predicted_less.withColumn('cumsum', final_predicted_less.cumsum.cast(IntegerType()))
final_predicted_less = final_predicted_less.withColumnRenamed('cumsum','Bin_Volume_Predicted_Time')

final_predicted_less = final_predicted_less.withColumn('cumsum', f.sum('MaxTime_Count').over(windowval_4))
final_predicted_less = final_predicted_less.withColumn('cumsum', final_predicted_less.cumsum.cast(IntegerType()))
final_predicted_less = final_predicted_less.withColumnRenamed('cumsum','Bin_Volume_MaxTime')
final_predicted_less = final_predicted_less.withColumnRenamed('predicted_time','Time')

final_predicted_less = final_predicted_less.select('client_name','Time','Bin_Volume_Predicted_Time','Bin_Volume_MaxTime')
final_predicted_less.show()



# Rolling up to the client Level.
#df_client = df_merged4_final.groupBy('fund_id','client_name','Quantile01(fund_MinimumTime)','Predicted_Time','MaxTime').agg(f.max('Predicted_Time').alias('Predicted_Time_max'))
##df_client = df_client.withColumnRenamed('Quantile01(Fund_MinimumTime)','Client_MinimumTime')
##df_client = df_client.withColumnRenamed('Alert Time','Client_AlertTime')
#df_client = df_client.select('client_name','Quantile01(fund_MinimumTime)','Predicted_Time','MaxTime')
#df_client_1 = df_client.fillna(0)
##df_client is the ClientPredictPricing_JunAug_18_weekless
#
#
##converting time into bins
#def bin(x,y):
#  if x == 16:
#    y = (x * 0) + (y * 1)
#  elif x == 17:
#    y = (1 * 60) + (y * 1)
#  else:
#    y = (2 * 60) + (y * 1)
#  return y
#bin_udf = f.udf(bin,IntegerType())
#def re_bin(x):
#  return (x / 5) + 1
#rebin_udf = f.udf(re_bin,IntegerType())
#
##union_df = df_merged4_final 
## Adding extra time for smooth Time series data,resample will start  from 15:59:00 to 19:04:00' so that time starts from 16:00:00
#
#df = sc.parallelize([['000','000','15:59:00','15:59:00',0,0,'15:59:00','15:59:00'],
#                     ['000','000','19:04:00','19:04:00',0,0,'19:04:00','19:04:00']]).\
#    toDF(["fund_id","client_name","Quantile01(Ticker_MinimumTime)","Predicted_Time","Totaldays","NoofMissdays","MaxTime(before 18:05)","MaxTime"])
#  
#union_df = df_merged4_final.unionAll(df)
#
## For predicted Time 
#union_df_1 = union_df.withColumn('bin',bin_udf(f.hour('Predicted_Time'),f.minute('Predicted_Time')).cast(IntegerType()))
#union_df_1 = union_df_1.withColumn('Bin',rebin_udf(union_df_1.bin).cast(IntegerType()))
#
#
#union_df_predicted = union_df_1.groupBy('fund_id','Bin',f.window("Predicted_Time", "5 minutes")).agg({'Predicted_Time':'count'}).withColumnRenamed('count(Predicted_Time)','PredictedT_Count')
#union_df_predicted = union_df_predicted.drop('window')
#
### Using window function to calculate cumulative sum
#windowval = Window.partitionBy('fund_id').orderBy('Bin').rangeBetween(Window.unboundedPreceding, 0)
#union_df_predicted = union_df_predicted.withColumn('cumsum', f.sum('PredictedT_Count').over(windowval))
#df_bin_sorted = union_df_predicted.withColumn('cumsum', union_df_predicted.cumsum.cast(IntegerType()))
#df_bin_sorted = df_bin_sorted.withColumnRenamed('cumsum','Bin_Volume_Predicted_Time')
#
#
##For Max Time < week (< 5 days)
#union_df_2 = union_df.withColumn('bin',bin_udf(f.hour('MaxTime'),f.minute('MaxTime')).cast(IntegerType()))
#union_df_2 = union_df_2.withColumn('Bin',rebin_udf(union_df_2.bin).cast(IntegerType()))
#
#union_df_predicted_2 = union_df_2.groupBy('fund_id','Bin',f.window("MaxTime", "5 minutes")).agg({'MaxTime':'count'}).withColumnRenamed('count(MaxTime)','MaxTime_Count')
#union_df_predicted_2 = union_df_predicted_2.drop('window')
#
### Using window function to calculate cumulative sum
#windowval_2 = Window.partitionBy('fund_id').orderBy('Bin').rangeBetween(Window.unboundedPreceding, 0)
#union_df_predicted_2 = union_df_predicted_2.withColumn('cumsum', f.sum('MaxTime_Count').over(windowval_2))
#df_bin_sorted_2 = union_df_predicted_2.withColumn('cumsum', union_df_predicted_2.cumsum.cast(IntegerType()))
#df_bin_sorted_2 = df_bin_sorted_2.withColumnRenamed('cumsum','Bin_Volume_MaxTime')

#Need to be checked for merging for < week for tikcer Volume
#df_sorted_final  = df_bin_sorted.join(df_bin_sorted_2,on = ['Bin','client_name','ticker'],how = 'inner')
#df_sorted_final = df_sorted_final.select('client_name','Bin','Bin_Volume_MaxTime','Bin_Volume_Predicted_Time')


##Client roll up volume
##df_client
#df_c = sc.parallelize([['000','15:59:00','15:59:00','15:59:00'],
#                     ['000','19:04:00','19:04:00','19:04:00']]).\
#    toDF(["client_name","Quantile01(Ticker_MinimumTime)","Predicted_Time","MaxTime"])
#union_df_c = df_client_1.unionAll(df_c)
#
##Predicted Time client roll up
#union_df_c_1 = union_df_c.withColumn('bin',bin_udf(f.hour('Predicted_Time'),f.minute('Predicted_Time')).cast(IntegerType()))
#union_df_c_1 = union_df_c_1.withColumn('Bin',rebin_udf(union_df_c_1.bin).cast(IntegerType()))
#
#union_df_predicted_c = union_df_c_1.groupBy('client_name','Bin',f.window("Predicted_Time", "5 minutes")).agg({'Predicted_Time':'count'}).withColumnRenamed('count(Predicted_Time)','PredictedT_Count')
#union_df_predicted_c = union_df_predicted_c.drop('window')
#
### Using window function to calculate cumulative sum
#windowval_c = Window.partitionBy('client_name').orderBy('Bin').rangeBetween(Window.unboundedPreceding, 0)
#union_df_predicted_c = union_df_predicted_c.withColumn('cumsum', f.sum('PredictedT_Count').over(windowval_c))
#df_bin_sorted_c = union_df_predicted_c.withColumn('cumsum', union_df_predicted_c.cumsum.cast(IntegerType()))
#df_bin_sorted_c = df_bin_sorted_c.withColumnRenamed('cumsum','Bin_Volume_Predicted_Time')
#
#
##For Max Time client rollup
#union_df_2_c = union_df_c.withColumn('bin',bin_udf(f.hour('MaxTime'),f.minute('MaxTime')).cast(IntegerType()))
#union_df_2_c = union_df_2_c.withColumn('Bin',rebin_udf(union_df_2_c.bin).cast(IntegerType()))
#union_df_predicted_2_c = union_df_2_c.groupBy('client_name','Bin',f.window("MaxTime", "5 minutes")).agg({'MaxTime':'count'}).withColumnRenamed('count(MaxTime)','MaxTime_Count')
#union_df_predicted_2_c = union_df_predicted_2_c.drop('window')
#
### Using window function to calculate cumulative sum
#windowval_2_c = Window.partitionBy('client_name').orderBy('Bin').rangeBetween(Window.unboundedPreceding, 0)
#union_df_predicted_2_c = union_df_predicted_2_c.withColumn('cumsum', f.sum('MaxTime_Count').over(windowval_2_c))
#df_bin_sorted_2_c = union_df_predicted_2_c.withColumn('cumsum', union_df_predicted_2_c.cumsum.cast(IntegerType()))
#df_bin_sorted_2_c = df_bin_sorted_2_c.withColumnRenamed('cumsum','Bin_Volume_MaxTime')



### Function to create Bin column from time
#def bin(x,y):
#  if x == 16:
#    y = (x * 0) + (y * 1)
#  elif x == 17:
#    y = (1 * 60) + (y * 1)
#  else:
#    y = (2 * 60) + (y * 1)
#  return y
#bin_udf = f.udf(bin,IntegerType())
#
#def re_bin(x):
#  return (x / 5) + 1
#rebin_udf = f.udf(re_bin,IntegerType())
#
#union_df = union_df.withColumn('bin',bin_udf(f.hour('Predicted_Time'),f.minute('Predicted_Time')).cast(IntegerType()))
#union_df = union_df.withColumn('Bin',rebin_udf(union_df.bin).cast(IntegerType()))
#
#
#union_df_predicted = union_df.groupBy('fund_id','Bin',f.window("Predicted_Time", "5 minutes")).agg({'Predicted_Time':'count'}).withColumnRenamed('count(Predicted_Time)','PredictedT_Count')
#union_df_predicted = union_df_predicted.drop('window')
#
### Using window function to calculate cumulative sum
#windowval = Window.partitionBy('fund_id').orderBy('Bin').rangeBetween(Window.unboundedPreceding, 0)
#union_df_predicted = union_df_predicted.withColumn('cumsum', f.sum('PredictedT_Count').over(windowval))
#df_bin_sorted = union_df_predicted.withColumn('cumsum', union_df_predicted.cumsum.cast(IntegerType()))
#df_bin_sorted = df_bin_sorted.withColumnRenamed('cumsum','Bin_Volume_Predicted_Time')
#
#
##For Max Time
#union_df_2 = union_df.withColumn('bin2',bin_udf(f.hour('MaxTime'),f.minute('MaxTime')).cast(IntegerType()))
#union_df_2 = union_df_2.withColumn('Bin2',rebin_udf(union_df_2.bin2).cast(IntegerType()))
#
#union_df_predicted_2 = union_df_2.groupBy('fund_id','Bin2',f.window("MaxTime", "5 minutes")).agg({'MaxTime':'count'}).withColumnRenamed('count(MaxTime)','MaxTime_Count')
#union_df_predicted_2 = union_df_predicted_2.drop('window')
#
### Using window function to calculate cumulative sum
#windowval_2 = Window.partitionBy('fund_id').orderBy('Bin2').rangeBetween(Window.unboundedPreceding, 0)
#union_df_predicted_2 = union_df_predicted_2.withColumn('cumsum', f.sum('MaxTime_Count').over(windowval_2))
#df_bin_sorted_2 = union_df_predicted_2.withColumn('cumsum', union_df_predicted_2.cumsum.cast(IntegerType()))
#df_bin_sorted_2 = df_bin_sorted_2.withColumnRenamed('cumsum','Bin_Volume_MaxTime')
#
#df_sorted_final  = df_bin_sorted_2.join(df_bin_sorted,on='fund_id',how = 'inner')
#df_sorted_final = df_sorted_final.select('fund_id','Bin_Volume_MaxTime','Bin_Volume_Predicted_Time')
#
##client roll up for <(5 days)week NDS
#
##df_Fundnew = df_merged4_final.groupBy('client_name','fund_id','Quantile01(Ticker_MinimumTime)','Predicted_Time','Totaldays','NoofMissdays','MaxTime(before 18:05)','MaxTime').agg(f.max('Predicted_Time').alias('Predicted_Time_max'))
##df_Fundnew_1 = df_Fundnew.dropDuplicates()
##df_Fundnew_1= df_Fundnew_1.withColumnRenamed('MaxTime','Fund_Max_PredictedTime')
##df_Fundnew_1 = df_Fundnew_1.withColumnRenamed('Quantile01(Ticker_MinimumTime)','Quantile01(Fund_MinimumTime)')
##df_Fundnew_1 = df_Fundnew_1.select('client_name','fund_id','Quantile01(Fund_MinimumTime)','Predicted_Time','Totaldays','NoofMissdays','MaxTime(before 18:05)','Fund_Max_PredictedTime')
####df_Fundnew_1 is the fund_PredictNDS_JunAug_18_weekless.
#
#df_client = df_merged4_final.groupBy('client_name','fund_id','Quantile01(Fund_MinimumTime)','Predicted_Time','Totaldays','NoofMissdays','MaxTime(before 18:05)','MaxTime').agg(f.max('Predicted_Time').alias('Predicted_Time_max'))
#df_client_1 = df_client.dropDuplicates()
#df_client_1 = df_client_1.select('client_name','Predicted_Time','MaxTime(before 18:05)','MaxTime')
###df_client_1 is the ClientPredictpricing_JunAug_18_weekless.
#
##df_merged4_final
#
## Adding extra time for smooth Time series data,resample will start  from 15:59:00 to 19:04:00' so that time starts from 16:00:00
#
#df_c = sc.parallelize([['000','15:59:00','15:59:00','15:59:00'],
#                     ['000','19:04:00','19:04:00','19:04:00']]).\
#       toDF(["client_name","Predicted_Time","MaxTime(before 18:05)","MaxTime"])
#  
#
#union_df_c = df_client_1.unionAll(df_c)
#
#
#### Function to create Bin column from time
##def bin(x,y):
##  if x == 16:
##    y = (x * 0) + (y * 1)
##  elif x == 17:
##    y = (1 * 60) + (y * 1)
##  else:
##    y = (2 * 60) + (y * 1)
##  return y
##bin_udf = f.udf(bin,IntegerType())
##
##def re_bin(x):
##  return (x / 5) + 1
##rebin_udf = f.udf(re_bin,IntegerType())
#
#union_df_c = union_df_c.withColumn('bin3',bin_udf(f.hour('Predicted_Time'),f.minute('Predicted_Time')).cast(IntegerType()))
#union_df_c = union_df_c.withColumn('Bin3',rebin_udf(union_df_c.bin3).cast(IntegerType()))
#
#
#union_df_predicted_c = union_df_c.groupBy('client_name','Bin3',f.window("Predicted_Time", "5 minutes")).agg({'Predicted_Time':'count'}).withColumnRenamed('count(Predicted_Time)','PredictedT_Count')
#union_df_predicted_c = union_df_predicted_c.drop('window')
#
### Using window function to calculate cumulative sum
#windowval = Window.partitionBy('client_name').orderBy('Bin3').rangeBetween(Window.unboundedPreceding, 0)
#union_df_predicted_c = union_df_predicted_c.withColumn('cumsum', f.sum('PredictedT_Count').over(windowval))
#df_bin_sorted_c = union_df_predicted_c.withColumn('cumsum', union_df_predicted_c.cumsum.cast(IntegerType()))
#df_bin_sorted_c = df_bin_sorted_c.withColumnRenamed('cumsum','Bin_Volume_Predicted_Time')
#
#
##For Max Time
#union_df_2_c = union_df_c.withColumn('bin4',bin_udf(f.hour('MaxTime'),f.minute('MaxTime')).cast(IntegerType()))
#union_df_2_c = union_df_2_c.withColumn('Bin4',rebin_udf(union_df_2_c.bin4).cast(IntegerType()))
#
#union_df_predicted_2_c = union_df_2_c.groupBy('client_name','Bin4',f.window("MaxTime", "5 minutes")).agg({'MaxTime':'count'}).withColumnRenamed('count(MaxTime)','MaxTime_Count')
#union_df_predicted_2_c = union_df_predicted_2_c.drop('window')
#
### Using window function to calculate cumulative sum
#windowval_2 = Window.partitionBy('client_name').orderBy('Bin4').rangeBetween(Window.unboundedPreceding, 0)
#union_df_predicted_2_c = union_df_predicted_2_c.withColumn('cumsum', f.sum('MaxTime_Count').over(windowval_2))
#df_bin_sorted_2_c = union_df_predicted_2_c.withColumn('cumsum', union_df_predicted_2_c.cumsum.cast(IntegerType()))
#df_bin_sorted_2_c = df_bin_sorted_2_c.withColumnRenamed('cumsum','Bin_Volume_MaxTime')
#
#df_sorted_final_c  = df_bin_sorted_2_c.join(df_bin_sorted_c,on='client_name',how = 'inner')
#df_sorted_final_c  = df_sorted_final_c.select('client_name','Bin_Volume_MaxTime','Bin_Volume_Predicted_Time')


#funds more than week(> 5 days)                             
more_than_week_funds = fund_count.filter(fund_count['fund_count'] > 5)
more_than_week_funds = more_than_week_funds.select(f.collect_set('fund_id')).collect()[0][0]
more_than_week_funds = [str(more_than_week_funds) for more_than_week_funds in more_than_week_funds]

df_more  = df_uniqfund.where(df_uniqfund.fund_id.isin(more_than_week_funds))


#df_more = df_uniqTicker30_more

#Finding Outliers 
test_1 = df_more.groupby('client_name','fund_id').agg(f.collect_list('event_time_numeric').alias('lis_event_time_numeric'))

test_1= test_1.withColumn('q1',udf_percent_rank(test_1.lis_event_time_numeric,f.lit(25)))
test_1= test_1.withColumn('q3',udf_percent_rank(test_1.lis_event_time_numeric,f.lit(75)))

test_1=test_1.cache()
test_1 = test_1.withColumn('iqr',test_1.q3 - test_1.q1)

test_1 = test_1.withColumn('fence_low',test_1.q1 - f.lit(3) * test_1.iqr)
test_1 = test_1.withColumn('fence_high',test_1.q3 + f.lit(3) * test_1.iqr)


test2 = test_1.join(df_more,on=['fund_id','client_name'],how='right')
#test1.show()

#df_outlier is the outliers and df_out is without outlier 
df_out_2 = test2.filter((test2.event_time_numeric > test2.fence_low) & (test2.event_time_numeric < test2.fence_high))
df_outlier_2 = test2.filter((test2.event_time_numeric < test2.fence_low) & (test2.event_time_numeric > test2.fence_high))
                         
df_out_moreweek =  df_out_2.select('client_name','fund_id','event_time_numeric')  
                              
#df_more_less = df_out_moreweek.groupBy('ticker','client_name','fund_id').agg(f.min('event_time_numeric').alias('seconds'))
df_more_quantiles = df_out_moreweek.groupby('client_name','fund_id').agg(f.collect_list("event_time_numeric").alias("lis_event_time_numeric"))

df_more_quantiles = df_more_quantiles.withColumn('Quantile01(fund_MinimumTime)',udf_percent_rank('lis_event_time_numeric',f.lit(1)))
df_more_quantiles = df_more_quantiles.withColumn('Quantile5',udf_percent_rank('lis_event_time_numeric',f.lit(5)))
df_more_quantiles = df_more_quantiles.withColumn('Quantile10',udf_percent_rank('lis_event_time_numeric',f.lit(10)))
df_more_quantiles = df_more_quantiles.withColumn('Quantile15',udf_percent_rank('lis_event_time_numeric',f.lit(15)))
df_more_quantiles = df_more_quantiles.withColumn('Quantile20',udf_percent_rank('lis_event_time_numeric',f.lit(20)))
df_more_quantiles = df_more_quantiles.withColumn('Predicted_Time',udf_percent_rank('lis_event_time_numeric',f.lit(50)))
df_more_quantiles = df_more_quantiles.withColumn('Quantile60',udf_percent_rank('lis_event_time_numeric',f.lit(60)))
df_more_quantiles = df_more_quantiles.withColumn('Quantile70',udf_percent_rank('lis_event_time_numeric',f.lit(70)))
df_more_quantiles = df_more_quantiles.withColumn('Quantile80',udf_percent_rank('lis_event_time_numeric',f.lit(80)))
df_more_quantiles = df_more_quantiles.withColumn('Quantile95',udf_percent_rank('lis_event_time_numeric',f.lit(95)))
df_more_quantiles = df_more_quantiles.withColumn('Quantile99(Max_PredictedTime)',udf_percent_rank('lis_event_time_numeric',f.lit(99)))

#df_more_month_quantiles = df_more_month_quantiles.withColumn('Alert Time1', split_col.getItem(1))
df_more_quantiles = df_more_quantiles.withColumn('Quantile01(fund_MinimumTime)',udf_string_time('Quantile01(fund_MinimumTime)'))
df_more_quantiles = df_more_quantiles.withColumn('Quantile5',udf_string_time('Quantile5'))
df_more_quantiles = df_more_quantiles.withColumn('Quantile10',udf_string_time('Quantile10'))
df_more_quantiles = df_more_quantiles.withColumn('Quantile15',udf_string_time('Quantile15'))
df_more_quantiles = df_more_quantiles.withColumn('Quantile20',udf_string_time('Quantile20'))
df_more_quantiles = df_more_quantiles.withColumn('Predicted_Time',udf_string_time('Predicted_Time'))
df_more_quantiles = df_more_quantiles.withColumn('Quantile60',udf_string_time('Quantile60'))
df_more_quantiles = df_more_quantiles.withColumn('Quantile70',udf_string_time('Quantile70'))
df_more_quantiles = df_more_quantiles.withColumn('Quantile80',udf_string_time('Quantile80'))
df_more_quantiles = df_more_quantiles.withColumn('Quantile95',udf_string_time('Quantile95'))
df_more_quantiles = df_more_quantiles.withColumn('Quantile99(Max_PredictedTime)',udf_string_time('Quantile99(Max_PredictedTime)'))
df_more_week = df_more_quantiles.select('client_name','fund_id','Quantile01(fund_MinimumTime)','Quantile5','Quantile10','Quantile15','Quantile20','Predicted_Time','Quantile60','Quantile70','Quantile80','Quantile95','Quantile99(Max_PredictedTime)')


#Stats more
df_new_1_more = df_stats.select(['client_name','fund_id','date_time','date'])
df_new_1_more = df_new_1_more.withColumn('time',f.split('date_time'," ").getItem(1))

beforetime_more = df_new_1_more.filter((df_new_1_more.time <'18:05:00'))
aftertime_more = df_new_1_more.filter((df_new_1_more.time >'18:05:00'))

TOtal_more = df_new_1_more.groupby(df_new_1_more.fund_id).agg(f.countDistinct('date')).alias('date')
df_total_more = TOtal_more.withColumnRenamed('count(DISTINCT date)','Totaldays')

maxtimebefore_more = beforetime_more.groupBy('fund_id').agg(f.max('time').alias('time'))
maxtimebefore_more = maxtimebefore_more.withColumnRenamed('time','MaxTime(before 18:05)')

maxtime_more = df_new_1_more.groupBy('fund_id').agg(f.max('time').alias('time'))
maxtime_more = maxtime.withColumnRenamed('time','MaxTime')

df_miss_more = aftertime_more.groupby(aftertime_more.fund_id).agg(f.countDistinct('date')).alias('date')
df_miss_more = df_miss_more.withColumnRenamed('count(DISTINCT date)','NoofMissdays')



df_merged1_more = df_more_week.join(df_total_more, on = ['fund_id'],how = 'left')
df_merged2_more = df_merged1_more.join(df_miss_more, on = ['fund_id'],how = 'left')
df_merged3_more = df_merged2_more.join(maxtimebefore_more, on = ['fund_id'],how = 'left')
df_merged4_more = df_merged3_more.join(maxtime_more, on = ['fund_id'],how = 'left')


df_merged4_more = df_merged4_more.withColumn('MaxTime(before 18:05)1',df_merged4_more['MaxTime(before 18:05)'])
df_merged4_more = df_merged4_more.withColumn('MaxTime(before 18:05)1',df_merged4_more['MaxTime(before 18:05)1'].cast(TimestampType()))

split_col = f.split(df_merged4_more['MaxTime(before 18:05)1'], ' ')
df_merged4_more = df_merged4_more.withColumn('Alert Time1', split_col.getItem(1))

df_merged4_more = df_merged4_more.withColumn('Alert Time1',df_merged4_more['Alert Time1'].cast(TimestampType()))

df_merged4_more = df_merged4_more.withColumn('Alert Time',df_merged4_more['Alert Time1'] - f.expr("INTERVAL 5 MINUTEs"))

split_col = f.split(df_merged4_more['Alert Time'], ' ')
df_merged4_more = df_merged4_more.withColumn('Alert Time', split_col.getItem(1))

columns_to_drop = ['MaxTime(before 18:05)1','Alert Time1']
df_merged4_more = df_merged4_more.drop(*columns_to_drop)
df_merged4_final_more = df_merged4_more.fillna(0)
#df_merged4_final_more is the dataframe for > week

# Volume Code
df_merged4_final_more = df_merged4_final_more.withColumnRenamed('Predicted_Time','predicted_time')
df_merged4_final_more = df_merged4_final_more.withColumnRenamed('Quantile99(Max_PredictedTime)','quantile99')
df_merged4_final_more  = df_merged4_final_more.select('client_name','predicted_time','quantile99')

df_merged4_final_more = df_merged4_final_more.withColumn("predicted_time",df_merged4_final_more.predicted_time.cast(TimestampType()))
df_merged4_final_more = df_merged4_final_more.withColumn("predicted_time",df_merged4_final_more.predicted_time - f.expr("interval 1 second"))
df_merged4_final_more = df_merged4_final_more.withColumn("predicted_time",(f.unix_timestamp(f.col("predicted_time")) - (f.unix_timestamp(f.col("predicted_time")) % f.lit(60)) + f.lit(60)).cast(TimestampType()))

df_merged4_final_more = df_merged4_final_more.withColumn("quantile99",df_merged4_final_more['quantile99'].cast(TimestampType()))
df_merged4_final_more = df_merged4_final_more.withColumn("quantile99",df_merged4_final_more['quantile99'] - f.expr("interval 1 second"))
df_merged4_final_more = df_merged4_final_more.withColumn("quantile99",(f.unix_timestamp(f.col("quantile99")) - (f.unix_timestamp(f.col("quantile99")) % f.lit(60)) + f.lit(60)).cast(TimestampType()))
df_merged4_final_more = df_merged4_final_more.withColumn('weekday',f.lit(1))

#Client volume for pricing > week (>5days)
# For predicted Time
data_1 = df_merged4_final_more.groupBy('client_name','weekday','Predicted_Time',f.window("Predicted_Time", "1 minutes")).agg({'Predicted_Time':'count'}).withColumnRenamed('count(Predicted_Time)','PredictedT_Count')
union_df_predicted_v = data_1.drop('window')
union_df_predicted_v = union_df_predicted_v.withColumn('predicted_time',f.split("predicted_time"," ").getItem(1)) 

# For Quantile99  Time
union_df_predicted_2_v = df_merged4_final_more.groupBy('client_name','weekday','quantile99',f.window("quantile99", "1 minutes")).agg({'quantile99':'count'}).withColumnRenamed('count(quantile99)','quantile99_Count')
union_df_predicted_2_v = union_df_predicted_2_v.drop('window')
union_df_predicted_2_v = union_df_predicted_2_v.withColumn('quantile99',f.split("quantile99"," ").getItem(1)) 
union_df_predicted_2_v = union_df_predicted_2_v.withColumnRenamed('quantile99','predicted_time')

temp = df_merged4_final_more.groupBy('client_name','weekday').max('weekday')
testing = temp.drop('max(weekday)')
temp = sc.parallelize([i for i in range(0,181)])
temp = temp.map(lambda x: (x, )).toDF(['bins'])

initial_time = '16:00:00'
temp = temp.withColumn('bin',f.lit(initial_time).cast(TimestampType()))

def bin_time(bins,bin):
  import pandas as pd
  import datetime
  return bin + datetime.timedelta(minutes = int(bins)) 
udf_bin_time = f.udf(bin_time,TimestampType())


new_df = temp.withColumn('bin',udf_bin_time(temp.bins,temp.bin)).drop('bins')
new_df = new_df.withColumn('predicted_time',f.split("bin"," ").getItem(1)).drop('bin') 


temp = testing.crossJoin(new_df)

union_df_predicted_v = union_df_predicted_v.join(temp, on= ['client_name','predicted_time','weekday'],how= 'outer')
union_df_predicted_v = union_df_predicted_v.fillna(0)
union_df_predicted_2_v = union_df_predicted_2_v.join(temp, on= ['client_name','predicted_time','weekday'],how= 'outer')
union_df_predicted_2_v = union_df_predicted_2_v.fillna(0)

final_predicted = union_df_predicted_v.join(union_df_predicted_2_v,on = ['client_name','weekday','predicted_time'])

final_predicted = final_predicted.withColumn('predicted_time',f.regexp_replace('predicted_time','15:59:00','16:00:00'))


## Using window function to calculate cumulative sum
windowval_3 = Window.partitionBy('client_name').orderBy('predicted_time').rangeBetween(Window.unboundedPreceding, 0)

final_predicted = final_predicted.withColumn('cumsum', f.sum('PredictedT_Count').over(windowval_3))
final_predicted = final_predicted.withColumn('cumsum', final_predicted.cumsum.cast(IntegerType()))
final_predicted = final_predicted.withColumnRenamed('cumsum','Bin_Volume_Predicted_Time')

final_predicted = final_predicted.withColumn('cumsum', f.sum('quantile99_Count').over(windowval_3))
final_predicted = final_predicted.withColumn('cumsum', final_predicted.cumsum.cast(IntegerType()))
final_predicted = final_predicted.withColumnRenamed('cumsum','Bin_Volume_Quantile99')
final_predicted = final_predicted.withColumnRenamed('predicted_time','Time')

final_predicted = final_predicted.select('client_name','Time','Bin_Volume_Predicted_Time','Bin_Volume_Quantile99')
final_predicted.show()

#final_predicted.write.mode("overwrite").insertInto("h011gtcsandbox.Pricing_moreweek_volume")



final_predicted.groupby('Time').agg(f.sum('Bin_Volume_Predicted_Time'),f.sum('Bin_Volume_Quantile99')).sort('Time').show(190)





























































## client roll up for more than a week (>5days)
#df_client_m = df_merged4_final_more.groupBy('fund_id','client_name','Predicted_Time','Quantile99(Max_PredictedTime)').agg(f.max('Predicted_Time').alias('Predicted_Time_max'))
#df_client_m_1 = df_client_m.dropDuplicates()
#df_client_m_1 = df_client_m_1.select('client_name','Predicted_Time','Quantile99(Max_PredictedTime)')
#df_client_m_1 = df_client_m_1.fillna(0)
##df_client_m is the ClientPredictNDS_JunAug_18_weekmore
#
#
##Ticker volume for NDS > week (>5days)
##union_df = df_merged4_final 
## Adding extra time for smooth Time series data,resample will start  from 15:59:00 to 19:04:00' so that time starts from 16:00:00
#df_m = sc.parallelize([['000','000','15:59:00','15:59:00','15:59:00','15:59:00','15:59:00','15:59:00','15:59:00','15:59:00','15:59:00','15:59:00','15:59:00',0,0,'15:59:00','15:59:00','15:59:00'],
#                       ['000','000',19:04:00','19:04:00','19:04:00','19:04:00','19:04:00','19:04:00','19:04:00','19:04:00','19:04:00','19:04:00','19:04:00',0,0,'19:04:00','19:04:00','19:04:00']]).\
#    toDF(["fund_id","client_name","Quantile01(Ticker_MinimumTime)",'Quantile5','Quantile10','Quantile15','Quantile20',"Predicted_Time",'Quantile60','Quantile70','Quantile80','Quantile95','Quantile99(Max_PredictedTime)',"Totaldays","NoofMissdays","MaxTime(before 18:05)","MaxTime",'Alert Time'])
#  
#union_df_m = df_merged4_final_more.unionAll(df_m)
#
#
## For predicted Time
#union_df_m_1 = union_df_m.withColumn('bin',bin_udf(f.hour('Predicted_Time'),f.minute('Predicted_Time')).cast(IntegerType()))
#union_df_m_1 = union_df_m_1.withColumn('Bin',rebin_udf(union_df_m_1.bin).cast(IntegerType()))
#
#
#union_df_predicted_v = union_df_m_1.groupBy('fund_id','Bin',f.window("Predicted_Time", "5 minutes")).agg({'Predicted_Time':'count'}).withColumnRenamed('count(Predicted_Time)','PredictedT_Count')
#union_df_predicted_v = union_df_predicted_v.drop('window')
#
### Using window function to calculate cumulative sum
#windowval_3 = Window.partitionBy('fund_id').orderBy('Bin').rangeBetween(Window.unboundedPreceding, 0)
#union_df_predicted_v = union_df_predicted_v.withColumn('cumsum', f.sum('PredictedT_Count').over(windowval_3))
#df_bin_sorted_v = union_df_predicted_v.withColumn('cumsum', union_df_predicted_v.cumsum.cast(IntegerType()))
#df_bin_sorted_v = df_bin_sorted_v.withColumnRenamed('cumsum','Bin_Volume_Predicted_Time')
#
#
##For Quantile99(Max_PredictedTime)
#union_df_2_v = union_df_m.withColumn('bin',bin_udf(f.hour('Quantile99(Max_PredictedTime)'),f.minute('Quantile99(Max_PredictedTime)')).cast(IntegerType()))
#union_df_2_v = union_df_2_v.withColumn('Bin',rebin_udf(union_df_2_v.bin).cast(IntegerType()))
#
#union_df_predicted_2_v = union_df_2_v.groupBy('fund_id','Bin',f.window("Quantile99(Max_PredictedTime)", "5 minutes")).agg({'Quantile99(Max_PredictedTime)':'count'}).withColumnRenamed('count(Quantile99(Max_PredictedTime))','Quantile99(Max_PredictedTime)_Count')
#union_df_predicted_2_v = union_df_predicted_2_v.drop('window')
#
### Using window function to calculate cumulative sum
#windowval_4 = Window.partitionBy('fund_id').orderBy('Bin').rangeBetween(Window.unboundedPreceding, 0)
#union_df_predicted_2_m = union_df_predicted_2_v.withColumn('cumsum', f.sum('Quantile99(Max_PredictedTime)_Count').over(windowval_4))
#df_bin_sorted_2_v = union_df_predicted_2_m.withColumn('cumsum', union_df_predicted_2_m.cumsum.cast(IntegerType()))
#df_bin_sorted_2_v = df_bin_sorted_2_v.withColumnRenamed('cumsum','Bin_Volume_Quantile99(Max_PredictedTime)')
#
##needs to be checked
##df_sorted_final_v  = df_bin_sorted_2_v.join(df_bin_sorted_v,on = ['client_name','ticker','Bin'],how =  'inner')
##df_sorted_final_v = df_sorted_final_v.select('client_name','Bin','Bin_Volume_Quantile99(Max_PredictedTime)','Bin_Volume_Predicted_Time')
##df_sorted_final_v.groupby('Bin').agg(f.sum('Bin_Volume_Quantile99(Max_PredictedTime)').alias('cumsum'),f.sum('Bin_Volume_Predicted_Time').alias('red_1')).show(100)
#
#
##Client roll up volume for > week (>5days)
#
## Adding extra time for smooth Time series data,resample will start  from 15:59:00 to 19:04:00' so that time starts from 16:00:00
#df_m_c = sc.parallelize([['000','15:59:00','15:59:00'],
#                       ['000','19:04:00','19:04:00']]).\
#    toDF(["client_name","Predicted_Time","Quantile99(Max_PredictedTime)"])
#  
#union_df_m_c = df_client_m_1.unionAll(df_m_c)
#
#
## For predicted Time> week 
#union_df_m_c_1 = union_df_m_c.withColumn('bin',bin_udf(f.hour('Predicted_Time'),f.minute('Predicted_Time')).cast(IntegerType()))
#union_df_m_c_1 = union_df_m_c_1.withColumn('Bin',rebin_udf(union_df_m_c_1.bin).cast(IntegerType()))
#union_df_predicted_v_c = union_df_m_c_1.groupBy('client_name','Bin',f.window("Predicted_Time", "5 minutes")).agg({'Predicted_Time':'count'}).withColumnRenamed('count(Predicted_Time)','PredictedT_Count')
#union_df_predicted_v_c = union_df_predicted_v_c.drop('window')
#
### Using window function to calculate cumulative sum
#windowval_5 = Window.partitionBy('client_name').orderBy('Bin').rangeBetween(Window.unboundedPreceding, 0)
#union_df_predicted_v_c = union_df_predicted_v_c.withColumn('cumsum', f.sum('PredictedT_Count').over(windowval_5))
#df_bin_sorted_v_c = union_df_predicted_v_c.withColumn('cumsum', union_df_predicted_v_c.cumsum.cast(IntegerType()))
#df_bin_sorted_v_c = df_bin_sorted_v_c.withColumnRenamed('cumsum','Bin_Volume_Predicted_Time')
#
##For Quantile99(Max_PredictedTime) > week
#union_df_2_v_c = union_df_m_c.withColumn('bin',bin_udf(f.hour('Quantile99(Max_PredictedTime)'),f.minute('Quantile99(Max_PredictedTime)')).cast(IntegerType()))
#union_df_2_v_c = union_df_2_v_c.withColumn('Bin',rebin_udf(union_df_2_v_c.bin).cast(IntegerType()))
#union_df_predicted_2_v_m = union_df_2_v_c.groupBy('client_name','Bin',f.window("Quantile99(Max_PredictedTime)", "5 minutes")).agg({'Quantile99(Max_PredictedTime)':'count'}).withColumnRenamed('count(Quantile99(Max_PredictedTime))','Quantile99(Max_PredictedTime)_Count')
#union_df_predicted_2_v_m = union_df_predicted_2_v_m.drop('window')
#
### Using window function to calculate cumulative sum
#windowval_6 = Window.partitionBy('client_name').orderBy('Bin').rangeBetween(Window.unboundedPreceding, 0)
#union_df_predicted_2_m_v = union_df_predicted_2_v_m.withColumn('cumsum', f.sum('Quantile99(Max_PredictedTime)_Count').over(windowval_6))
#df_bin_sorted_2_v_m = union_df_predicted_2_m_v.withColumn('cumsum', union_df_predicted_2_m_v.cumsum.cast(IntegerType()))
#df_bin_sorted_2_v_m = df_bin_sorted_2_v_m.withColumnRenamed('cumsum','Bin_Volume_Quantile99(Max_PredictedTime)')
#
##needs to be checked 
##df_sorted_final_v  = df_bin_sorted_2_v.join(df_bin_sorted_v,on = ['client_name','ticker','Bin'],how =  'inner')
##df_sorted_final_v = df_sorted_final_v.select('client_name','Bin','Bin_Volume_Quantile99(Max_PredictedTime)','Bin_Volume_Predicted_Time')
##df_sorted_final_v.groupby('Bin').agg(f.sum('Bin_Volume_Quantile99(Max_PredictedTime)').alias('cumsum'),f.sum('Bin_Volume_Predicted_Time').alias('red_1')).show(100)
#
#
#
#
#
##df_merged4_final_more is the dataframe for > week
# 
##############end for testing#####################