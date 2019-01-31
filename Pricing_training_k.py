## import required python packages
from __future__ import print_function
import sys, re
from pyspark.sql import SparkSession,HiveContext
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql import Window
import numpy as np

## creating spark session
spark = SparkSession\
        .builder\
        .appName("client_pricing")\
        .getOrCreate()

## creating spark and hive context
sc = spark.sparkContext
sqlctxt = HiveContext(sc)

## reading dat from hive table
data = sqlctxt.sql("SELECT nav_date,fund_id,client_name,datetime,date_identifier from h011gtcsandbox.xnd_pricing_data")
data = data.dropna()

## changing date column to required format
data = data.withColumn('nav_date', f.from_unixtime(f.unix_timestamp('nav_date', 'MM/dd/yyy')).alias('nav_date'))
data = data.withColumn('nav_date',data.nav_date.cast(DateType()))


## changing datetime to required format
data = data.withColumn('datetime',f.from_unixtime(f.unix_timestamp('datetime','MM/dd/yy HH:mm')))
data = data.withColumn('datetime',data.datetime.cast(TimestampType()))

data = data.filter((data.nav_date >= '2017-10-02') & (data.nav_date <= '2018-04-31'))

## collecting date for partitioning
date = data.select(f.max('nav_date')).collect()[0][0]
year = date.year
month = date.month

if month < 10:
  month = '0' + str(month)
  
part_date = str(year) + str(month)

## extracting weekday column using date
data = data.withColumn('week_day',f.date_format('nav_date','E'))

sqlctxt.sql("set hive.exec.dynamic.partition.mode=nonstrict")
funds = data.filter(f.month('nav_date') == 4).select('fund_id').distinct()

funds.registerTempTable("fund_id")
#sqlctxt.sql("insert overwrite table h011gtcsandbox.xnd_pricing_fund_id PARTITION (YYYYMM = " + str(part_date) + ") select * from fund_id")

funds = funds.select(f.collect_set('fund_id')).collect()[0][0]
funds = [str(funds) for funds in funds]


data_without_funds = data.where(~data.fund_id.isin(funds))
funds_miss = data.filter( (data.fund_id == '2DEC'))
data_with_funds = data.where(data.fund_id.isin(funds))
data = data_with_funds.unionAll(funds_miss)

## function to convert weekday from string to numerical EX: Monday as 0 so on
def week_day(x):
  if x == "Mon":
    y = 0
  elif x == "Tue":
    y = 1 
  elif x == "Wed":
    y = 2
  elif x == "Thu":
    y = 3
  elif x == "Fri":
    y = 4
  return y
weekday = f.udf(week_day,IntegerType())

## Applying function to convert weekday from srting to numerical EX: Monday as 0 so on
data = data.withColumn('week_day',weekday(data.week_day))  

client_info = data.select('client_name').distinct().sort('client_name')
client_info = client_info.withColumn("client_identifier", f.row_number().over(Window.orderBy('client_name')))
client_info = client_info.select('client_identifier','client_name')



client_info.registerTempTable("client_df")
#sqlctxt.sql("insert overwrite table h011gtcsandbox.xnd_pricing_client_info PARTITION (YYYYMM = " + str(part_date) + ")  select * from client_df")
  
  
client_info = sqlctxt.sql("select client_identifier,client_name from h011gtcsandbox.xnd_pricing_client_info where yyyymm = '201804'")
client_info = client_info.drop_duplicates()
client_info = client_info.filter(client_info.client_name != 'NULL')

## joiining training data and client info dataframes
data = data.join(client_info,on='client_name',how='left')


## Grouping the data
#data = data.groupBy('nav_date','FUND_ID','Client_Name','client_identifier','week_day').agg({'datetime':'max'}).withColumnRenamed('max(datetime)','datetime')
data = data.dropna()


## Function to create Bin column from time
def bin(x,y):
  if x == 16:
    y = (x * 0) + (y * 1)
  elif x == 17:
    y = (1 * 60) + (y * 1)
  else:
    y = (2 * 60) + (y * 1)
  return y
bin_udf = f.udf(bin,IntegerType())

def re_bin(x):
  return (x / 5) + 1
rebin_udf = f.udf(re_bin,IntegerType())

data = data.withColumn('bin',bin_udf(f.hour('datetime'),f.minute('datetime')).cast(IntegerType()))
data = data.withColumn('Bin',rebin_udf(data.bin).cast(IntegerType()))


## Calculating the count for every 5 minutes
data = data.groupBy('nav_date','Bin',f.window("datetime", "5 minutes")).agg({'datetime':'count'}).withColumnRenamed('count(datetime)','Count')
data = data.drop('window')


## creating dummy dataframe to fill missing client identifier bin values with zero
temp = data.groupBy('nav_date',).max('Bin')
testing = temp.drop('max(Bin)')
temp = sc.parallelize([i for i in range(1,38)])
temp = temp.map(lambda x: (x, )).toDF(['Bin'])
temp = testing.crossJoin(temp)
temp = temp.withColumn('Bin',temp.Bin.cast(IntegerType()))


data = data.join(temp, on= ['nav_date','Bin'],how= 'outer')
data = data.fillna(0)

## Extracting weekday from nav_date column
data = data.withColumn('weekday',f.date_format('nav_date','E'))

## converting weekday from string to numerical EX: Monday as 0 so on
data = data.withColumn('weekday',weekday(data.weekday))

## Using window function to calculate cumulative sum
windowval = Window.partitionBy('nav_date').orderBy('Bin').rangeBetween(Window.unboundedPreceding, 0)
data = data.withColumn('cumsum', f.sum('count').over(windowval))
df_bin_sorted = data.withColumn('cumsum', data.cumsum.cast(IntegerType()))


###


w = Window().partitionBy('nav_date')
df_bin_sorted = df_bin_sorted.select("*", f.collect_list("cumsum").over(w).alias("group_cumsum"))


def cumsumpct_calculate(num,maximum):
  return num / float(np.max(maximum))
udf_cum_pct = f.udf(cumsumpct_calculate,FloatType())

df_bin_sorted = df_bin_sorted.withColumn('cumsumpct',f.format_number(udf_cum_pct(df_bin_sorted.cumsum,df_bin_sorted.group_cumsum),4))                                               
df_bin_sorted = df_bin_sorted.drop('group_cumsum')

###

## Function to convert Bin column to required format
def rebin(x):
  a = (x % 38) - 1
  y = 961 + ( a * 5)
  return y
rebin = f.udf(rebin)

## Applying the above function on Bin column
df_bin_sorted = df_bin_sorted.withColumn('Bin',rebin(df_bin_sorted.Bin).cast(IntegerType())).cache()





## Anamoly detection for the training data

w = Window().partitionBy('client_identifier','Bin').orderBy('nav_date')
df_complete = df_bin_sorted.select("*", f.lag("cumsum").over(w).alias("shift_cumsum"))

df_complete = df_complete.withColumn("diff", f.when(f.isnull(df_complete.cumsum - df_complete.shift_cumsum), 0)
                              .otherwise(f.abs(df_complete.cumsum - df_complete.shift_cumsum)))

df_complete = df_complete.groupby('client_identifier','weekday','Bin').agg(f.collect_list("cumsum").alias("Xbar"),f.collect_list("diff").alias("MRbar"))

#def find_median(values_list):
#    try:
#        median = np.median(values_list) #get the median of values in a list in each row
#        return round(float(median),2)
#    except Exception:
#        return None #if there is anything wrong with the given values

def find_median(values_list):
  median = np.median(values_list) #get the median of values in a list in each row
  return round(float(median),2)

median_finder = f.udf(find_median,FloatType())

df_complete = df_complete.withColumn("Xbar",f.ceil(median_finder("Xbar"))) 
df_complete = df_complete.withColumn("MRbar",f.ceil(median_finder("MRbar")))
df_complete = df_complete.withColumn("UCL_Individual",df_complete.Xbar + (f.lit(2.66 ) * df_complete.MRbar))
df_complete = df_complete.withColumn("LCL_Individual",df_complete.Xbar - (f.lit(2.66 ) * df_complete.MRbar))

def _is_outlier(cumsum, ucl,lcl):
        if lcl <= cumsum <= ucl:
            return 0
        return 1
outlier_udf = f.udf(_is_outlier)
      
df_complete = df_bin_sorted.join(df_complete,on=['client_identifier','weekday','Bin'])

df_complete = df_complete.withColumn('is_outlier',outlier_udf(df_complete.cumsum,df_complete.UCL_Individual,df_complete.LCL_Individual))


## Client Statistics at Weekday Level
## Calculating Standard deviation by Bin, Weekday and overall
bin_stdev = df_bin_sorted.groupby('client_identifier','weekday','Bin').agg(f.format_number(f.stddev('cumsum'),3).alias('StdDev'))

weekday_stdev = df_bin_sorted.groupby('client_identifier','weekday').agg(f.format_number(f.stddev('cumsum'),3).alias('StdDev'))
  
overall_stdev = df_bin_sorted.groupby('weekday').agg(f.format_number(f.stddev('cumsum'),3).alias('StdDev'))


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




####
df_predicted_pct = df_bin_sorted.groupby('client_identifier','weekday','Bin').agg(f.collect_list("cumsumpct").alias("group_cumsumpct"))
df_predicted_pct = df_predicted_pct.withColumn('Red_1_pct',udf_percent_rank('group_cumsumpct',f.lit(1)))
df_predicted_pct = df_predicted_pct.withColumn('Red_5_pct',udf_percent_rank('group_cumsumpct',f.lit(5)))
df_predicted_pct = df_predicted_pct.withColumn('Red_10_pct',udf_percent_rank('group_cumsumpct',f.lit(10)))
df_predicted_pct = df_predicted_pct.withColumn('Red_20_pct',udf_percent_rank('group_cumsumpct',f.lit(20)))
df_predicted_pct = df_predicted_pct.withColumn('Predicted_pct',udf_percent_rank('group_cumsumpct',f.lit(50)))
df_predicted_pct = df_predicted_pct.withColumn('Green_pct',udf_percent_rank('group_cumsumpct',f.lit(95)))




end_volumes = df_bin_sorted.groupby('client_identifier','weekday','nav_date').agg(f.max('cumsum').alias('cumsum'))
df_end_volumes = end_volumes.groupby('client_identifier','weekday').agg(f.collect_list("cumsum").alias("cumsum"))
df_end_volumes = df_end_volumes.withColumn("DEV",udf_percent_rank('cumsum',f.lit(50))).drop('cumsum')

df_end_volumes.registerTempTable("df_end_volume")
#sqlctxt.sql("insert overwrite table h011gtcsandbox.xnd_pricing_train_end_vol PARTITION (YYYYMM = " + str(part_date) + ")  select * from df_end_volume")

  

df_predicted = df_predicted_pct.join(df_end_volumes, on = ['client_identifier','weekday'], how = 'left')

df_predicted = df_predicted.withColumn('Red_1',f.round(df_predicted.Red_1_pct * df_predicted.DEV))
df_predicted = df_predicted.withColumn('Red_5',f.round(df_predicted.Red_5_pct * df_predicted.DEV))
df_predicted = df_predicted.withColumn('Red_10',f.round(df_predicted.Red_10_pct * df_predicted.DEV))
df_predicted = df_predicted.withColumn('Red_20',f.round(df_predicted.Red_20_pct * df_predicted.DEV))
df_predicted = df_predicted.withColumn('Predicted',f.round(df_predicted.Predicted_pct * df_predicted.DEV))
df_predicted = df_predicted.withColumn('Green',f.round(df_predicted.Green_pct * df_predicted.DEV))

df_results = df_predicted.select('client_identifier','weekday','Bin','Red_1','Red_5','Red_10','Red_20', 'Predicted')

predicted_overall = df_results.groupby('Bin','weekday').agg(f.sum("Red_1").alias("Red_1") ,\
                                                            f.sum("Red_5").alias("Red_5") ,\
                                                            f.sum("Red_10").alias("Red_10") ,\
                                                            f.sum("Red_20").alias("Red_20") ,\
                                                            f.sum("Predicted").alias("Predicted") )


#print (predicted_overall.sort('weekday','Bin').show(10000))

##Calculate Train error

df_merged_train = df_bin_sorted.join(df_predicted, on =['client_Identifier','weekday','Bin'], how = 'left')

def _get_error_type(error):    
    if error > 0:
        return 'Positive Error'
    elif error < 0 :
        return 'Negative Error'
    return 'No Error'
udf_error = f.udf(_get_error_type,StringType())


df_merged_train = df_merged_train.withColumn('Error',df_merged_train.Predicted - df_merged_train.cumsum)
df_merged_train = df_merged_train.withColumn('Absolute_Error',f.abs(df_merged_train.Error)) 
df_merged_train = df_merged_train.withColumn('Error_pct',f.abs(df_merged_train.Predicted_pct - df_merged_train.cumsumpct) * 100 ) 
df_merged_train = df_merged_train.withColumn('Error_type',udf_error(df_merged_train.Error))




Client_Error = df_merged_train.groupBy('client_identifier','weekday').agg(f.collect_list('Absolute_Error').alias('Absolute_Error_list'),\
                                                                         f.count('Absolute_Error').alias('count'),\
                                                                         f.mean('Absolute_Error').alias('mean'),\
                                                                         f.min('Absolute_Error').alias('min'),\
                                                                         f.max('Absolute_Error').alias('max'),\
                                                                         f.stddev('Absolute_Error').alias('stddev'))

Client_Error = Client_Error.withColumn('quarter',udf_percent_rank('Absolute_Error_list',f.lit(25)))
Client_Error = Client_Error.withColumn('median',udf_percent_rank('Absolute_Error_list',f.lit(50)))
Client_Error = Client_Error.withColumn('threefourth',udf_percent_rank('Absolute_Error_list',f.lit(75)))

Client_Error = Client_Error.drop('Absolute_Error_list')
Client_Error = Client_Error.select('weekday','client_identifier','count','stddev','mean','min','quarter','median','threefourth','max')



errors_at_bin = df_merged_train.groupBy('client_identifier','weekday','Bin').agg(f.collect_list('Absolute_Error').alias('Absolute_Error_list'),\
                                                                         f.count('Absolute_Error').alias('count'),\
                                                                         f.min('Absolute_Error').alias('min'),\
                                                                         f.max('Absolute_Error').alias('max'),\
                                                                         f.stddev('Absolute_Error').alias('stddev'))

errors_at_bin = errors_at_bin.withColumn('quarter',udf_percent_rank('Absolute_Error_list',f.lit(25)))
errors_at_bin = errors_at_bin.withColumn('mean',udf_percent_rank('Absolute_Error_list',f.lit(50)))
errors_at_bin = errors_at_bin.withColumn('threefourth',udf_percent_rank('Absolute_Error_list',f.lit(75)))


errors_pct_bin = df_merged_train.groupBy('client_identifier','weekday','Bin').agg(f.collect_list('Error_pct').alias('Absolute_Error_list'),\
                                                                         f.count('Error_pct').alias('count'),\
                                                                         f.min('Error_pct').alias('min'),\
                                                                         f.max('Error_pct').alias('max'),\
                                                                         f.stddev('Error_pct').alias('stddev'))

errors_pct_bin = errors_pct_bin.withColumn('quarter',udf_percent_rank('Absolute_Error_list',f.lit(25)))
errors_pct_bin = errors_pct_bin.withColumn('mean',udf_percent_rank('Absolute_Error_list',f.lit(50)))
errors_pct_bin = errors_pct_bin.withColumn('threefourth',udf_percent_rank('Absolute_Error_list',f.lit(75)))




actual_overall = df_bin_sorted.groupby('Bin','nav_date','weekday').agg(f.sum('cumsum').alias('cumsum'))

def create_alerts(Predicted,cumsum,Red_1,Red_5,Red_10,Red_20):
    volume = cumsum
    if  volume >= Predicted:
        return 'GREEN'
    elif  volume < Predicted and volume >= Red_20:
        return 'AMBER' 
    elif volume < Red_1:
        return 'RED_1'
    elif  volume < Red_5:
        return 'RED_5'
    elif  volume < Red_10:
        return 'RED_10'
    elif  volume < Red_20:
        return 'RED_20'
udf_alerts = f.udf(create_alerts)    

df_merged_train = df_merged_train.withColumn('Alerts',udf_alerts(df_merged_train.Predicted,df_merged_train.cumsum,df_merged_train.Red_1,df_merged_train.Red_5,df_merged_train.Red_10,df_merged_train.Red_20))

df_train_alerts = df_merged_train.groupby('Client_Identifier','weekday').pivot('Alerts').count()
df_train_alerts = df_train_alerts.withColumn('RED_5',df_train_alerts.RED_5 + df_train_alerts.RED_1)
df_train_alerts = df_train_alerts.withColumn('RED_10',df_train_alerts.RED_10 + df_train_alerts.RED_5)
df_train_alerts = df_train_alerts.withColumn('RED_20',df_train_alerts.RED_20 + df_train_alerts.RED_10)

client_day_count = df_bin_sorted.groupby('Client_Identifier','weekday').agg(f.count('Count').alias('Count'))

df_train_alerts_merged = df_train_alerts.join(client_day_count, on = ['client_identifier','weekday'])
df_train_alerts_merged = df_train_alerts_merged.withColumn('RED_Max_Count',df_train_alerts_merged.Count * f.lit(0.01))

df_predicted = df_predicted.drop('group_cumsumpct') 

df_predicted_new = df_predicted.withColumnRenamed('RED_1','Red_Limit')
df_predicted_new = df_predicted_new.select('Bin','client_identifier','weekday','Predicted','Red_Limit')
df_predicted_overall = df_predicted_new.groupby('Bin','weekday').agg(f.sum('Predicted').alias('Predicted'),f.sum('Red_Limit').alias('Red_Limit'))


df_bin = spark.read.csv('/user/p642336/Time_bins.csv',inferSchema=True,header = True)
df_predicted_merge = df_predicted_overall.join(df_bin,on = 'Bin')
df_predicted_merged = df_predicted_merge.drop('Bin')
df_predicted_merged = df_predicted_merged.withColumnRenamed('Time_Bin','Bin')
df_predicted_merged = df_predicted_merged.withColumnRenamed('Red_Limit','Lower_Limit')

predicted_overall.registerTempTable("predictions_overall")
sqlctxt.sql("insert overwrite TABLE h011gtcsandbox.xnd_pricing_predicted_overall PARTITION (YYYYMM = " + str(part_date) + ") select * from predictions_overall")

df_results.registerTempTable("predicted_results")
sqlctxt.sql("insert overwrite TABLE h011gtcsandbox.xnd_pricing_predicted_results PARTITION (YYYYMM = " + str(part_date) + ") select * from predicted_results")


Client_Error.registerTempTable("clients_error")
sqlctxt.sql("insert overwrite TABLE h011gtcsandbox.xnd_pricing_client_error PARTITION (YYYYMM = " + str(part_date) + ") select * from clients_error")




