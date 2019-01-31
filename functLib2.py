# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 02:45:50 2018

@author: p638855
"""

import pandas as pd
##get_ipython().magic('matplotlib inline')
import matplotlib.pyplot as pyplot
from pandas import ExcelWriter
import numpy as np 
from scipy.stats import norm
import xlrd
import matplotlib.pyplot as plt
##import datetime as dt
import warnings
warnings.filterwarnings('ignore')
import time, datetime
import operator
import scipy.stats as stats
import os
from datetime import time
from datetime import datetime as dt
from openpyxl import load_workbook

def convert_to_time(x):
    x_list = x.split(':')
    x_list[-1] = x_list[-1].split('.')[0]
    new_x = ':'.join(x_list)
    return pd.Timestamp(new_x).time()

def Add_newSheet(pathof_file,filename,dataf, sheetname):
    #path = r"C:\Users\fedel\Desktop\excelData\PhD_data.xlsx"
    path =   pathof_file+filename
    book = load_workbook(path)
    writer = pd.ExcelWriter(path, engine = 'openpyxl')
    writer.book = book
    dataf.to_excel(writer, sheet_name = sheetname)
    writer.save()
    writer.close()

def convertTime(t):
    '''Not Required'''
    x = time.strptime(t,'%H:%M:%S')
    #x=t
    return str(int(datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds()))

def convertToExcelTime(t):
    '''Excel converts total seconds to fraction of  day'''
    totalSeconds= t.hour * 3600 +t.minute *60 +t.second
    excel_time=totalSeconds/(24 * 3600)
    return(excel_time)

def read_exl_file(excelfile, sheetName):
    '''
    read excel file with ext exls not exsb.  Convert the file if requied before passing. Read the required sheet. 
    '''
    xl_train=pd.ExcelFile(excelfile,encoding='utf-8',low_memory=False)
    df_train=xl_train.parse(sheetName)
    return df_train
 
def QtTimeAlldayNoOutlier(temp_df_noOutlier,q): 
    '''Find quantile on data with no outlier, for all day'''
    ##temp_df_noOutlier['Total_Second']= temp_df_noOutlier['Time'].map(lambda x: convertTime(str(x)))
    ##QTime= temp_df_noOutlier['Seconds'].astype(int).quantile(q)
    QTime=temp_df_noOutlier['Seconds'].quantile(q , interpolation='nearest') 
    #QTime=np.int(np.round(QTime))
    Qt_Time= str(datetime.timedelta(seconds=QTime))
    return(Qt_Time)
 
def remove_outlier_Fundwise(df_uniqFund,Fundgroups,fundId,sigma):
    '''fundId wise outlier will be removed with IQR method sigma=3.0 or 1.5'''
    Fund_501=df_uniqFund.ix[Fundgroups.groups.get(str(fundId))]
    ##Fund_501['Seconds']=Fund_501['Time'].map(lambda x: convertTime(str(x)))
    q1 = Fund_501['Seconds'].astype(int).quantile(0.25)
    q3 = Fund_501['Seconds'].astype(int).quantile(0.75)
    iqr = q3-q1 #Interquartile range
    fence_low  = q1-sigma *iqr
    fence_high = q3+sigma*iqr
    df_out = Fund_501.loc[(Fund_501['Seconds'].astype(int) > fence_low) & (Fund_501['Seconds'].astype(int) < fence_high)]
    df_outlier= Fund_501.loc[(Fund_501['Seconds'].astype(int) < fence_low) | (Fund_501['Seconds'].astype(int)> fence_high)]
    df_outlier
    ##df_out.drop(['Seconds'],axis=1,inplace=True)
    return(df_out,df_outlier)
 
def MedianTimeAlldayNoOutlier(temp_df_noOutlier):#,df_uniqFund,Fundgroups):
    #temp_df_noOutlier['Seconds']= temp_df_noOutlier['Time'].map(lambda x: convertTime(str(x)))
    MedTime=temp_df_noOutlier['Seconds'].median()
    Median_Time= str(datetime.timedelta(seconds=MedTime))
    return(Median_Time)
    
def MedianTime (temp_df_noOutlier):#,df_uniqFund,Fundgroups):
    temp_df_noOutlier['Seconds']= temp_df_noOutlier['Time'].map(lambda x: convertTime(str(x)))
    MedTime=temp_df_noOutlier['Seconds'].median()
    Median_Time= str(datetime.timedelta(seconds=MedTime))
    return(Median_Time)
def StdDevi(temp_df_noOutlier):#,df_uniqFund,Fundgroups):
    #temp_df_noOutlier['Seconds']= temp_df_noOutlier['Time'].map(lambda x: convertTime(str(x)))
    std=temp_df_noOutlier['Seconds'].std()
    STD= str(datetime.timedelta(seconds=std))
    return(STD)
    
def Write_excel(df,filename): 
    writer = pd.ExcelWriter(filename+'.xlsx', engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer,index = False)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    
 
def remove_outlier(df_in, col_name):
    '''   
    Pass df, groped on Fund and time in seconds 
    '''
       
    q1 = df_in[col_name].quantile(0.25)
    q3 = df_in[col_name].quantile(0.75)
    iqr = q3-q1 #Interquartile range
    fence_low  = q1-1.5*iqr
    fence_high = q3+1.5*iqr
    df_out = df_in.loc[(df_in[col_name] > fence_low) & (df_in[col_name] < fence_high)]
    return df_out


def Write_excel2(df,filename): 
    writer = pd.ExcelWriter(filename+'.xlsx', engine='xlsxwriter',date_format="yyyy-dd-mm",datetime_format='hh:mm:ss')
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
     
def daywise_Fund_median(Fund_501,WDay):
    Fund_501_wday=Fund_501.groupby('Day')
    Fund_501_day1=Fund_501.ix[Fund_501_wday.groups.get(WDay)]
    Fund_501_day1['Seconds'] = Fund_501_day1['Time'].map(lambda x: convertTime(str(x)))
    MedTime=Fund_501_day1['Seconds'].median()
    dayMedianTime=str(datetime.timedelta(seconds=MedTime))
    return(dayMedianTime)

def daywise_Fund_quantile(Fund_501,WDay,q):
    Fund_501_wday=Fund_501.groupby('Day')
    Fund_501_day1=Fund_501.ix[Fund_501_wday.groups.get(WDay)]
    Fund_501_day1['Seconds'] = Fund_501_day1['Time'].map(lambda x: convertTime(str(x)))
    QTime=Fund_501_day1['Seconds'].astype(int).quantile(q)
    QTime=np.int(np.round(QTime))
    QTime=str(datetime.timedelta(seconds=QTime))
    return(QTime)

def QtTime(fundId,df_uniqFund,Fundgroups,q):    
    temp_df=df_uniqFund.ix[Fundgroups.groups.get(str(fundId))]
    Day_QTime=[]
    for Week_Day in range(0,5):
        try:
            temp_day_qt=daywise_Fund_quantile(temp_df,Week_Day,q)
        except:
            temp_day_qt='00:00:00'
        Day_QTime.append(temp_day_qt)
    #Fund_501=df_uniqFund.ix[Fundgroups.groups.get('0501')]
    #temp_df['Total_Second']=temp_df['Time'].map(lambda x: convertTime(str(x)))
    QTime=temp_df['Seconds'].astype(int).quantile(q)
    QTime=np.int(np.round(QTime))
    Qt_Time= str(datetime.timedelta(seconds=QTime))
    return(Qt_Time,Day_QTime)

def MedianTime(fundId,df_uniqFund,Fundgroups):    
    #print(fundId)
    temp_df=df_uniqFund.ix[Fundgroups.groups.get(str(fundId))]
    Day_meadTime=[]
    #Week_Day=4
    for Week_Day in range(0,5):
        try:
            temp_day_meadian=daywise_Fund_median(temp_df,Week_Day)
        except:
            temp_day_meadian='00:00:00'
        Day_meadTime.append(temp_day_meadian)
    ##temp_df['Total_Second']=temp_df['Time'].map(lambda x: convertTime(str(x)))
    MedTime=temp_df['Seconds'].median()
    Meadian_Time= str(datetime.timedelta(seconds=MedTime))
    return(Meadian_Time,Day_meadTime)
 
def FreqTime(fundId,df_uniqFund,Fundgroups):    
    temp_df=df_uniqFund.ix[Fundgroups.groups.get(str(fundId))]
    temp_df['HourMin'] = temp_df['Time'].map(lambda x: x.hour*60)+temp_df['Time'].map(lambda x: x.minute)
    ModeT=temp_df['HourMin'].mode()
    try:
        ModeTime=ModeT[0]
    except:
        ModeTime=0
    Mode_Time=pd.to_timedelta(ModeTime,unit='m')
    return(Mode_Time)
def Roll_Mean(x,w):
        b=len(x)
        last=b-w
        for  i in range(last):
            roll_mean= (sum(x[i:i+w]))/w
            print(roll_mean)
        return(roll_mean)
def MedianTime (temp_df_noOutlier):#,df_uniqFund,Fundgroups):
    temp_df_noOutlier['Seconds']= temp_df_noOutlier['Time'].map(lambda x: convertTime(str(x)))
    MedTime=temp_df_noOutlier['Seconds'].median()
    Median_Time= str(datetime.timedelta(seconds=MedTime))
    return(Median_Time)
def StdDevi(temp_df_noOutlier):#,df_uniqFund,Fundgroups):
    #temp_df_noOutlier['Seconds']= temp_df_noOutlier['Time'].map(lambda x: convertTime(str(x)))
    std=temp_df_noOutlier['Seconds'].std()
    STD= str(datetime.timedelta(seconds=std))
    return(STD)               