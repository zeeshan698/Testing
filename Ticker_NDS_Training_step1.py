# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Jun 21 03:48:59 2018

@author: p638855
"""
from  functLib import *
if __name__ == "__main__":  
    os.chdir("H:/Downloads/NDSData/NDS/")
    #path=r'H:\Downloads\NDSData\NDS'
    #os.chdir("H:/Downloads/NewData_Sep18/")
    # UniqueTickerWeekMore_NDS_JunAug_18_step1.xlsx
    df=pd.ExcelFile('UniqueTickerWeekMore_NDS_JunAug_18_step1.xlsx',index_col=None,datetime_format='hh:mm:ss')
    #df=pd.ExcelFile('UniqueTicker30NDS_JunAug_18_step1_v01.xlsx',index_col=None,datetime_format='hh:mm:ss')
    df_uniqTicker5=pd.read_excel(df,index_col=None)
    df_uniqTicker5.columns
    df_uniqTicker5.rename(columns={'Time_Num':'Time2'},inplace=True)
    df_uniqTicker5['Time']=pd.DatetimeIndex(df_uniqTicker5['DateTime']).time
    df_uniqTicker5['Seconds']=df_uniqTicker5['Time'].map(lambda t:t.hour * 3600 +t.minute *60 +t.second)                
    del df
    #######Group on Ticker
    Tickergroups = df_uniqTicker5.groupby('Ticker')
    df=pd.DataFrame()
    Tickerid=[]
    Fund_outlier=pd.DataFrame()
    Ticker_outlier=pd.DataFrame()
    Temp_Ticker_outlier=pd.DataFrame()
    Temp_Fund_outlier=pd.DataFrame()
    Ticker_Nooutlier=pd.DataFrame()
    Fund_Nooutlier=pd.DataFrame()
    Temp_Ticker_noOutlier=pd.DataFrame()
    Temp_Fund_noOutlier=pd.DataFrame()
    MedT=[]
    quntTime01=[]
    quntTime05=[]
    quntTime10=[]
    quntTime20=[]
    quntTime15=[]
    quntTime60=[]
    quntTime70=[]
    quntTime80=[]
    quntTime95=[]
    quntTime99=[]
    for ticker in Tickergroups.groups.keys():
        Tickerid.append(ticker)
        #Remove outlier Fundwise Daywise
        Temp_Ticker_noOutlier,Temp_Ticker_outlier=remove_outlier_Fundwise(df_uniqTicker5,Tickergroups,ticker,3.0)
        Ticker_outlier=pd.concat([Ticker_outlier,Temp_Ticker_outlier],ignore_index=True)
        Ticker_Nooutlier=pd.concat([Ticker_Nooutlier,Temp_Ticker_noOutlier],ignore_index=True)
        #Find Median of Time on Ticker wise all days processing timing after removing outliers
        #MedTime =MedianTimeAlldayNoOutlier(Temp_Ticker_noOutlier)
        #MedT.append(MedTime)
        #Find Quantile99 of Time on Ticker wise all days processing timing after removing outliers
        QTime01=QtTimeAlldayNoOutlier(Temp_Ticker_noOutlier,0.01)
        quntTime01.append(QTime01)
        QTime05=QtTimeAlldayNoOutlier(Temp_Ticker_noOutlier,0.05)
        quntTime05.append(QTime05)
        QTime10=QtTimeAlldayNoOutlier(Temp_Ticker_noOutlier,0.10)
        quntTime10.append(QTime10)
        QTime15=QtTimeAlldayNoOutlier(Temp_Ticker_noOutlier,0.15)
        quntTime15.append(QTime15)
        QTime20=QtTimeAlldayNoOutlier(Temp_Ticker_noOutlier,0.20)
        quntTime20.append(QTime20)
        QTime70=QtTimeAlldayNoOutlier(Temp_Ticker_noOutlier,0.70)
        quntTime70.append(QTime70)
        QTime60=QtTimeAlldayNoOutlier(Temp_Ticker_noOutlier,0.60)
        quntTime60.append(QTime60)
        QTime80=QtTimeAlldayNoOutlier(Temp_Ticker_noOutlier,0.80)
        quntTime80.append(QTime80)
        QTime95=QtTimeAlldayNoOutlier(Temp_Ticker_noOutlier,0.95)
        quntTime95.append(QTime95)
        QTime99 =QtTimeAlldayNoOutlier(Temp_Ticker_noOutlier,0.99)
        quntTime99.append(QTime99)
    #outlier Analysis
    Ticker_Out_count=pd.DataFrame({'TickerOutCount':Ticker_outlier.groupby('Ticker')['Ticker'].count()})
    Ticker_Out_count.reset_index(inplace=True)
    Ticker_Out_count.to_csv("TickerwiseOutliers_countNDS_JunAug18_step1.csv")
    Ticker_outlier.to_csv("Ticker_outlierAlldaysNDS_JunAug18_step1.csv",index=False)
    Ticker_outlier.groupby('DateTime')['Ticker'].count()
    Write_excel(Ticker_outlier,'TickerOutlier_DateTime_NDS_JunAug_18_step1')
	 #Writing data after removal of outliers
    Ticker_Nooutlier.shape
    Write_excel(Ticker_Nooutlier,'TickerNoOutlierNDS_JunAug_18_Weekmore_step1')
    ########       
    df['Ticker']=Tickerid
    df['Quantile01']=quntTime01
    df['Quantile05']=quntTime05
    df['Quantile10']=quntTime10
    df['Quantile15']=quntTime15
    df['Quantile20']=quntTime20
    df['Median_Time']=MedT
    df['Median_Time']=pd.to_datetime(df['Median_Time']).dt.strftime("%H:%M:%S")
    df['Quantile60']=quntTime60
    df['Quantile70']=quntTime70
    df['Quantile80']= quntTime80
    df['Quantile95']=quntTime95
    df['Quantile99']= quntTime99
    df['Quantile99-2Mins'] =df['Quantile99'] - pd.Timedelta('00:02:00')
    df['Quantile99-2Mins']=pd.to_datetime(df['Quantile99-2Mins']).dt.strftime("%H:%M:%S")
    #interchange the last 2 columns as per the final requirements
    Write_excel(df,'TickerPredictMinMax3_JunAug_18_step1')
    df.reset_index(inplace=True)
    dfnew3 = pd.merge(ClientFundTicker,df,how='inner',on='Ticker')
    del dfnew3['index']
    Write_excel(dfnew3,'TickerPredict_NoOutlierNDS_JunAug_18_step1')
    dfnew3.rename(columns={'Median_Time':'Predicted_Time','Quantile01':'Quantile01(Ticker_MinimumTime)','Quantile99':'Quantile99(Max_PredictedTime)','Quantile99-2Mins':'Alert Time'},inplace=True)
    dfnew3.columns
    Write_excel(dfnew3,'TickerPredict_DashboardNDS_JunAug_18_new_step1')