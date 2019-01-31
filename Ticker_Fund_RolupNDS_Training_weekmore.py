# -*- coding: utf-8 -*-
"""
Created on Thu Jun 21 03:48:59 2018

@author: p638855
"""
from  functLib import *
if __name__ == "__main__":  
    os.chdir("H:/Downloads/NDSData/NDSNewData/")
    os.chdir(path1)
    path1='H:\\Downloads\\NDSData\\resultsbyAnusha22Oct18\\'
   # path2='H:\Downloads\\NDSData\\Step_1\\'
    #file_main='NDS_ticker_analysis_JunAug_step1_v03.xlsx'
    file_main1='NDS_ticker_analysis_JunAug_step1_v04.xlsx'
    #TickerPredict_DashboardNDS_JunAug_18_new
    #df=pd.ExcelFile(path2+ file_main,index_col=None)#,datetime_format='hh:mm:ss')
    df2=pd.ExcelFile(path1+ file_main1,index_col=None)
    #df=pd.ExcelFile('UniqueTicker30NDS.xlsx',index_col=None,datetime_format='hh:mm:ss')
    dfnew3=pd.read_excel(df2,index_col=None)
    dfnew3.columns
    colnm= [ 'Quantile01(Ticker_MinimumTime)', 'Quantile05', 'Quantile10', 'Quantile15', 'Quantile60', 'Quantile70', 'Quantile80',
       'Quantile95', 'Quantile99(Max_PredictedTime)', 'Alert Time',
       'MaxTime']
    for cl in colnm:
        #df[cl]=pd.to_timedelta(df[cl])
        print(cl)
        dfAnly[cl].apply(lambda x:convert_to_time(x) if (not pd.isnull(x)) & (not isinstance(x, type(datetime.time(16,5,3)))) else x)
    dfnew3['Predicted_Time'] = dfnew3['Predicted_Time'].apply(lambda x:convert_to_time(x) if not isinstance(x, type(datetime.time(16,5,3))) else x)
    #16204
    #6564 after considering first in drop duplicates
    # Ticker to Fund with Ticker with max Predicted Time 
    #dfnew3['Predicted_Time']=pd.to_datetime(dfnew3['Predicted_Time']).dt.time#apply(pd.Timestamp)
    #dfnew3['Predicted_Time']=pd.DatetimeIndex(dfnew3['Predicted_Time']).time
    df_Fundnew=dfnew3.groupby('FUND_ID').apply(lambda x: x[x['Predicted_Time'] == x['Predicted_Time'].max()])
    #.drop_duplicates('Predicted_Time')
    df_Fund2 =  df_Fundnew.reset_index(level=0, drop=True).reset_index()
    
    del df_Fund2['index']
    ##del df_Fund2['level_1']
    del df_Fund2['Ticker']
    df_Fund2.drop_duplicates(inplace=True)
    df_Fund2.rename(columns={'Quantile99(Max_PredictedTime)':'Fund_Max_PredictedTime'},inplace=True)
    df_Fund2.rename(columns={'Quantile01(Ticker_MinimumTime)':'Quantile01(Fund_MinimumTime)'},inplace=True)
    df_Fund2.reset_index(inplace=True)
    Write_excel(df_Fund2,"Fund_PredictNDS_JunAug_18_weekmore")
    df_client=df_Fund2.groupby('Client Name').apply(lambda x: x[x['Predicted_Time']==x['Predicted_Time'].max()])
    df_client2 =  df_client.reset_index(level=0, drop=True).reset_index()
    del df_client2['index']
    ##del df_client2['level_1']
    del df_client2['FUND_ID']
    df_client2.rename(columns={'Fund_MinimumTime':'Client_MinimumTime','Fund_AlertTime':'Client_AlertTime'},inplace=True)
    #df_client3=df_clien2t[['Client Name','Client_MinimumTime','Predicted_Time','Client_AlertTime','Max_PredictedTime']]
    Write_excel(df_client2,'ClientPredictNDS_JunAug_18_weekmore')
    
    #####=====Roll up over
        ###training validation 
    df_Outlier=Ticker_outlier
    df_Outlier2=df_Outlier[['DateTime','FUND_ID','Ticker']]
    df_Outlier2['Outlier']='Outlier'
    ####Actual merging with Outlier
    os.chdir("H:/Downloads/NDSData/NDSNewData")
    df=pd.ExcelFile('UniqueTicker30NDS_JanMay_18.xlsx',index_col=None,datetime_format='hh:mm:ss')
    dfA=pd.read_excel(df,index_col=None)
     
    df_Actual=dfA[['DateTime','FUND_ID', 'Date','Ticker','Time2']]
    df_Actual['Time']=pd.DatetimeIndex(df_Actual['DateTime']).time
    df_Actual['Time_Num']=  df_Actual['Time'].map(lambda x: convertToExcelTime(x))
   
    df_ActualOut= pd.merge(df_Actual,df_Outlier2,on=['Ticker','FUND_ID','DateTime'],how='left')
    ######Predict Time reading
    df_PredictTime = dfnew3
    #df_PredictTime.rename(columns={'Median_Time':'Predicted_Time', 'Quantile80':'Alert_Time ','Quantile99':'Max_PredictTime'},inplace=True)
    df_PredictTime['PTime_Num']=pd.to_datetime(df_PredictTime['Predicted_Time']).map(lambda x: convertToExcelTime(x))
    df_PredictTime
    ####Preparing ClientFund
    cols2=['Client Name' ,'FUND_ID','Ticker']
    ClientFund=df_PredictTime[cols2]
    ClientFund.drop_duplicates(inplace=True)
    ClientFund.shape 
    #######Fund Actual , Predict, outlier merge
    Fund_TimeCompare=pd.merge(df_ActualOut,df_PredictTime,how='left', on=['Ticker','FUND_ID' ])
    Fund_TimeCompare.Outlier.fillna('NotOutlier',inplace=True)
    ####Time difff wrt Numerical time
    #Fund_TimeCompare['Time_Num'] = Fund_TimeCompare['Time'].map(lambda x: convertToExcelTime(x))
    Fund_TimeCompare['TimeDiff_Pred-Act']= Fund_TimeCompare['PTime_Num']-Fund_TimeCompare['Time_Num'] 
    
    Fund_TimeCompare['TimeDiff_Pred-Act'].abs().mean() 
    ##0.007393027837962033 with May data
    ###0.0071138266593152675 with new training data 
    Fund_TimeCompare[Fund_TimeCompare.Outlier=='NotOutlier']['TimeDiff_Pred-Act'].abs().mean()
    '''0.006932415539354868 Jan_May'''
    '''0.006622419565246415 with no outlier new data Jan_April18 9.32 mins'''
    '''0.0051068982229119314 with no outlier'''
    '''0.005542266364384706after considering drop duplicate first'''
    '''0.0060779191906313096 Mean Abs Res Error with no outlier 8.45 mins''' 
    '''0.00686476519005209 overall on train data 9.53 mins'''
    Fund_TimeCompare.drop_duplicates(inplace=True)
    #Fund_TimeCompare.to_csv("Fund_TimeCompareNDSdata.csv")
    Write_excel(Fund_TimeCompare,'Fund_TimeReport_NDSdata_JanMay_18')
    '''Analysis on time difference'''
    TickerTimeDiffMean =pd.DataFrame({'Tickert_AbsMeanError' :Fund_TimeCompare.groupby('Ticker')['TimeDiff_Pred-Act'].apply(lambda x : x.abs().mean())}) 
    TickerTimeDiffMean.reset_index(inplace=True)
    TickerTimeDiffMean.head()
    ##FundTimeDiffMean.to_csv("Fund_MAE.csv")
    Write_excel(TickerTimeDiffMean,'Ticker_MAE_NDSData')
    TickerTimeDiff_MAE=pd.merge(Fund_TimeCompare,TickerTimeDiffMean, how='inner',on=['Ticker' ])
    Write_excel(TickerTimeDiff_MAE,"TickerTimeDiff_MAE_NDS_JanMay_18")
    Ticker_NDS_Report=TickerTimeDiff_MAE[['Ticker','FUND_ID','Client Name', 'Ticker_MinimumTime', 'Predicted_Time','Alert Time', 'Max_PredictedTime', 'Tickert_AbsMeanError']]
    Ticker_NDS_Report2=Ticker_NDS_Report.drop_duplicates()
    Write_excel(Ticker_NDS_Report2,"Ticker_NDS_Report_JanMay_18")
    FundTimeDiffMean=pd.DataFrame({'Fund_AbsMeanError' : Fund_TimeCompare.groupby('FUND_ID')['TimeDiff_Pred-Act'].apply(lambda x : x.abs().mean())})
    FundTimeDiffMean.reset_index(inplace=True)
    FundTimeDiffMean.head()
    ##FundTimeDiffMean.to_csv("Fund_MAE.csv")
    Write_excel(FundTimeDiffMean,'Fund_MAE_NDSData_JanMay_18')
    #####
    clientTimeDiffMean=pd.DataFrame({'Client_AbsMeanError' : Fund_TimeCompare.groupby('Client Name')['TimeDiff_Pred-Act'].apply(lambda x : x.abs().mean())})
    clientTimeDiffMean.reset_index(inplace=True)
    clientTimeDiffMean.head()
    Write_excel(clientTimeDiffMean,'Client_MAE_NDSData_JanMay_18')
    ####
     
    '''New Analysis'''
    TickerTimeDiffAnaly=Fund_TimeCompare.groupby('Ticker')['TimeDiff_Pred-Act'].agg([('negMean',lambda x: x[x<0].mean()),('negCount',lambda x: x[x<0].count().astype(int)),('positMean',lambda x:x[x>=0].mean()),('positCount',lambda x: x[x>=0].count().astype(int)),('MAResidualError',lambda x : x.abs().mean())])   
    TickerTimeDiffAnaly.reset_index(inplace=True)
    Write_excel(TickerTimeDiffAnaly,"TickerTimeDiffPNMeansCountNDSdata") 
    FundTimeDiffAnaly=Fund_TimeCompare.groupby('FUND_ID')['TimeDiff_Pred-Act'].agg([('negMean',lambda x: x[x<0].mean()),('negCount',lambda x: x[x<0].count().astype(int)),('positMean',lambda x:x[x>=0].mean()),('positCount',lambda x: x[x>=0].count().astype(int)),('MAResidualError',lambda x : x.abs().mean())])   
    FundTimeDiffAnaly.reset_index(inplace=True) 
    ClientFundDiffAnaly=pd.merge(FundTimeDiffAnaly,ClientFund,on='FUND_ID',how='left')
    ##ClientFundDiffAnaly.columns=ClientFundDiffAnaly.columns[-1,0:5]
    ClientFundDiffAnaly=ClientFundDiffAnaly[['Client Name','FUND_ID', 'negMean', 'negCount', 'positMean', 'positCount','MAResidualError']] 
    Write_excel(ClientFundDiffAnaly,"ClienFundTimeDiffPNMeansCountNDSdata") 
    #######analysis over  