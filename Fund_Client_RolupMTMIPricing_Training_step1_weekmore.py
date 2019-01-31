# -*- coding: utf-8 -*-
"""
Created on Thu Jun 21 03:48:59 2018

@author: p638855
"""
from  functLib2 import *
if __name__ == "__main__":  
    os.chdir("H:/Downloads/NDSData/Step_1/")
    path='H://Downloads//NDSData//resultsbyAnusha22Oct18//'
    dfPricing_Fnd='Pricing_Fund_analysis_JunAug_stats_greater5_1.xlsx'
    dfMTMI_Fnd='MTMI_Fund_analysis_JunAug_stats_greater5_1.xlsx'
    #dfMTMI_Fnd=pd.ExcelFile('MTMI_Fund_analysis_JunAug.xlsx',index_col=None,datetime_format='hh:mm:ss')
    #dfPricing_Fnd=pd.ExcelFile('Pricing_Fund_analysis_JunAug_step1.xlsx',index_col=None,datetime_format='hh:mm:ss')
    dfMTMI_Fnd=pd.read_excel(path+dfMTMI_Fnd,index_col=None,datetime_format='hh:mm:ss')
    dfPricing_Fnd=pd.read_excel(path+dfPricing_Fnd,index_col=None,datetime_format='hh:mm:ss')
    
    dfnew3=dfPricing_Fnd
    dfnew3=dfMTMI_Fnd
    #dfnew3=dfNDS_Fnd
    #Roll up from Ticker to Fund with 
    #Fund which has  max of PredictedTime 
    for dfnew3 in [dfMTMI_Fnd,dfPricing_Fnd]:
        dfnew3.rename(columns={'Quantile50(Predicted_Time)':'Predicted_Time'},inplace=True)
        df_Fundnew=dfnew3.groupby('FUND_ID').apply(lambda x: x[x['Predicted_Time']==x['Predicted_Time'].max()]).drop_duplicates('Predicted_Time')
        df_Fund2 =  df_Fundnew.reset_index(level=0, drop=True).reset_index()
        #del df_Fund2['index']
        #del df_Fund2['FUND_ID']
        df_Fund2.drop( axis=1, columns=['index','FUND_ID','Totaldays', 'NoofMissdays', 'MaxTime(before 18:05)','MaxTime'],inplace=True)
        df_Fund2.drop_duplicates(inplace=True)
        df_Fund2.rename(columns={'Quantile99(Max_PredictedTime)':'Client_Max_PredictedTime'},inplace=True)
        df_Fund2.rename(columns={'Quantile01(Fund_MinimumTime)':'Quantile01(Client_MinimumTime)'},inplace=True)
        df_Fund2.reset_index(inplace=True)
        del df_Fund2['index']
        filenm='Client_Fund_'+str(dfnew3)+'_JunAug18_minT'
        Write_excel(df_Fund2,'Client_Fund_Pricing_JunAug18_weekmore')
        Write_excel(df_Fund2,'Client_Fund_MTMI_JunAug18_weekmore')
        #Write_excel(df_Fund2,filenm)
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