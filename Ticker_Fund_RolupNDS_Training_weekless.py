# -*- coding: utf-8 -*-
"""
Created on Thu Jun 21 03:48:59 2018

@author: p638855
"""
from  functLib import *
if __name__ == "__main__":  
    path1='H:\\Downloads\\NDSData\\resultsbyAnusha22Oct18\\'
    os.chdir(path1)
    file_main1='Ticker_combine_NDS_JunAug18_weekless.xlsx'
    df2=pd.ExcelFile(path1+file_main1,index_col=None,datetime_format='hh:mm:ss')
    dfnew3=pd.read_excel(df2,index_col=None)
    dfnew3.columns
    colnm= [  'MaxTime','MaxTime(before 18:05)',
        'Predicted_Time', 'Quantile01(Ticker_MinimumTime)']
    for cl in colnm:
        try:
            #df[cl] =pd.to_datetime(df[cl]).dt.strftime("%H:%M:%S")
            dfAnly[cl].apply(lambda x:convert_to_time(x) if (not pd.isnull(x)) & (not isinstance(x, type(datetime.time(16,5,3)))) else x)
        except:
            print(cl)
    dfnew3['Predicted_Time'] = dfnew3['Predicted_Time'].apply(lambda x:convert_to_time(x) if not isinstance(x, type(datetime.time(16,5,3))) else x)
    df_Fundnew=dfnew3.groupby('FUND_ID').apply(lambda x: x[x['Predicted_Time'] == x['Predicted_Time'].max()])
    df_Fund2 =  df_Fundnew.reset_index(level=0, drop=True).reset_index()
    del df_Fund2['index']
    del df_Fund2['Ticker']
    df_Fund2.drop_duplicates(inplace=True)
    df_Fund2.rename(columns={'MaxTime':'Fund_Max_PredictedTime'},inplace=True)
    df_Fund2.rename(columns={'Quantile01(Ticker_MinimumTime)':'Quantile01(Fund_MinimumTime)'},inplace=True)
    df_Fund2.reset_index(inplace=True)
    Write_excel(df_Fund2,"Fund_PredictNDS_JunAug_18_weekless")
    df_client=df_Fund2.groupby('Client Name').apply(lambda x: x[x['Predicted_Time']==x['Predicted_Time'].max()])
    df_client2 =  df_client.reset_index(level=0, drop=True).reset_index()
    del df_client2['index']
    del df_client2['level_0']
    del df_client2['FUND_ID']
    df_client2.rename(columns={'Quantile01(Fund_MinimumTime)':'Client_MinimumTime','Fund_Max_PredictedTime':'Client_Max_PredictedTime'},inplace=True)
    #df_client3=df_clien2t[['Client Name','Client_MinimumTime','Predicted_Time','Client_AlertTime','Max_PredictedTime']]
    Write_excel(df_client2,'ClientPredictNDS_JunAug_18_weekless')
    
    #####=====Roll up over
     
    