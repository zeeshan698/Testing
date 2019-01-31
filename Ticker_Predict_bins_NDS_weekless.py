# -*- coding: utf-8 -*-
"""
Created on Wed Jan  3 06:55:40 2018
Converting Predicted time to bins count
@author: p638855
"""
from  functLib2 import *
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
    
if __name__ == "__main__":
    path1='H:\\Downloads\\NDSData\\resultsbyAnusha22Oct18\\'
    os.chdir(path1)
    file_Tck_less='NDS_lessthan_week_tickers_22oct18.xlsx'
    #reading sheets and combine in one df
    sheet1=['new_Q1<SLA','new_Q50<SLA','Always Miss']
    df_Anly2=read_exl_file(path1+file_Tck_less,sheet1[0])
    col9=['Client Name', 'FUND_ID', 'Ticker', 'Quantile01(Ticker_MinimumTime)',
       'Predicted_Time', 'Totaldays', 'NoofMissdays', 'MaxTime(before 18:05)',
       'MaxTime']
    col8=['Client Name', 'FUND_ID', 'Ticker', 'Quantile01(Ticker_MinimumTime)',
       'Predicted_Time', 'Totaldays', 'NoofMissdays',  
       'MaxTime']
    df_Anly2=df_Anly2[col9]
    df_Anly3=read_exl_file(path1+file_Tck_less,sheet1[1])
    df_Anly3=df_Anly3[col9]
    df_Anly4=read_exl_file(path1+file_Tck_less,sheet1[2])
    df_Anly4=df_Anly4[col8] 
    dfAnly4=df_Anly4.dropna(axis=0)
    df_Anly=pd.concat([df_Anly2,df_Anly3],ignore_index=True)
    df_Anly.dropna(axis=0,inplace=True)
    df_AnlyM=pd.concat([df_Anly,dfAnly4], ignore_index=True)
    Write_excel(df_AnlyM,"Ticker_combine_NDS_JunAug18_weekless")
    ###3 sheets in one df done
    colnm=df_AnlyM.columns
    df=df_AnlyM
    last_row=df_AnlyM.shape[0] 
    '''Adding extra time for smooth Time series data,resample will start  from 16:00:00 to 19:00:00'''
    df.loc[last_row+1]=['000','000','16:00:00', '16:00:00',0,'16:00:00','16:00:00','000',0]
    df.loc[last_row+2]=['000','000','19:00:00', '19:00:00',0,'19:00:00','19:00:00','000',0]
    #df.rename(columns={'Quantile50(Predicted_Time)':'Predicted_Time'},inplace=True)
    #df['Predicted_Time'] = df['Predicted_Time'].apply(lambda x:convert_to_time(x) if not isinstance(x, type(datetime.time(16,5,3))) else x)
    #df['Predicted_Time'] =pd.to_datetime(df['Predicted_Time']).dt.strftime("%H:%M:%S")
    df['Predicted_Time']= pd.to_timedelta(df['Predicted_Time'].astype(str))
    df.set_index(['Predicted_Time'])
    Fund_Medcount= df[['Predicted_Time']].resample('5T',on= 'Predicted_Time').count()
    Fund_Medcount.head(10)
    Fund_Medcount.tail(10)
    '''removing extra count added'''
    Fund_Medcount['Predicted_Time'].iloc[0]=0
    Fund_Medcount['Predicted_Time'].iloc[-1]=0                              
    Fund_Medcount.rename(columns={'Predicted_Time':'PredictedT_Count'},inplace=True)
    Fund_Medcount.reset_index(inplace=True)
    Fund_Medcount['Bin_Volume_Predict']=Fund_Medcount['PredictedT_Count'].cumsum()
    Fund_Medcount.rename(columns={'Predicted_Time':'Time'},inplace=True)
    ###
    All_Vol_Fund=VolumeCount(df,Fund_Medcount)
    Write_excel(All_Vol_Fund,"Ticker_volume_NDS_JunAug18_weekless")
    #####
   
 
