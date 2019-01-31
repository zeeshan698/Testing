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
    Fund_col=colnm[~colnm.isin(['Quantile50(Predicted_Time)','Client Name', 'FUND_ID','Totaldays', 'NoofMissdays'])]
    TempVolume=Fund_Medcount[['Time','Bin_Volume_Predict']]  
    for cl in Fund_col:   
        #print(cl)
        try:
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
    file_FundNDSless='Fund_PredictNDS_JunAug_18_weekless.xlsx'
    dfAnly=pd.read_excel(file_FundNDSless,index_col=None,datetime_format='hh:mm:ss')
    colnm=dfAnly.columns
    del dfAnly['index']
    df=dfAnly
    last_row=df.shape[0] 
    '''Adding extra time for smooth Time series data,resample will start  from 16:00:00 to 19:00:00'''
    df.loc[last_row+1]=['000','000','16:00:00','16:00:00', 0,'16:00:00','16:00:00',0]
    df.loc[last_row+2]=['000','000','19:00:00', '19:00:00',0,'19:00:00','19:00:00',0]
    #df.rename(columns={'Quantile50(Predicted_Time)':'Predicted_Time'},inplace=True)
    df['Predicted_Time'] = df['Predicted_Time'].apply(lambda x:convert_to_time(x) if not isinstance(x, type(datetime.time(16,5,3))) else x)
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
    Write_excel(All_Vol_Fund,"Fund_volume_NDS_JunAug18_weekless")
    #####
   
 
