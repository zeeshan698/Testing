# -*- coding: utf-8 -*-
"""
Created on Thu Jun 21 03:48:59 2018

@author: p638855
"""
from  functLib2 import *
if __name__ == "__main__":  
    os.chdir("H:/Downloads/NDSData/Step_1/")
    #for MTMI
    path1='H:\\Downloads\\NDSData\\resultsbyAnusha22Oct18\\'
    os.chdir(path1)
    #MTMI part
    file_FundMTMIless='UniqueFundLess30MTMI_JunAug_18_step1_MasterFormat.xlsx'
    sheet1='less_5 Pred Time Analysis'
    dfAnly=read_exl_file(path1+file_FundMTMIless,sheet1)
    dfnew3=dfAnly 
 
 
    #MTMI part over
    
    
    ###pricing part
    
    dfPricing_Fnd='Pricing_Fund_analysis_JunAug_stats_less5.xlsx'
    df_PricingFnd=pd.ExcelFile(dfPricing_Fnd,index_col=None,datetime_format='hh:mm:ss')
    df_PricingFnd=pd.read_excel(dfPricing_Fnd,index_col=None)
    dfnew3= df_PricingFnd
    #######
  #common part
    #dfnew3=dfNDS_Fnd
    #Roll up from Ticker to Fund with 
    #Fund which has  max of PredictedTime 
    #for dfnew3 in [dfMTMI_Fnd,dfPricing_Fnd]:
    dfnew3.rename(columns={'Quantile50(Predicted_Time)':'Predicted_Time'},inplace=True)
    df_Fundnew=dfnew3.groupby('FUND_ID').apply(lambda x: x[x['Predicted_Time']==x['Predicted_Time'].max()]).drop_duplicates('Predicted_Time')
    df_Fund2 =  df_Fundnew.reset_index(level=0, drop=True).reset_index()
    df_Fund2.drop( axis=1, columns=['index','FUND_ID','Totaldays', 'NoofMissdays'],inplace=True)
    df_Fund2.drop_duplicates(inplace=True)
    df_Fund2.reset_index(inplace=True)
    del df_Fund2['index']
    filenm='Client_Fund_'+str(dfnew3)+'_JunAug18_minT'
    Write_excel(df_Fund2,'Client_Fund_Pricing_JunAug18_weekless')
    Write_excel(df_Fund2,'Client_Fund_MTMI_JunAug18_weekless')
     