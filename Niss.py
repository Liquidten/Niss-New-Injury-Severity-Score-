import pandas as pd
import dask.dataframe as dd
import numpy as np
import time
from dask.distributed import Client, LocalCluster
from datetime import datetime, timedelta
import itertools

def Readprocessedfiles(path):
    df = pd.read_csv(path, sep=',', dtype='object', engine='c')
    #df = dd.read_csv(path, sep=',', dtype='object', engine='c', na_filter=False)  ## Dask
    return df
    
def NISS_score(df, int):

    cols2 = ["sev_{}".format(i) for i in range(1, int)]

    a = []
    df[cols2] = df[cols2].astype(float)
    for i in range(1, 38):
        a.append(int(df['sev_{}'.format(i)]))
    b = sorted(a, reverse=True)[:3]
    results = sum([i ** 2 for i in b])
    if any(i == 9 for i in b):
        return(99)
    elif any(i == 6 for i in b):
        return(75)
    else:
        return results
        
def MergeData(df1, df2):

    Mergedf = dd.merge(left=df1, right=df2, how='inner', on=['CLM_ID_AMBL'])

    return Mergedf
    
def mainfunction(df, columns, sev, int):
    
    df = df.drop(['X'], axis=1)
    df1 = df[columns]
    df1 = df1.astype(str)
    df1 = df1.replace("nan", '0', regex=True)
    df1["niss"] = df1.apply(lambda df1: NISS_score(df1, int), axis=1)
    df1 = df1.drop(sev, axis=1)
    Mergeddf = MergeData(df1, df)
    return Mergeddf
    
  
    
if __name__ == "__main__":

   IP = '/gpfs/data/sanghavi-lab/Sameep/2012_Data/Combined2/IP_Severity_Score.csv'
   OP = '/gpfs/data/sanghavi-lab/Sameep/2012_Data/Combined2/OP_Severity_Score.csv'
   
   """ Read files """
   IP_data = Readprocessedfiles(IP)
   OP_data = Readprocessedfiles(OP)
   
   """ Assigning columns list for number of columns required for IP and OP """
   columns_ip = ['CLM_ID_AMBL'] + ['sev_{}'.format(i) for i in range(1, 38)]
   columns_op = ['CLM_ID_AMBL'] + ['sev_{}'.format(i) for i in range(1, 40)]
   
   """ Droping comlumns bases on number of colums """
   sev_ip = ['sev_1', 'sev_2', 'sev_3', 'sev_4', 'sev_5', 'sev_6', 'sev_7', 'sev_8', 'sev_9', 'sev_10', 'sev_11', 'sev_12', 'sev_13', 'sev_14', 'sev_15', 'sev_16', 'sev_17', 'sev_18', 'sev_19', 'sev_20', 'sev_21', 'sev_22', 'sev_23', 'sev_24', 'sev_25', 'sev_26', 'sev_27', 'sev_28', 'sev_29', 'sev_30', 'sev_31', 'sev_32', 'sev_33', 'sev_34', 'sev_35', 'sev_36', 'sev_37']
   sev_op = ['sev_1', 'sev_2', 'sev_3', 'sev_4', 'sev_5', 'sev_6', 'sev_7', 'sev_8', 'sev_9', 'sev_10', 'sev_11', 'sev_12',
                                'sev_13', 'sev_14', 'sev_15', 'sev_16', 'sev_17', 'sev_18', 'sev_19', 'sev_20', 'sev_21', 'sev_22', 'sev_23',
                                'sev_24', 'sev_25', 'sev_26', 'sev_27', 'sev_28', 'sev_29', 'sev_30', 'sev_31', 'sev_32', 'sev_33', 'sev_34', 'sev_35', 'sev_36', 'sev_37', 'sev_38', 'sev_39']
   
   int_ip = 38  ## number of colums in data ip
   int_op = 40  ## number of columns in data op
   
   Merged_df_IP = mainfunction(IP_data, columns_ip, sev_ip, int_ip)
   Merged_df_OP = mainfunction(OP_data, columns_op, sev_op, int_op)
   
   