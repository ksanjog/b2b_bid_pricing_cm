#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 13:26:23 2020

@author: Kumar_Sanjog
"""

# For multi-proc
#import multiprocessing
from helper_modules.ai_db_connect import conn_eng
from pathos.multiprocessing import ProcessingPool as Pool
import pandas as pd 
#import numpy as np
import inspect
from int_dir.config_file import *

def runQuery(query):
    engine = conn_eng()
    #connect_text = "dbname='%s' user='%s' host=%s port=%s password='%s'" % (dbname, user, host, port, password)
    #con = psycopg2.connect(connect_text)
    #cur = con.cursor()
    engine.execute(query)
    del engine
    return

def get_key(ds, schemas,selected_schema):
    engine = conn_eng()
    pk_query = "SELECT * FROM "+schemas[selected_schema]+"."+ds
    pk_df = pd.read_sql(pk_query,engine)
    del engine
    return pk_df

#Final Feature creation
def file_write(final_df,out_path,out_file):
    
    print('Writing file now!!')
    
    #final_df = final_df.drop('key',axis = 'columns')
    final_df.to_csv(out_path + out_file + '.csv', index = False)
    
    print('Everything done and file written!!')
    
    return

def fc_protocol(temp,join_key,key_df):
    fc_df = pd.merge(key_df,temp, on = join_key, how = 'left')    
    fc_df = fc_df.sort_values(by = ['key'])
    assert (fc_df.shape[0] == key_df.shape[0]),"Final features data has more rows than the target indicating the presence of duplicates"
    
    return fc_df

def trig_func(args):
    (function,(WRITE_D,key_df, schemas, all_tables, fc_protocol, selected_schema)) = args
    engine = conn_eng()
    temp = function(WRITE_DB,engine, key_df, schemas, all_tables, fc_protocol,selected_schema)
    if 'key' in temp.columns:
        temp = temp.drop(columns=['key'], axis = 1)
    if 'target' in temp.columns:
        temp = temp.drop(columns=['target'], axis = 1)
        
    del engine
    return temp

def create_features(WRITE_DB, FP, all_tables, schemas, CPUS, selected_schema, selected_table):
 
    #define key dataframe
    
    key_df = get_key(all_tables[selected_table], schemas,selected_schema)
    key_df = key_df.sort_values(by = ['key','date'])
    
    if selected_schema == 'scoring_schema':
        key_df = key_df[['key']]
    else:
        key_df = key_df[['key','date','target']]
    
    pool = Pool(CPUS)
    
    #Features output framework
    all_functions = inspect.getmembers(FP, inspect.isfunction)
    all_functions = [x[1] for x in all_functions]
#     print(all_functions)
    
    args = (WRITE_DB,key_df, schemas, all_tables, fc_protocol,selected_schema)
    all_functions = [(x,args) for x in all_functions]
    
    
    if WRITE_DB:        
       
        temp = pool.map(trig_func, all_functions)
        df = pd.concat(temp, axis = 1)
        df = pd.concat([key_df,df], axis = 1)
        
        engine = conn_eng()
        if selected_schema == 'scoring_schema':
            df.to_sql(all_tables['scoring_table'],schema= schemas['output_schema'],
                      con=engine, index=False,if_exists ='replace')
           
        else:   
            df.to_sql(all_tables['features_table'],schema= schemas['output_schema'],
                      con=engine, index=False,if_exists ='replace')
            
        engine.dispose()
        del engine
        #print_summary(MISSING_VALUE_TREATMENT,df)       
    else:              
        temp = pool.map(trig_func, all_functions)
        df = pd.concat(temp, axis = 1)
        df = pd.concat([key_df,df], axis = 1)
#         print_summary(MISSING_VALUE_TREATMENT,df)
        return df
    return 'Files written to DB'


