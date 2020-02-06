import pandas as pd 
#import numpy as np 
#from sqlalchemy.types import Date,VARCHAR,INTEGER
from int_dir.config_file import *
from helper_modules.ai_db_connect import conn_eng
from datetime import datetime as dt


def table_creator(DB_WRITE, engine, schemas, all_tables, REFERENCE_DATE,SCORING_DATE_START, SCORING_DATE_END,TABLE_CREATE):
    
    """
    Calculates target variable (what was the price for a bid) for data before a Reference date.
        - Reads raw data PostGres
        - Filters for relevant data
        - writes primary key with price to database 

    Parameters
    ----------
    TARGET_DB_WRITE : Boolean, Global variable
        Flag to set if we are going to re-write the target DB
    engine : SQL Alchemy Engine
        The engine to use to perform SQL transactions 
    schemas : dictionary, Global variable
        Dictionary of schemas for input, intermediate and output
    all_tables : dictionary, Global variable
        Dictionary of table names for all tables 
     REFERENCE_DATE: string (date), Global Variable
         The last date to consider for all invoice transactions    

    Returns
    -------
    final_target_data : DataFrame
        DataFrame with key, bid IDs, destination IDs, fiscal year and price (target)
    If needed writes to SQL database and creates target dataframe depending on the switch
    
    """
    print('Creating '+TABLE_CREATE+' table')
    
    #Formulate the query
    query = "SELECT distinct fiscal,destination_no,bid_line_no,award_price_ty,bid_due_date as date FROM "+schemas['intermediate_schema']+"."+all_tables['all_bid_table_intrmdt']+" WHERE award_price_ty > 0"

    # Read the SQL query
    raw_data = pd.read_sql(query,engine)
    
    raw_data['key'] = raw_data['fiscal'].astype(str) +'_'+raw_data['destination_no'].astype(str) +'_'+raw_data['bid_line_no'].astype(str)
    
    raw_data = raw_data.rename(columns={'award_price_ty':'target'})
    raw_data = raw_data.groupby(['fiscal','destination_no','bid_line_no','key']).first().reset_index()
    
    raw_data = raw_data[['fiscal','destination_no','bid_line_no','date','key','target']]

#     # Filter all key combinations that have 1 award price
#     cnt_rows = raw_data.groupby('key').agg({'target',pd.Series.nunique}).rename(columns = {'target
    
#     raw_data = pd.merge(raw_data, cnt_rows, on = 'key')
#     raw_data = raw_data.loc[('
    
    
    #Filter to the dates before the reference date
    if TABLE_CREATE == 'TARGET':
        output_data = raw_data[(pd.to_datetime(raw_data['date']).dt.year <= REFERENCE_DATE)]
    else:
        output_data = raw_data[(pd.to_datetime(raw_data['date']).dt.year == SCORING_DATE_END)]
        

    if TABLE_CREATE == 'TARGET':        
        if DB_WRITE:
            output_data.to_sql(all_tables['target'],schema= schemas['intermediate_schema'],
                                            con=engine, index=False,if_exists ='replace')
            print("Target table written to database successfully")
            
            return
        else:
            return output_data.reset_index(drop = True)

    else:        
        if DB_WRITE:
            output_data.to_sql(all_tables['scoring'],schema= schemas['scoring_schema'],
                                            con=engine, index=False,if_exists ='replace')
            print("Scoring table written to database successfully")

            return
        else:
            return output_data.reset_index(drop = True)

