from int_dir.config_file import *

def create_all_bid_table(schs,tbls,engine,SELECTED_SCHEMA):
    
    """
    This function has the query to create the intermediate table of all bids table
    
    """
    raw_schema = schs['input_schema']
    selected_schema = schs[SELECTED_SCHEMA]
    
    main_table = tbls['all_bid_table']
    feature_table = tbls['all_bid_table_intrmdt']
    
    query_data = "SELECT * FROM "+raw_schema+"."+main_table+" where bid_type_name in ('Municipal','County','State','Institutional','Govt Tendered')"
    
    unserved_zip = "SELECT * FROM "+raw_schema+".unserved_zip_codes_2020_01_15"

    # Read the SQL query
    raw_data = pd.read_sql(query_data,engine)
    unserved_zip = pd.read_sql(unserved_zip,engine)
    
    raw_data['key'] = raw_data['fiscal'].astype(str) +'_'+raw_data['destination_no'].astype(str)+'_'+raw_data['bid_line_no'].astype(str)
    
    
    # Removing unserved zip codes from data
    unserved_zip['flag'] = 1
    raw_data = pd.merge(raw_data,unserved_zip,'left',on='destination_zip_postal')
    raw_data = raw_data.loc[raw_data['flag'] != 1].drop(['flag'],axis=1).reset_index(drop=True)
    
    # Removing outlier from data - award_price_ty == 5727    
    raw_data = raw_data.loc[raw_data['award_price_ty'] < 5000]
    
    raw_data.to_sql(feature_table,schema= selected_schema,con=engine, index=False,if_exists ='replace')
    
    print(selected_schema+'.'+feature_table+' Successfully Written')
    
    return