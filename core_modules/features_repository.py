import pandas as pd
import numpy as np
from int_dir.config_file import *
import datetime

##################################################################################################################################################################################################################################################################################

# Feature1: Product Type 
# Level:Key
# Datasource: Bid data

def product_bid_customer_region_type(WRITE_DB,engine, key_df,schemas,all_tables,fc_protocol,selected_schema):  

    """
    Find product type associated with the bid number
    
    Parameters
    ----------
    WRITE_DB : Boolean, Global variable
        Flag to set if we are going to write features to master DB
    MISSING_VALUE_TREATMENT : Boolean, Global variable
        Flag to set if we are going to perform missing value treatment on the features
    OUTLIER_TREATMENT : Boolean, Global variable
        Flag to set if we are going to treat for outliers in the data
    engine : SQL Alchemy Engine
        The engine to use to perform SQL transactions
    key_df : DataFrame
        Dataframe with key and target 
    schemas : dictionary, Global variable
        Dictionary of schemas for input, intermediate and output
    all_tables : dictionary, Global variable
        Dictionary of table names for all tables  
    fc_protocol : Function
        Function to merge key_df (refer above) and df (key, feature)    
    selected_schema : selection parameter, string
        Parameter to select schema amongst input, intermediate and output       
    Returns
    -------
    df : DataFrame
        DataFrame with key,sales_last_quarter
    
    """
    
    #Define the join key
    join_key="key"
    
    #Formulate the query to read the required columns from the Bid intermediates table

    query = "SELECT distinct key, sales_region_name,product_name,bid_type_name,customer_type_name FROM \
     "+schemas[selected_schema]+"."+all_tables['all_bid_table_intrmdt']+" WHERE award_price_ty > 0"
    
    product_type_df=pd.read_sql(query,engine)
    
    product_type_df = product_type_df.drop_duplicates().groupby('key').first().reset_index()
    
    #Join with key df
    df = fc_protocol(product_type_df,join_key,key_df)
    
    print("Feature dataframe for sales region, product type, customer type and bid type created")

    return df



# Feature2: Number of competitors (excluding CMP)
# Level:Key (fiscal, bid_no, destination_no)
# Datasource: raw_data
# Filter: Competitors with bid more than 0

def num_competitors(WRITE_DB,engine, key_df,schemas,all_tables,fc_protocol,selected_schema):  

    """
    Calculates amount of sales for each key in the past 3 months
        - Find the number of days between the last_trx_date and the billing_date for each invoice
        - Filter out the records with transactions in past 3 months     
    Returns
    -------
    df : DataFrame
        DataFrame with key,num_competitors
    
    """
    
    #Define the join key
    join_key="key"
    
    #Formulate the query 
    query = "SELECT distinct fiscal,destination_no,bid_line_no,price_competitor_id,committed_price_ty FROM "+schemas[selected_schema]+"."+all_tables['all_bid_table_intrmdt']+" WHERE committed_price_ty > 0"
    temp=pd.read_sql(query,engine)
    
    # Filtering out CMP to see only competitors and making a key with fiscal+1 to see last year's competitors
    temp = temp.loc[(temp['price_competitor_id'] != "CMP")]
    temp['key'] = ((temp['fiscal']+1).astype(str) + "_" + temp['destination_no'].astype(str) + "_" + temp['bid_line_no'].astype(str))
    
    #Calculation of feature
    num_comp_df = temp.groupby('key').agg({'price_competitor_id': pd.Series.nunique}).reset_index()    
    
    #Rename the columns
    num_comp_df.columns=['key','num_competitors_prevyear']
        
    num_comp_df['Is_NoCompetitionPrevYear'] = np.where(num_comp_df['num_competitors_prevyear'] == 0, 1, 0)
        
    #Join with key df
    df = fc_protocol(num_comp_df,join_key,key_df)
    
    print("Feature with number of competitors in previous year created")
    
    return df


# Feature3: Bid Cost, Qty, Distance etc.
# Level:Key (fiscal, bid_no, destination_no)
# Datasource: raw_data

def bid_cost_qty_distance(WRITE_DB,engine, key_df,schemas,all_tables,fc_protocol,selected_schema):  

    """
    Calculates amount of sales for each key in the past 3 months
        - Find the number of days between the last_trx_date and the billing_date for each invoice
        - Filter out the records with transactions in past 3 months
  
    Returns
    -------
    df : DataFrame
        DataFrame with key,num_competitors
    
    """
    
    #Define the join key
    join_key="key"
    
    #Formulate the query 
    query = "SELECT distinct key,award_price_ty,total_qty_ty, min_purchase_qty_ty, max_supply_qty_ty, distance_ty, stockpile_depot_cost_ty,direct_transfer_depot_cost_ty, freight_cost_ty, fuel_surcharge_cost_ty,equipment_cost_ty,committed_cost_ty FROM "+schemas[selected_schema]+"."+all_tables['all_bid_table_intrmdt']+" WHERE award_price_ty > 0"
    
    temp=pd.read_sql(query,engine)
    
    #Dataframe with feature and key
    bid_df = temp
    bid_df = bid_df.groupby('key').agg({'total_qty_ty': np.mean, 
                                        'distance_ty': np.mean, 
                                        'stockpile_depot_cost_ty': np.mean, 
                                        'direct_transfer_depot_cost_ty': np.mean, 
                                        'freight_cost_ty': np.mean, 
                                        'fuel_surcharge_cost_ty': np.mean, 
                                        'equipment_cost_ty': np.mean, 
                                        'committed_cost_ty': np.mean}).reset_index()
        
    #Join with key df
    df = fc_protocol(bid_df,join_key,key_df)
    
    print("Features with cost, quantity and distance details created")
    
    return df



# Feature4: Average Award price for fiscal, destination
# Level:Key (fiscal, destination_no)
# Datasource: raw_data

def average_awardprice_destination(WRITE_DB,engine, key_df,schemas,all_tables,fc_protocol,selected_schema):  

    """
    Calculates amount of sales for each key in the past 3 months
        - Find the number of days between the last_trx_date and the billing_date for each invoice
        - Filter out the records with transactions in past 3 months
     
    Returns
    -------
    df : DataFrame
        DataFrame with key,num_competitors
    
    """
    
    #Define the join key
    join_key="key"
    
    #Formulate the query 
    query = "SELECT distinct key,fiscal,bid_line_no,destination_no,award_price_ty FROM "+schemas[selected_schema]+"."+all_tables['all_bid_table_intrmdt']+" WHERE award_price_ty > 0"
    temp=pd.read_sql(query,engine) 
        
    #Dataframe with feature and key
    avg_awardprice_df = temp.drop_duplicates().reset_index()
    avg_awardprice_df['fiscal'] = (avg_awardprice_df['fiscal']+1)
    avg_awardprice_df = avg_awardprice_df.groupby(['fiscal','destination_no']).agg({'award_price_ty': np.mean}).rename({'award_price_ty':'avg_awardprice_destination_prevyear'}, axis = 1).reset_index()
    
    avg_awardprice_df = pd.merge(temp,avg_awardprice_df,on=['fiscal','destination_no'])
    
    avg_awardprice_df = avg_awardprice_df[['key','avg_awardprice_destination_prevyear']].groupby('key').first().reset_index()
    
    #Join with key df
    df = fc_protocol(avg_awardprice_df,join_key,key_df)
    
    print("Features with average award price for destination in previous year created")
    
    return df

# Feature5: Average Award price movement for region
# Level:Key (fiscal, destination_no)
# Datasource: raw_data

def avgprice_change_region(WRITE_DB,engine, key_df,schemas,all_tables,fc_protocol,selected_schema):  

    """
    Calculates amount of sales for each key in the past 3 months
        - Find the number of days between the last_trx_date and the billing_date for each invoice
        - Filter out the records with transactions in past 3 months
     
    Returns
    -------
    df : DataFrame
        DataFrame with key,num_competitors
    
    """
    
    #Define the join key
    join_key="key"
    
    #Formulate the query 
    query = "SELECT distinct key,fiscal,bid_line_no,destination_no,sales_region_name,award_price_ty,total_qty_ty FROM "+schemas[selected_schema]+"."+all_tables['all_bid_table_intrmdt']+" WHERE award_price_ty > 0"
    temp=pd.read_sql(query,engine) 
        
    temp['weighted_awardprice'] = temp['award_price_ty']/temp['total_qty_ty']
    
    #Dataframe for previous 1 yr
    avg_awardprice_prev1yr_df = temp
    avg_awardprice_prev1yr_df['fiscal'] = (avg_awardprice_prev1yr_df['fiscal']+1)
    avg_awardprice_prev1yr_df = avg_awardprice_prev1yr_df[['fiscal','sales_region_name','weighted_awardprice']].drop_duplicates().reset_index()
    avg_awardprice_prev1yr_df = avg_awardprice_prev1yr_df.groupby(['fiscal','sales_region_name']).agg({'weighted_awardprice': np.mean}).rename({'weighted_awardprice':'avg_wt_awardprice_region_prev1year'}, axis = 1).reset_index()
    
    #Dataframe for previous 2 yr
    avg_awardprice_prev2yr_df = temp
    avg_awardprice_prev2yr_df['fiscal'] = (avg_awardprice_prev2yr_df['fiscal']+2)
    avg_awardprice_prev2yr_df = avg_awardprice_prev2yr_df[['fiscal','sales_region_name','weighted_awardprice']].drop_duplicates().reset_index()
    avg_awardprice_prev2yr_df = avg_awardprice_prev2yr_df.groupby(['fiscal','sales_region_name']).agg({'weighted_awardprice': np.mean}).rename({'weighted_awardprice':'avg_wt_awardprice_region_prev2year'}, axis = 1).reset_index()
    
    
    avg_awardprice_df = temp
    avg_awardprice_df = pd.merge(avg_awardprice_df,avg_awardprice_prev1yr_df,on=['fiscal','sales_region_name'])
    avg_awardprice_df = pd.merge(avg_awardprice_df,avg_awardprice_prev2yr_df,on=['fiscal','sales_region_name'])
    
    avg_awardprice_df['per_pricemovement_region'] = avg_awardprice_df['avg_wt_awardprice_region_prev1year']/avg_awardprice_df['avg_wt_awardprice_region_prev2year'] - 1
    
    avg_awardprice_df = avg_awardprice_df[['key','avg_wt_awardprice_region_prev1year','per_pricemovement_region']].groupby('key').first().reset_index()
    
    #Join with key df
    df = fc_protocol(avg_awardprice_df,join_key,key_df)
    
    print("Features with average award price for region and its movement in previous years created")
    
    return df


# Feature5: Min, Max Quantity
# Level:Key (fiscal, destination_no, customer_no)
# Datasource: raw_data

def min_max_qty(WRITE_DB,engine, key_df,schemas,all_tables,fc_protocol,selected_schema):  

    """
    Calculates amount of sales for each key in the past 3 months
        - Find the number of days between the last_trx_date and the billing_date for each invoice
        - Filter out the records with transactions in past 3 months
    
    """
    
    #Define the join key
    join_key="key"
    
    #Formulate the query 
    query = "SELECT distinct key,min_purchase_qty_ty,max_supply_qty_ty,total_qty_ty FROM "+schemas[selected_schema]+"."+all_tables['all_bid_table_intrmdt']+" WHERE award_price_ty > 0"
    temp=pd.read_sql(query,engine) 
        
    #Dataframe with feature and key
    minmax_qty_df = temp.groupby('key').agg({'min_purchase_qty_ty': np.mean,
                                             'max_supply_qty_ty': np.mean,
                                             'total_qty_ty': np.mean}).reset_index()
    
    minmax_qty_df['min_qty_per'] = minmax_qty_df['min_purchase_qty_ty']/minmax_qty_df['total_qty_ty']
    minmax_qty_df['max_qty_per'] = minmax_qty_df['max_supply_qty_ty']/minmax_qty_df['total_qty_ty']

    minmax_qty_df['Is_minmaxqtypresent'] = np.where(minmax_qty_df['max_qty_per'] > 0, 1, 0)
    
    minmax_qty_df = minmax_qty_df[['key','total_qty_ty','min_qty_per','max_qty_per','Is_minmaxqtypresent']]
    
    #Join with key df
    df = fc_protocol(minmax_qty_df,join_key,key_df)
    
    print("Features with total, min & max quantity and respective % created")
    
    return df



# Feature6: Timeframe of contract
# Level:Key (fiscal, destination_no, customer_no)
# Datasource: raw_data

def contract_timeframe(WRITE_DB,engine, key_df,schemas,all_tables,fc_protocol,selected_schema):  

    """
    Calculates amount of sales for each key in the past 3 months
        - Find the number of days between the last_trx_date and the billing_date for each invoice
        - Filter out the records with transactions in past 3 months
    
    """
    
    #Define the join key
    join_key="key"
    
    #Formulate the query 
    query = "SELECT distinct key,effective_date,expiry_date FROM "+schemas[selected_schema]+"."+all_tables['all_bid_table_intrmdt']+" WHERE award_price_ty > 0"
    temp=pd.read_sql(query,engine) 
        
    #Dataframe with feature and key
    temp['contract_timeframe'] = [int(i.days) for i in ((temp['expiry_date']) - (temp['effective_date']))]
    timeframe_df = temp.groupby('key').agg({'contract_timeframe': np.mean}).reset_index()
    
    #Join with key df
    df = fc_protocol(timeframe_df,join_key,key_df)
    
    print("Features with contract timeframe created")
    
    return df


# Feature7: Rollover
# Level:Key (fiscal, destination_no, customer_no)
# Datasource: raw_data

def rollover_contract(WRITE_DB,engine, key_df,schemas,all_tables,fc_protocol,selected_schema):  

    """
    Calculates amount of sales for each key in the past 3 months
        - Find the number of days between the last_trx_date and the billing_date for each invoice
        - Filter out the records with transactions in past 3 months    
    """
    
    #Define the join key
    join_key="key"
    
    #Formulate the query 
    query = "SELECT distinct key,rollover FROM "+schemas[selected_schema]+"."+all_tables['all_bid_table_intrmdt']+" WHERE award_price_ty > 0"
    temp=pd.read_sql(query,engine)
        
    #Dataframe with feature and key
    rollover_df = temp.groupby('key').first().reset_index()
    
    #Join with key df
    df = fc_protocol(rollover_df,join_key,key_df)
    
    print("Features with rollover flag created")
    
    return df


# Feature8: Next Best competitor
# Level:Key (fiscal, destination_no, customer_no)
# Datasource: raw_data

def next_best_competitor(WRITE_DB,engine, key_df,schemas,all_tables,fc_protocol,selected_schema):  
    
    """
    Calculates amount of sales for each key in the past 3 months
        - Find the number of days between the last_trx_date and the billing_date for each invoice
        - Filter out the records with transactions in past 3 months    
    """
    
    #Define the join key
    join_key="key"
    
    #Formulate the query 
    query = "SELECT distinct key,fiscal,destination_no,bid_line_no,price_competitor_id, committed_price_ty, award_price_ty, award_competitor_id, award_price_ty, (committed_price_ty - award_price_ty) as price_gap FROM "+schemas[selected_schema]+"."+all_tables['all_bid_table_intrmdt']+" WHERE award_price_ty > 0 and committed_price_ty > 0"
    temp=pd.read_sql(query,engine)
        
    # Create a pseudo key to fetch data from previous year
    temp['key'] = ((temp['fiscal']+1).astype(str) + "_" + temp['destination_no'].astype(str) + "_" + temp['bid_line_no'].astype(str))
    
    
    # Previous year details for next best competitor
    prevyr_df = temp.loc[temp['price_competitor_id'] != "CMP"]
    
    price_gap_df = prevyr_df.groupby('key').agg({'price_gap':np.min}).reset_index()
    price_gap_df = price_gap_df.rename(columns = {'price_gap':'min_price_gap_nbc'})
    
    prevyr_df = pd.merge(prevyr_df, price_gap_df, on = ['key'])
    
    nbc_df = prevyr_df.loc[prevyr_df['price_gap'] == prevyr_df['min_price_gap_nbc']] # nbc is for next_best_competitor
    nbc_df = nbc_df[['key','price_competitor_id','committed_price_ty','min_price_gap_nbc']].drop_duplicates().groupby('key').first().reset_index()
    
    nbc_df['Is_cargill_nbc'] = np.where(nbc_df['price_competitor_id'] == 'CS', 1, 0)
    nbc_df['Is_KplusS_nbc'] = np.where(nbc_df['price_competitor_id'] == 'MS', 1, 0)
    
    nbc_df = nbc_df.rename(columns = {'price_competitor_id':'next_best_competitor',
                                      'committed_price_ty':'nbc_committed_price_prevyr'})
    
    # Previous year details for CMP
    cmp_prevyr_df = temp.loc[temp['price_competitor_id'] == "CMP"]
    cmp_prevyr_df = cmp_prevyr_df.drop_duplicates().groupby('key').first().reset_index()
    cmp_prevyr_df = cmp_prevyr_df[['key','committed_price_ty','award_competitor_id']]
    cmp_prevyr_df = cmp_prevyr_df.rename(columns = {'committed_price_ty':'cmp_committed_price_prevyr'})
    cmp_prevyr_df['Is_cmp_awarded_prevyr'] = np.where(cmp_prevyr_df['award_competitor_id'] == "CMP", 1, 0)
    
    
    # Merging nbc and cmp data from previous yr
    cmp_nbc_df = pd.merge(nbc_df,cmp_prevyr_df, on = ['key'])
    cmp_nbc_df['cmp_nbc_pricediff_prevyr'] = cmp_nbc_df['cmp_committed_price_prevyr'] - cmp_nbc_df['nbc_committed_price_prevyr']
    
    cmp_nbc_df = cmp_nbc_df.drop(['next_best_competitor','award_competitor_id'],axis = 1)
                                             

    #Join with key df
    df = fc_protocol(cmp_nbc_df,join_key,key_df)
    
    print("Features with next best competitor created")
    
    return df



# Feature9: Fill Rate
# Level:Key (fiscal, destination_no, customer_no)
# Datasource: raw_data

def fill_rate(WRITE_DB,engine, key_df,schemas,all_tables,fc_protocol,selected_schema):  
    
    """
    Calculates amount of sales for each key in the past 3 months
        - Find the number of days between the last_trx_date and the billing_date for each invoice
        - Filter out the records with transactions in past 3 months    
    """
    
    #Define the join key
    join_key="key"
    
    #Formulate the query 
    query = "SELECT distinct fiscal,destination_no,bid_line_no,bid_due_date,price_competitor_id,award_competitor_id,total_qty_ty FROM "+schemas[selected_schema]+"."+all_tables['all_bid_table_intrmdt']+" WHERE committed_price_ty > 0 and award_price_ty > 0"
    temp=pd.read_sql(query,engine)
        
    # Create a pseudo key to fetch data from previous year
    temp['key'] = ((temp['fiscal']).astype(str) + "_" + temp['destination_no'].astype(str) + "_" + temp['bid_line_no'].astype(str))
    
    temp['bid_due_date'] = pd.to_datetime(temp['bid_due_date'],format = "%Y-%m-%d")
    
    
    # Check how much is the fill rate for competitors by any date
    wonbids = temp.loc[temp['price_competitor_id'] == temp['award_competitor_id']]
    wonbids = wonbids.groupby(['fiscal','price_competitor_id','bid_due_date']).agg({'total_qty_ty':np.sum}).reset_index()
    wonbids = wonbids.sort_values(by=['fiscal','price_competitor_id','bid_due_date'])
    
    wonbids['per_total_capacity'] = wonbids['total_qty_ty']/wonbids.groupby(['fiscal','price_competitor_id'])['total_qty_ty'].transform('sum')/1.25
    wonbids['fill_rate'] = wonbids.groupby(['fiscal','price_competitor_id'])['per_total_capacity'].cumsum()
    
    wonbids = wonbids.drop(['per_total_capacity'],axis=1)
    wonbids['bid_due_date'] = pd.to_datetime(wonbids['bid_due_date'],format = "%Y-%m-%d")
    
    
    # Create a dataframe with all the fiscal, pricecompetitor, date combinations from 2016-01-01 till 2019-12-31
    date1 = '2015-01-01'
    date2 = '2019-12-31'
    mydates = pd.date_range(date1, date2).tolist()
    mydates = pd.DataFrame({'bid_due_date': [d.strftime("%Y-%m-%d") for d in mydates]})
    mydates['k'] = 1
    mydates['bid_due_date'] = pd.to_datetime(mydates['bid_due_date'],format = "%Y-%m-%d")
    
    fis_comp_comb = temp[['fiscal','price_competitor_id']].drop_duplicates().reset_index(drop = True)
    fis_comp_comb['k'] = 1
    fis_comp_dt_comb = pd.merge(fis_comp_comb,mydates,on=['k']).drop(['k'],axis=1)
    
    # Merge fiscal, competitor, date combination with fill rate
    # only dates on which competitor won the bid will have fill rate
    # For other dates, we will need to do an upward fill of same value as below
    
    fis_comp_dt_comb['bid_due_date'] = pd.to_datetime(fis_comp_dt_comb['bid_due_date'],format = "%Y-%m-%d")
    
    fill_rate_date = pd.merge(fis_comp_dt_comb, wonbids, on = ['fiscal','price_competitor_id','bid_due_date'], how='outer')
    t = fill_rate_date.loc[(fill_rate_date['fiscal'] == 2016) & (fill_rate_date['price_competitor_id'] == "AM")]
    #     above table has these columns - (fiscal,price_competitor_id,bid_due_date,fill_rate)
    
    fill_rate_date = fill_rate_date.sort_values(by = ['fiscal','price_competitor_id','bid_due_date']).reset_index()
    fill_rate_date['fill_rate'] = fill_rate_date.groupby(['fiscal','price_competitor_id'])['fill_rate'].apply(lambda x : x.ffill())
    fill_rate_date['fill_rate'] = fill_rate_date['fill_rate'].fillna(0)
    
    
    fill_rate_date_cmp = fill_rate_date.loc[fill_rate_date['price_competitor_id'] == "CMP"]
    fill_rate_date_cmp = fill_rate_date_cmp.rename(columns = {'fill_rate':'fill_rate_CMP'})
    fill_rate_date_cmp = fill_rate_date_cmp[['fiscal','bid_due_date','fill_rate_CMP']]
    
#     print(fill_rate_date.iloc[:100,:])
    
    # Creating final df for feature
    # (key, fill_rate)
    
    winner_fill_rate_df = pd.merge(temp,fill_rate_date,on=['fiscal','price_competitor_id','bid_due_date'])
    winner_fill_rate_df = pd.merge(winner_fill_rate_df,fill_rate_date_cmp,on=['fiscal','bid_due_date'])
    winner_fill_rate_df = winner_fill_rate_df.loc[winner_fill_rate_df['price_competitor_id'] == winner_fill_rate_df['award_competitor_id']]
    winner_fill_rate_df = winner_fill_rate_df.groupby('key').first().reset_index()
    winner_fill_rate_df = winner_fill_rate_df[['key','fill_rate_CMP']] # add fill_rate as well for nbc
#     winner_fill_rate_df[winner_fill_rate_df==np.inf]=np.nan
                                       
    #Join with key df
    df = fc_protocol(winner_fill_rate_df,join_key,key_df)
    
    print("Features with winner's fillrate created")
    
    return df



# Flags for bid/bidline
# Level:Key (fiscal, destination_no, customer_no)
# Datasource: raw_data

def flag_bid(WRITE_DB,engine, key_df,schemas,all_tables,fc_protocol,selected_schema):  

    """
    Calculates amount of sales for each key in the past 3 months
        - Find the number of days between the last_trx_date and the billing_date for each invoice
        - Filter out the records with transactions in past 3 months    
    """
    
    #Define the join key
    join_key="key"
    
    #Formulate the query 
    query = "SELECT distinct key,bid_name,bid_no,bid_line_no,destination_state_prov as state FROM "+schemas[selected_schema]+"."+all_tables['all_bid_table_intrmdt']+" WHERE award_price_ty > 0"
    temp=pd.read_sql(query,engine)
        
    #Dataframe with feature and key
    # Check if state has just one bid line number or not    
    state_level = temp.groupby('state')['bid_line_no'].nunique().reset_index()
    state_level['bid_line_no'] = np.where(state_level['bid_line_no'] == 1,1,0)
    state_level.columns = ['state','is_onlybid_in_state']
    
    # check if bid is an emergency or supplementary
    
    bid_supp_emerg = temp[['key','bid_name']].drop_duplicates()
    bid_supp_emerg['is_emergency_bid'] = bid_supp_emerg['bid_name'].str.lower().str.match(pat = '(emergency)')
    bid_supp_emerg['is_supplemental_bid'] = bid_supp_emerg['bid_name'].str.lower().str.match(pat = '(supplemental)')
    bid_supp_emerg = bid_supp_emerg[['key','is_emergency_bid','is_supplemental_bid']]
    
    
    all_flags_df = temp[['key','state']].drop_duplicates()
    all_flags_df = pd.merge(all_flags_df, state_level,'left',on='state').drop(['state'],axis=1)
    all_flags_df = pd.merge(all_flags_df, bid_supp_emerg,'left',on='key')
    
    all_flags_df = all_flags_df.groupby('key').first().reset_index()
    
#     print(all_flags_df.head)
    
    #Join with key df
    df = fc_protocol(all_flags_df,join_key,key_df)
    
    print("Features with flags for emergency and cupplementary bid and for only bid in state created")
    
    return df


# Feature - Cluster level attributes
# Level:Key (fiscal, destination_no, customer_no)
# Datasource: raw_data

def cluster_features(WRITE_DB,engine, key_df,schemas,all_tables,fc_protocol,selected_schema):  

    """
    // description of features
    """
    
    #Define the join key
    join_key="key"
    
    #Formulate the query 
    query1 = "SELECT distinct key,fiscal,price_competitor_id,committed_price_ty,award_price_ty FROM "+schemas[selected_schema]+"."+all_tables['all_bid_table_intrmdt']+" WHERE award_price_ty > 0 and committed_price_ty > 0"
    temp1=pd.read_sql(query1,engine)
    
    query2 = "SELECT * FROM "+schemas[selected_schema]+"."+all_tables['cluster_table']
    temp2=pd.read_sql(query2,engine)
    
    temp2 = temp2.groupby('key').first().reset_index()
    cluster_base = pd.merge(temp1, temp2, how = 'left', on = 'key')
    
    key_base = cluster_base.groupby(['key']).first().reset_index()
    key_base = key_base[['key','fiscal','cluster']]
    cluster_base['fiscal'] = cluster_base['fiscal'] + 1
    
    # average award price previous year
    avg_comm_price = cluster_base[['cluster','fiscal','key','price_competitor_id','committed_price_ty']].drop_duplicates().reset_index(drop=True)
    avg_comm_price = avg_comm_price.groupby(['fiscal','cluster']).agg({'committed_price_ty':np.mean}).reset_index()
    
    avg_comm_price.columns = ['fiscal','cluster','avg_committed_price_prevyr_cluster']
    
    # number of distinct competitors in cluster
    num_competitors = cluster_base[['cluster','fiscal','price_competitor_id']].drop_duplicates().reset_index(drop=True)
    num_competitors = num_competitors.groupby(['fiscal','cluster']).agg({'price_competitor_id':pd.Series.nunique}).reset_index()
    
    num_competitors.columns = ['fiscal','cluster','num_competitors_prevyr_cluster']
    
    # number of distinct competitors in cluster
    avg_award_price = cluster_base[['cluster','fiscal','award_price_ty']].drop_duplicates().reset_index(drop=True)
    avg_award_price = avg_award_price.groupby(['fiscal','cluster']).agg({'award_price_ty':np.mean}).reset_index()
    
    avg_award_price.columns = ['fiscal','cluster','avg_award_price_prevyr_cluster']
    
    
    cluster_features_df = pd.merge(key_base, avg_comm_price, 'left', on = ['cluster','fiscal'])
    cluster_features_df = pd.merge(cluster_features_df, avg_award_price, 'left', on = ['cluster','fiscal'])
    cluster_features_df = pd.merge(cluster_features_df, num_competitors, 'left', on = ['cluster','fiscal'])
    
    #Join with key df
    df = fc_protocol(cluster_features_df,join_key,key_df)
    
    print("Features with cluster level avg award price, committed price and num of competitors created")
    
    return df








