
# coding: utf-8

#Import libraries
import shap
import pandas as pd
import numpy as np
import xgboost
import sklearn
from sklearn.externals import joblib
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import sys, os

#Import configuration and common libraries
from int_dir.config_file import *
from helper_modules.ai_db_connect import *

def col_supress(row):
    
    REJ_COLUMNS = {'churn_prob','key','customer','product'}
    
    cols = [i for i in df.columns if i not in REJ_COLUMNS]
    
    for col in cols:
        if row[col]<0:
            row[col] = 0
    return row

def get_data(qry):
    engine = conn_eng()
    df = pd.read_sql(qry,engine)
    engine.dispose()
    del engine
    return df

#Set up environment variables
PROD_REF_DATE = str(datetime.date.today()- relativedelta(years=3))
VAL_QRY = "select key,max(last_trx_date) as last_trx_date,max(diff_mean) as diff_mean, max(diff_std) as diff_std,sum(inv_delivered_gp) as value_at_risk from (select i.key,i.inv_delivered_gp, i.billing_date, k.last_trx_date, k.base_date, k.diff_mean, k.diff_std from "+schemas['scoring_schema']+"."+all_tables['dm_invoice_features']+" as i left join (select key, last_trx_date,diff_mean,diff_std, (last_trx_date-interval '1 year')::date as base_date  from "+schemas['scoring_schema']+"."+all_tables['scoring']+") as k on i.key = k.key where i.billing_date>=k.base_date and i.billing_date<=k.last_trx_date) as X group by key"

prd_qry = "SELECT customer_bill_to_id,max(horizontal) as horizontal, "+PRODUCT_HIERARCHY+",product_no,sum(inv_delivered_gp) as inv_dgp FROM "+schemas['input_schema']+"."+all_tables['invoice_table']+" where billing_date >= '"+PROD_REF_DATE+"' and customer_bill_to_id in (SELECT distinct customer_bill_to_id FROM "+schemas['scoring_schema']+"."+all_tables['dm_invoice_features']+") group by customer_bill_to_id, "+PRODUCT_HIERARCHY+",product_no"

# odr_qry = "select distinct i.customer_bill_to_id,j."+PH_CODE+" from \
# (select distinct customer_bill_to_id,product_code from "+schemas['input_schema']+"."+all_tables['sales_order_otif_table']+" \
# where line_item_status not in ('CNCL','CLSD')and order_entry_date >= '"+SCORING_DATE_START+"' \
# and order_entry_date <= '"+SCORING_DATE_END+"') as i \
# left join "+schemas['input_schema']+"."+all_tables['prod_dm_table']+" as j \
# on i.product_code = j.product_no order by customer_bill_to_id"


odr_qry = "select distinct i.customer_bill_to_id,j."+PH_CODE+" from (select distinct t.customer_bill_to_id, t.product_code from "+schemas['input_schema']+"."+all_tables['sales_order_otif_table']+" t inner join (   select customer_bill_to_id, product_code ,max(order_entry_date) as MaxDate     from "+schemas['input_schema']+"."+all_tables['sales_order_otif_table']+"     group by customer_bill_to_id, product_code ) tm on t.customer_bill_to_id = tm.customer_bill_to_id and t.order_entry_date = tm.MaxDate where t.line_item_status not in ('CNCL','CLSD') and t.order_entry_date >= '"+SCORING_DATE_START+"' and     t.order_entry_date <= '"+SCORING_DATE_END+"') as i left join "+schemas['static_input_schema']+"."+all_tables['prod_dm_table']+" as j on i.product_code = j.product_no order by customer_bill_to_id"

# VAR_CLASS_FILE = CLASSES_FILE
# REP_MAP = CUST_REP_MAPPING

#Churn probability cuts
#Intrinsic risk cut_off
INT_CUT_OFF = 0.3

#percentiles for cut-offs
q = [0.10,0.30,0.60, 1.0]

if DB_READ:
    qry1 = 'select * from '+ schemas['output_schema']+'.'+all_tables['scoring_table']
    qry2 = 'select * from '+ schemas['output_schema']+'.'+all_tables['output_table']
    df = get_data(qry1)
    base_table = get_data(qry2)
    
else:
    df = pd.read_csv(OUTPUT_PATH_SCORING + all_tables['scoring_table'] + '.csv')
    base_table = pd.read_csv(OUTPUT_PATH_PREPED + all_tables['output_table'] + '.csv')

#Number of recommendations to generate
N = round(len(df)*0.042)

value_df = get_data(VAL_QRY)

ord_flag_df = get_data(odr_qry)

ord_flag_df[PH_CODE] = ord_flag_df[PH_CODE].str.replace('-','_')
ord_flag_df['customer_bill_to_id'] = ord_flag_df['customer_bill_to_id'].astype(str)

#order pattern variables
ord_cuts = {'Very High':{'lower':1.5,'upper':2},'High':{'lower':1,'upper':1.5},
            'Medium':{'lower':0,'upper':1}, 'Low':0}
order_ref_date = max(value_df.last_trx_date) + timedelta(days=1)

risk_vals = ['Very High','High','Medium','Low','Very Low']
order_risk_vals = ['Well past due','Past due','Within order pattern']

#import customer and product attributes
cust_rep_map_query="SELECT * FROM "+schemas['static_input_schema']+"."+all_tables['cust_rep_mapping']
atts = get_data(cust_rep_map_query)

cust_att = atts[['Product LOB','Bill To Key','New Region','New District','General Manager','Sales Manager',
                 'Seller','Bill To Name']].drop_duplicates().reset_index(drop = True)
cust_att['Bill To Key'] = cust_att['Bill To Key'].astype(str)
cust_att = cust_att.rename(columns = {'New Region':'Region',
                                      'New District':'District',
                                      'Product LOB':'LOB'})
cust_att = cust_att.drop_duplicates(['Bill To Key','LOB'], keep = 'first').reset_index(drop=True)

prod_att = atts[['Product Key', 'Product Name']].drop_duplicates().reset_index(drop = True)
prod_att['Product Key'] = prod_att['Product Key'].astype(str)

product_query="SELECT * FROM "+schemas['static_input_schema']+"."+all_tables['prod_dm_table']
prod_df = get_data(product_query)
    
prod_df = prod_df[[PRODUCT_HIERARCHY,PH_CODE]].drop_duplicates().reset_index(drop = True)
prod_df = prod_df.dropna()
#prod_df[PH_CODE] = prod_df[PH_CODE].str.replace('-','_')

cpp_query="SELECT * FROM "+schemas['static_input_schema']+"."+all_tables['CPP_table']
cpp_df = get_data(cpp_query)
cpp_df = cpp_df.fillna('0')
cpp_df['Customer'] = cpp_df['Customer Ship To'].astype(str).str[0:6]

brtg_df = cpp_df[cpp_df['Supplier Name'].str.contains('brenntag',regex=False, case = False)]
nex_df = cpp_df[cpp_df['Supplier Name'].str.contains('nexeo',regex=False, case = False)]

#Import variable classes
var_class_query="SELECT * FROM "+schemas['static_input_schema']+"."+all_tables['varclass_table']
vclass = get_data(var_class_query) 
    
#Import Saved model pickle
churn_model = joblib.load((OUTPUT_PATH_PICKLES+MODEL_FILE_NAME))

#Create dummies for the data 
NON_FEATURE_COLUMNS = {'target','key','last_trx_date','rep_tnr', PRODUCT_HIERARCHY}

cols = [i for i in df.columns if i not in NON_FEATURE_COLUMNS]

#save keys to add to final dataframe
keys = df['key']

df = pd.get_dummies(df[cols])

base_table = pd.get_dummies(base_table[cols])

# Deal with missing or extra columns
missing_columns = (set(base_table.columns) - set(df.columns))-NON_FEATURE_COLUMNS
for col in missing_columns:
    df[col] = float('nan')

columns_to_drop = list(set(df.columns) -set(base_table.columns))
df = df.drop(columns_to_drop, axis = 1)

#Rearrange columns to input into model
df = df[base_table.columns]

#Now, create the predictions
predicted_risk = churn_model.predict_proba(df)
predicted_risk_df = pd.DataFrame(predicted_risk)

explainer = shap.TreeExplainer(churn_model)
shap_values = explainer.shap_values(df)
shap_df = pd.DataFrame(shap_values)
shap_df.columns = df.columns
shap_df["churn_prob"] = predicted_risk_df[1]
shap_df["key"] = keys

shap_right = shap_df


shap_df[['Customer','Product']] = shap_df['key'].str.split('_',n=1, expand = True)

shap_df = pd.merge(shap_df, value_df, on = 'key', how = 'left')

get_cols = ['Customer','Product']+ ['churn_prob','value_at_risk','last_trx_date','diff_mean','diff_std']

cat_cols = dict.fromkeys(set(vclass.Class))
for cat in cat_cols.keys():
    cat_cols[cat] = []
    for var in vclass.Variable[vclass.Class == cat]:
        cat_cols[cat].extend([s for s in shap_right.columns if var.strip() in s])

shap_right = shap_right.apply(lambda row: col_supress(row),axis=1)

top_df = pd.DataFrame()
for cat in cat_cols.keys():
    top_df[cat] = shap_right[cat_cols[cat]].sum(axis=1)

top_df['odds_total'] = top_df.apply(lambda r: get_row_tot(r),axis=1)

driver_cols = ['Top Driver '+str(i) for i in range(1,TOP_N_DRIVERS+1,1)]

top_df[driver_cols] = top_df.apply(lambda r: get_row_cont(r),axis=1)

top_df = top_df[driver_cols]

final_out = pd.concat([shap_df[get_cols],top_df], axis = 1)

prd_df = get_data(prd_qry)

int_df = prd_df.groupby(['customer_bill_to_id',PRODUCT_HIERARCHY])[['inv_dgp']].idxmax()

hz_df = prd_df[[PRODUCT_HIERARCHY,'horizontal']].groupby(PRODUCT_HIERARCHY)[['horizontal']].max().reset_index()

subset_df = prd_df.drop('horizontal', axis='columns').iloc[int_df["inv_dgp"].values,:].rename(columns = {'customer_bill_to_id':'Customer',
                                                                     PRODUCT_HIERARCHY:'Product',
                                                                     'product_no':'Top Product',
                                                                     'inv_dgp':'top_prod_idgp'})
subset_df = pd.merge(subset_df,hz_df, left_on = 'Product', 
                     right_on = PRODUCT_HIERARCHY, 
                     how = 'left').drop(PRODUCT_HIERARCHY, axis = 'columns').rename(columns = {'horizontal':'Horizontal'})

subset_df['Customer'] = subset_df['Customer'].astype(str)

output = pd.merge(final_out, subset_df[['Customer','Horizontal','Product','Top Product']], on = ['Customer','Product'], how= 'left')

output['Top Product'] = output['Top Product'].replace(np.nan,0)
output['Top Product'] = output['Top Product'].astype(int).astype(str)
output['LOB'] = LOB.upper()

output = pd.merge(output,cust_att, 
                  left_on = ['Customer','LOB'], 
                  right_on = ['Bill To Key','LOB'], 
                  how = 'left', 
                  indicator = False, validate = 'm:1').drop(['Bill To Key'], axis = 'columns')
output = pd.merge(output,prod_att, 
                  left_on = ['Top Product'], 
                  right_on = ['Product Key'], 
                  how = 'left', 
                  indicator = False, validate = 'm:1').drop(['Product Key'], axis = 'columns')

output = pd.merge(output,prod_df, 
                  left_on = ['Product'], 
                  right_on = [PRODUCT_HIERARCHY], 
                  how = 'left', 
                  indicator = False, validate = 'm:1').drop([PRODUCT_HIERARCHY], axis = 'columns')

output['Open Order Flag'] = np.where((output['Customer']+'_'+output[PH_CODE]).isin(ord_flag_df['customer_bill_to_id']+'_'+ord_flag_df[PH_CODE]),
                                     'Yes','No')


#Setting up dataframe as per desired output
output = output[['LOB','Region', 'District', 'General Manager', 'Sales Manager', 'Seller',
                 'Customer', 'Bill To Name','Horizontal','Product','Top Product','Product Name','churn_prob',
                 'value_at_risk', 'last_trx_date','diff_mean', 'diff_std','Open Order Flag',PH_CODE]+driver_cols]

output['Annual customer iDGP at risk'] = output.groupby('Customer')['value_at_risk'].transform(np.sum)

output['order_ref_date'] = order_ref_date

output['order_diff'] = output['order_ref_date'] - output['last_trx_date']

output['Order deviation risk'] = output[['diff_mean',
                               'diff_std',
                               'order_diff']].apply(lambda row: order_risk(row['diff_mean'],
                                                                                   row['diff_std'],
                                                                                   row['order_diff']),axis=1)

output['churn_risk'] = output[['churn_prob','diff_mean',
                               'diff_std',
                               'order_diff']].apply(lambda row: churn_risk(row['churn_prob'],row['diff_mean'],
                                                                                   row['diff_std'],
                                                                                   row['order_diff']),axis=1)

leftover_df = output[(output['churn_prob']/output['churn_risk'])<=INT_CUT_OFF]
output = output[(output['churn_prob']/output['churn_risk'])>INT_CUT_OFF]

output = output.sort_values(by = ['churn_risk'],ascending = False).reset_index(drop = True)

output['Total churn risk'] = np.where(output.index < int(N*q[0]),risk_vals[0],
                                      np.where(output.index < int(N*q[1]),
                                                risk_vals[1],
                                                np.where(output.index < int(N*q[2]),
                                                         risk_vals[2],
                                                         np.where(output.index <= int(N*q[3]),risk_vals[3],risk_vals[4]))))
leftover_df['Total churn risk'] = "Unavailable"

output = pd.concat([output, leftover_df], axis = "rows", ignore_index = True)

output = output.rename(columns = {'Bill To Name':'Customer Name','Product':'Product Family',
                                  'value_at_risk':'Annual product iDGP at risk','diff_mean':'Avg. days between orders',
                                  'Product Name':'Top Product Name','last_trx_date':'Last Transaction Date',
                                 'order_diff':'Days since last transaction', PH_CODE:'Product Family Code',
                                  'diff_std':'Standard deviation for order days',
                                 'last_ord_date':'Last Order Date'})

output['Days since last transaction'] = output['Days since last transaction'].dt.days
output['Brenntag CPP'] = np.where(output['Customer'].isin(brtg_df['Customer'].unique()),'Yes',' ')
output['Nexeo CPP'] = np.where(output['Customer'].isin(nex_df['Customer'].unique()),'Yes',' ')
output['AI_Source'] = 'Churn Prevention'

#Sense check numbers
output['value_test'] = output['churn_prob'] * output['Annual product iDGP at risk']

print(output[output['Total churn risk'].isin(['Very High','High','Medium'])]['Annual product iDGP at risk'].sum())

print(output[['Total churn risk','churn_prob']].groupby('Total churn risk')[['churn_prob']].mean())

print(output[['Total churn risk','churn_prob']].groupby('Total churn risk')[['churn_prob']].min())

print(output[['Total churn risk','churn_prob']].groupby('Total churn risk')[['churn_prob']].max())

output = output[['AI_Source','LOB', 'Region', 'District', 'General Manager', 'Sales Manager', 'Seller','Customer',
                 'Customer Name','Product Family Code','Horizontal','Product Family','Top Product', 'Top Product Name',
                 'Total churn risk','Annual product iDGP at risk','Annual customer iDGP at risk','Last Transaction Date',
                 'Open Order Flag','Avg. days between orders','Days since last transaction','Brenntag CPP','Nexeo CPP',
                 'Order deviation risk','churn_prob','churn_risk'] + driver_cols]
#############################################################################################################
ind_qry = "select distinct t.customer_bill_to_id::text as customer, t.uic_industry_group_name as industry             from raw_data.invoice_2018_10_17 t             inner join (             select customer_bill_to_id, max(billing_date) as MaxDate             from raw_data.invoice_2018_10_17             group by customer_bill_to_id             ) tm on t.customer_bill_to_id = tm.customer_bill_to_id and t.billing_date = tm.MaxDate"

ind_df = get_data(ind_qry)
ind_df = ind_df.drop_duplicates(['customer'],keep = 'first')
#############################################################################################################
lc_qry = "select customer_bill_to_id::text as customer, max(call_date) as last_call_date             from "+schemas['input_schema']+"."+all_tables['salescall_table']+" group by customer_bill_to_id"

lc_df = get_data(lc_qry)

#############################################################################################################
sc_qry = "select customer_bill_to_id::text as customer , count(distinct case_number) as open_cases             from "+schemas['input_schema']+"."+all_tables['service_case']+"             where case_status in ('Assigned','Draft','Awaiting Investigator Assignment','Under Investigation')             and case_record_type in ('Customer Complaint','Customer Complaint Assigned',             'Internal Complaint','Internal Complaint Assigned')             group by customer_bill_to_id"

sc_df = get_data(sc_qry)

#############################################################################################################
output = pd.merge(output,ind_df, 
                  left_on = ['Customer'], 
                  right_on = ['customer'], 
                  how = 'left', 
                  indicator = False, validate = 'm:1').drop(['customer'], 
                                                            axis = 'columns').rename(columns = {'industry':'Industry'})
output = pd.merge(output,lc_df, 
                  left_on = ['Customer'], 
                  right_on = ['customer'], 
                  how = 'left', 
                  indicator = False, validate = 'm:1').drop(['customer'],axis = 'columns')

output = pd.merge(output,sc_df, 
                  left_on = ['Customer'], 
                  right_on = ['customer'], 
                  how = 'left', 
                  indicator = False, validate = 'm:1').drop(['customer'], axis = 'columns')
#############################################################################################################
#chris output
if LOB == 'lcd':
    chris = output[['AI_Source','LOB', 'Region', 'District', 'General Manager', 'Sales Manager', 'Seller','Customer',
                     'Customer Name','Horizontal','Product Family Code','Product Family','Top Product', 'Top Product Name',
                     'Total churn risk','Annual product iDGP at risk','Annual customer iDGP at risk','Last Transaction Date',
                     'Open Order Flag','Avg. days between orders','Days since last transaction','Brenntag CPP','Nexeo CPP',
                     'Order deviation risk','churn_prob','churn_risk','open_cases','last_call_date'] + driver_cols]
    chris.to_csv((OUTPUT_PATH_SCORD+'chris_'+SCORED_FILE_NAME), index = False, sep= ',')
    
if LOB == 'fi':
    chris = output[['AI_Source','LOB', 'Region', 'Industry', 'General Manager', 'Sales Manager', 'Seller','Customer',
                     'Customer Name','Horizontal','Product Family Code','Product Family','Top Product', 'Top Product Name',
                     'Total churn risk','Annual product iDGP at risk','Annual customer iDGP at risk','Last Transaction Date',
                     'Open Order Flag','Avg. days between orders','Days since last transaction','Brenntag CPP','Nexeo CPP',
                     'Order deviation risk','churn_prob','churn_risk','open_cases','last_call_date'] + driver_cols]
    chris.to_csv((OUTPUT_PATH_SCORD+'chris_'+SCORED_FILE_NAME), index = False, sep= ',')

#for John
if LOB == 'lcd':
    john = output[['AI_Source','LOB', 'Region', 'District', 'General Manager', 'Sales Manager', 'Seller','Customer',
                 'Customer Name','Horizontal','Product Family','Top Product', 'Top Product Name',
                 'Total churn risk','Annual product iDGP at risk','Annual customer iDGP at risk','Last Transaction Date',
                 'Open Order Flag','Avg. days between orders','Days since last transaction','Brenntag CPP','Nexeo CPP',
                 'Order deviation risk'] + driver_cols]
    john.to_csv((OUTPUT_PATH_SCORD+'ro_'+SCORED_FILE_NAME), index = False, sep= ',')  

if LOB == 'fi':
    fi_hi_query="SELECT * FROM "+schemas['static_input_schema']+"."+all_tables['fi_sales_hrchy_table']
    fi_hi = get_data(fi_hi_query)
    fi_hi = fi_hi.drop_duplicates(['Sales rep'], keep = 'first').reset_index(drop=True)
    john = output.drop(['LOB', 'Region', 'District', 'General Manager', 'Sales Manager','Industry'], axis = 'columns')
    john = pd.merge(john,
                      fi_hi,
                      left_on = ['Seller'],
                      right_on = ['Sales rep'],
                      how = 'left', validate = 'm:1').drop(['Sales rep'], axis = 'columns')
    john = john[['AI_Source','Industry', 'Director', 'Sales Manager', 'Seller','Customer',
                 'Customer Name','Horizontal','Product Family','Top Product', 'Top Product Name',
                 'Total churn risk','Annual product iDGP at risk','Annual customer iDGP at risk','Last Transaction Date',
                 'Open Order Flag','Avg. days between orders','Days since last transaction','Brenntag CPP','Nexeo CPP',
                 'Order deviation risk'] + driver_cols]
    john.to_csv((OUTPUT_PATH_SCORD+'ro_'+SCORED_FILE_NAME), index = False, sep= ',')

output.iloc[0,:]

#for dashbaord
if LOB == 'lcd':
    dashboard = output[['AI_Source','LOB', 'Region', 'District', 'General Manager', 'Sales Manager', 'Seller','Customer',
                 'Customer Name','Product Family Code','Horizontal','Product Family','Top Product', 'Top Product Name',
                 'Total churn risk','Annual product iDGP at risk','Annual customer iDGP at risk','Last Transaction Date',
                 'Open Order Flag','Avg. days between orders','Days since last transaction','Brenntag CPP','Nexeo CPP',
                 'Order deviation risk'] + driver_cols]
    dashboard.to_csv((OUTPUT_PATH_SCORD+SCORED_FILE_NAME), index = False, sep= '|')
    print(dashboard.shape)


if LOB == 'fi':
    fi_hi_query="SELECT * FROM "+schemas['static_input_schema']+"."+all_tables['fi_sales_hrchy_table']
    fi_hi = get_data(fi_hi_query)
    fi_hi = fi_hi.drop_duplicates(['Sales rep'], keep = 'first').reset_index(drop=True)
    dashboard = output.drop(['LOB', 'Region', 'District', 'General Manager', 'Sales Manager','Industry'], axis = 'columns')
    dashboard = pd.merge(dashboard,
                      fi_hi,
                      left_on = ['Seller'],
                      right_on = ['Sales rep'],
                      how = 'left', validate = 'm:1').drop(['Sales rep'], axis = 'columns')
    dashboard = dashboard[['AI_Source','Industry', 'Director', 'Sales Manager', 'Seller','Customer',
                 'Customer Name','Product Family Code','Horizontal','Product Family','Top Product', 'Top Product Name',
                 'Total churn risk','Annual product iDGP at risk','Annual customer iDGP at risk','Last Transaction Date',
                 'Open Order Flag','Avg. days between orders','Days since last transaction','Brenntag CPP','Nexeo CPP',
                 'Order deviation risk'] + driver_cols]
    dashboard.to_csv((OUTPUT_PATH_SCORD+SCORED_FILE_NAME), index = False, sep= '|')
    print(dashboard.shape)

if WRITE_TO_S3_PROD:
    S3_transfer = 'aws s3 cp '+ (OUTPUT_PATH_SCORD+SCORED_FILE_NAME) + ' ' + S3_PROD_PATH
else:
    S3_transfer = 'aws s3 cp '+ (OUTPUT_PATH_SCORD+SCORED_FILE_NAME) + ' ' + S3_DEV_PATH



