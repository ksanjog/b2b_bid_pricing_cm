#Import standard libraries
import pandas as pd
import numpy as np
import sys
import os
import datetime
import time
import multiprocessing
from datetime import timedelta
from dateutil.relativedelta import relativedelta

# For establishing connections to Post Gres
import psycopg2

#For the features repository
import core_modules.features_repository as FP
import inspect

# supress all warnings
import warnings
warnings.filterwarnings("ignore")

#Date of run
RUN_DATE = '2020-01-15'

# Global Variables
WRITE_DB = True # If we are going to write features to master DB
TARGET_DB_WRITE = False # If we are going to re-write the target DB
INTERMEDIATE_TABLE_WRITE = TARGET_DB_WRITE # If we are going to recreate the intermediate tables 
SCORING_DB_WRITE = True #If we are going to re-write the scoring DB
DB_READ = WRITE_DB # If we want to read the model table from a database or a .csv file
TOP_N_DRIVERS = 5

#Model and data parameters
SAVE_MODEL = True # Are we going to save the model or not
#MISSING_VALUE_TREATMENT = True
#OUTLIER_TREATMENT = True

GLOBAL_PATH = '/home/ncod/Sanjog/code/b2b_bid_pricing'

INPUT_PATH = GLOBAL_PATH+'/int_dir/'
OUTPUT_PATH_PREPED = GLOBAL_PATH +'/int_dir/' # Output path
OUTPUT_PATH_SCORING = GLOBAL_PATH +'/int_dir/' # Output path
OUTPUT_PATH_PICKLES = GLOBAL_PATH +'/int_dir/' # Output path
OUTPUT_PATH_SCORD = GLOBAL_PATH +'/int_dir/' # Output path

#Defining filter to be used in the data
# Filters to be used on base data
ALL_FILTERS = {}

TODAY_DATE =  RUN_DATE.replace('-','_')
# SCORING_DATE_END = str(datetime.datetime.strptime(RUN_DATE, '%Y-%m-%d').date()-datetime.timedelta(days=2))
# SCORING_DATE_START =  str(datetime.datetime.strptime(SCORING_DATE_END, '%Y-%m-%d').date() - relativedelta(months=3))
# REFERENCE_DATE = str(datetime.datetime.strptime(SCORING_DATE_START, '%Y-%m-%d').date() - relativedelta(days=1))
SCORING_DATE_END = datetime.datetime.strptime(RUN_DATE, '%Y-%m-%d').year
SCORING_DATE_START = datetime.datetime.strptime(RUN_DATE, '%Y-%m-%d').year
REFERENCE_DATE = int(SCORING_DATE_START)-1

CPUS = multiprocessing.cpu_count() # Number of cores on which multi proc to be run, currently set to total CPU count

# For XGBoost Modelling
N_FOLDS = 5 # Number of folds data to be taken for cross validation
OBJECTIVE = 'binary:logistic' # can be picked from sklearn list of objective functions
SEEDER = 1 # SEED to split the dataframe into test and train
NCORES = round(multiprocessing.cpu_count()*0.8)  # Number of cores
AUC_POS = 0 # Position in list (return from multiproc function for XGB) for AUC value
MODEL_POS = 1 # Position in list (return from multiproc function for XGB) for Model 
PARAM_ST = 2 # Position in list (return from multiproc function for XGB) for start of the parameters
PARAM_ED = 5 # Position in list (return from multiproc function for XGB) for end of the parameters
HYPERPARAMETER_OBJECTIVE_SELECTION = "AUC" # Choose between AUC, PRECISION, ACCURACY,FSCORE AND RECALL (Don't change the style)

# Hyper parameters to tune
# DEPTH = [6, 8]
# ESTIMATORS = [50,100]
# L_RATE = [0.01, 0.05, 0.1,0.3]
DEPTH = [6]
ESTIMATORS = [50]
L_RATE = [0.01]



# Global variables to set the connections - To do, change to upper cases
dbname = 'bid_pricing'
user = 'admin'
host = 'localhost'
port = '5432'
password = 'password'

# Declare and initialize variables - To do, change to upper cases
schemas = {'input_schema':"raw_data",
           'output_schema': 'processed_data', 
           'intermediate_schema':'intermediate_data',
           'scoring_schema':'scoring_data',
           'static_input_schema':'static_input'}
    
all_tables = {
    'target':"bid_target_" + TODAY_DATE,
    'scoring':"bid_scoring_" + TODAY_DATE,
    'all_bid_table':"all_bid_table_2020_01_15",
    'all_bid_table_intrmdt':"all_bid_table_intrmdt_" + TODAY_DATE,
    'features_table':"bid_features_table_2020_01_20",
    'cluster_table':"static_cluster_mapping"}


SELECTED_SCHEMA_TRAINING = 'intermediate_schema'
SELECTED_SCHEMA_SCORING = 'scoring_schema'
SELECTED_TABLE_TRAINING = 'target'
SELECTED_TABLE_SCORING = 'scoring'

MODEL_FILE_NAME = 'bid_model_pickle_'+TODAY_DATE+".pkl"
SCORED_FILE_NAME = 'ai.'+datetime.datetime.now().strftime("%Y-%m-%d-%H.%M.%S")+'.bid_scored_sheet_'+TODAY_DATE+".csv"
