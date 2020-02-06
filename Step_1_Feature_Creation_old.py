

#Import statements
from int_dir.config_file import *
from helper_modules.ai_db_connect import *
from core_modules.create_target_scoring_tables import * 
import core_modules.create_intermediate_tables as dm_table_creation
from core_modules.create_features import *
import time

#Create the intermediate tables if necessary 
#def execute_table_creation(args):
#    engine = conn_eng()
#    (function,schemas,all_tables,SELECTED_SCHEMA,SELECTED_TABLE) = args
#    function(schemas, all_tables, engine,SELECTED_SCHEMA,SELECTED_TABLE)
#    engine.dispose()
#    del engine
#    return
#
#all_elements = inspect.getmembers(dm_table_creation, inspect.isfunction)
#
#function_list = [x[1] for x in all_elements]
#
#args_target = [(x,schemas,all_tables,SELECTED_SCHEMA_TRAINING,SELECTED_TABLE_TRAINING) for x in function_list]
#
#args_scoring = [(x,schemas,all_tables,SELECTED_SCHEMA_SCORING,SELECTED_TABLE_SCORING) for x in function_list]

##############################################################################################################################
# Create the target data file
if TARGET_DB_WRITE:
    engine = conn_eng()
    table_creator(TARGET_DB_WRITE, engine, schemas, all_tables, REFERENCE_DATE,SCORING_DATE_START, SCORING_DATE_END, 'TARGET')
    engine.dispose()
    del engine

#if INTERMEDIATE_TABLE_WRITE:
#    start = time.time()
#    for arg in args_target:
#        execute_table_creation(arg)
#    end = time.time()
#    print(end-start)

#Create all features from the database (using multiprocessing)
start = time.time()
features_target = create_features(WRITE_DB,FP, all_tables, schemas, CPUS,SELECTED_SCHEMA_TRAINING, SELECTED_TABLE_TRAINING)
end = time.time()
print(end-start)

#if not WRITE_DB:
#    #Write the output to file
#    file_write(features_target,OUTPUT_PATH_PREPED,all_tables['output_table'])
#    print(features_target.shape)

###############################################################################################################################
# Create the scoring data output
if SCORING_DB_WRITE:
    engine = conn_eng()
    table_creator(SCORING_DB_WRITE, engine, schemas, all_tables, REFERENCE_DATE,SCORING_DATE_START, SCORING_DATE_END, 'SCORING')
    engine.dispose()
    del engine
    
#print(INTERMEDIATE_TABLE_WRITE)
#if INTERMEDIATE_TABLE_WRITE:
#    start = time.time()
#    print(INTERMEDIATE_TABLE_WRITE)
#    for arg in args_scoring:
#        execute_table_creation(arg)
#    end = time.time()
#    print(end-start)

#Create all features from the database (using multiprocessing)
start = time.time()
features_scoring = create_features(WRITE_DB,MISSING_VALUE_TREATMENT,OUTLIER_TREATMENT, FP, all_tables, schemas, CPUS, 
                              SELECTED_SCHEMA_SCORING, SELECTED_TABLE_SCORING)
end = time.time()
print(end-start)

#if not WRITE_DB:
#    #Write the output to file
#    file_write(features_scoring,OUTPUT_PATH_SCORING,all_tables['scoring_table'])
#    print(features_scoring.shape)



