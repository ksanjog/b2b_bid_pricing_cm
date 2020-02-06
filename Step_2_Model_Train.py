
#coding: utf-8


#Import configuration and common libraries
from int_dir.config_file import *
from helper_modules.ai_db_connect import *
from sklearn.model_selection import train_test_split
import multiprocessing as mp # Multiprocessing
import xgboost
import sklearn
from sklearn.externals import joblib
from sklearn import linear_model
from sklearn.ensemble import RandomForestRegressor

# For model evaluation metrics
from scipy import stats
from datetime import datetime
import pickle # For saving model
import multiprocessing as mp # Multiprocessing



def clean_model_data(DB_READ,schemas,all_tables):
    
    """
    The function loads and cleans up the model data. Also creates dummies for variables
    
    """
    
    if DB_READ:
        engine = conn_eng()
        qry1 = 'select * from '+ schemas['output_schema']+'.'+all_tables['features_table']
        df = pd.read_sql(qry1,engine)
        engine.dispose()
        del engine

    else:
        df = pd.read_csv(OUTPUT_PATH_PREPED + all_tables['output_table'] + '.csv')
        
    df = df.replace([np.inf, -np.inf], np.nan)
    df.fillna(df.mean(), inplace=True)

    # #Create dummies for the data 
    NON_FEATURE_COLUMNS = {'target','key','date'}

    cols = [i for i in df.columns if i not in NON_FEATURE_COLUMNS]

    dfd = pd.get_dummies(df[cols])
        
    dfd = pd.concat([df[['target']],dfd], axis = 1)
    dfd = pd.concat([df[['key']],dfd], axis = 1)
    dfd = pd.concat([df[['date']],dfd], axis = 1)
    
    print("Model data cleaned")
    
    return dfd

def split_train_test(input_data,SEEDER,test_start_dt,test_end_dt):
    """
    Splits the data into test and train with a SEEDER
    
    Paramters
    ---------
    input_data : Dataframe 
        Dataframe with model data (with dummies)
    SEEDER : Float
        SEED to split the dataframe into test and train
        
    Output
    -------
    x_train,y_train,x_test,y_test : Dataframe
        Dataframes with corresponding variables for training and testing
    """
    
    # Change datatype of target
    input_data["target"] = input_data["target"].astype(object)

    # Split dataset giving the training dataset 75% of the data
#     train,test= train_test_split(input_data, test_size=0.25,random_state=SEEDER)
    
    train = input_data.loc[(input_data['date'] < test_start_dt)]
    test = input_data.loc[(input_data['date'] >= test_start_dt) & (input_data['date'] < test_end_dt)]
    
    
    train_key = train[['key','date']]
    test_key = test[['key','date']]
    
    train = train.drop(['key','date'],axis=1)
    test = test.drop(['key','date'],axis=1)
        
    cols = [i for i in train.columns if i not in 'target']
    
    x_train = train[cols]
    y_train = train[['target']]
    
    x_test = test[cols]
    y_test = test[['target']]
    
    print("Train-Test split done")
    
    
    return (x_train,y_train,x_test,y_test, train_key, test_key)
    

def search_space(x_train,y_train,x_test,y_test):
    """
    Define search space for hyperparameters according to the model specified.
    
    Parameters
    ----------
    MODEL : string
        Model to run as specified by user, cab be RF, XGB or OLSREG
    x_train, y_train, x_test, y_test : DataFrame
        Relevant datasets
    
    Returns
    -------
    args : tuple
        Tuple that stores the hyperparameters relevant to the model,
        as well as x_train, y_train, x_test, y_test datasets
    """
    
    args = []
    
    # XGBoost search space (already defined in config file as Global variables)

    for d in DEPTH:
        for e in ESTIMATORS:
            for l in L_RATE:
                args.append((d,e,l,x_train,y_train,x_test,y_test))
    
    print("Search space creation done")
    
#     print(args)
    return args

def linear_model(x_train,y_train,x_test,y_test):
    """
    Configuration function to run XGBoost Regression model.
    
    Parameters
    ----------
    args : tuple
        Tuple that stores the hyperparameters relevant to the model,
        as well as x_train, y_train, x_test, y_test datasets
    
    Returns
    -------
    tuple, with R2 on testing set, model object, and relevant hyperparameters used
    """
    
    # Only to put placeholder values for now
    x_train.fillna(x_train.mean(), inplace=True)
    x_test.fillna(x_test.mean(), inplace=True)
    
    
    reg = linear_model.LinearRegression()
    reg.fit(x_train, y_train)
    
    # Regression coefficients 
    Model_metrics = reg.coef_
    print('Coefficients: \n', reg.coef_)
    
    # variance score:
    print('Variance score: {}',format(reg.score(x_test, y_test)))

    return (Model_metrics, reg)

def randomforest_model(x_train,y_train,x_test,y_test,n_estimators):
    
    # Only to put placeholder values for now
    x_train[x_train==np.inf]=np.nan
    x_test[x_test==np.inf]=np.nan
    
    x_train.fillna(x_train.mean(), inplace=True)
    x_test.fillna(x_test.mean(), inplace=True)
#     y_train.fillna(y_train.mean(), inplace=True)
#     y_test.fillna(y_test.mean(), inplace=True)
    
    rf = RandomForestRegressor(n_estimators = n_estimators, random_state = 42, n_jobs=-1, max_features = 'sqrt')
    rf.fit(x_train, y_train)
    
    predictions = rf.predict(x_test)
    errors = abs(predictions - y_test['target'])
    error_per = (100*errors/y_test['target'])
    
    # Print out the mean absolute error (mae)
    print('Mean Absolute Error:', round(np.mean(errors), 2), 'degrees.')
    print('Mean Absolute Squared Error:', round(np.mean(errors**2), 2), 'degrees.')
    print('Mean Absolute % Error:', round(np.mean(error_per), 2), 'per.')
    print('Accuracy:', round(100 - np.mean(error_per), 2), 'per.')
    
    # Get numerical feature importances
    importances = list(rf.feature_importances_)
    
    # List of tuples with variable and importance
    feature_list = list(x_train.columns)
    feature_importances = [(feature, round(importance, 2)) for feature, importance in zip(feature_list, importances)]
    
    # Sort the feature importances by most important first
    feature_importances = sorted(feature_importances, key = lambda x: x[1], reverse = True)
    
    # Print out the feature and importances 
    #[print('Variable: {:20} Importance: {}'.format(*pair)) for pair in feature_importances]
    
    
#     predictions_df = pd.DataFrame({'Predicted_price':predictions})
#     predictions_df = pd.concat([x_test,y_test,predictions_df])
   
    return (rf,feature_importances)
    

    
# XGBoost Regressor Model
def xgboost_model(x_train,y_train,x_test,y_test,n_estimators):
    # Only to put placeholder values for now
    

    for col in x_train.select_dtypes(include=['uint8']).columns:
        x_train[col] = x_train[col].astype(int)
    for col in x_test.select_dtypes(include=['uint8']).columns:
        x_test[col] = x_test[col].astype(int)
    
    y_train = y_train.astype(float)
    y_test = y_test.astype(float)

    
    x_train[x_train==np.inf]=np.nan
    x_test[x_test==np.inf]=np.nan
    
    x_train.fillna(x_train.mean(), inplace=True)
    x_test.fillna(x_test.mean(), inplace=True)

#     print(x_train.dtypes)
#     print(y_train.dtypes)
    
    xgb = xgboost.XGBRegressor(n_estimators=n_estimators, learning_rate=0.1, loss = 'ls')
    xgb.fit(x_train, y_train)
    
    predictions = xgb.predict(x_test)
    errors = abs(predictions - y_test['target'])
    error_per = (100*errors/y_test['target'])
    
    # Print out the mean absolute error (mae)
    print('Mean Absolute Error:', round(np.mean(errors), 2), 'degrees.')
    print('Mean Absolute Squared Error:', round(np.mean(errors**2), 2), 'degrees.')
    print('Mean Absolute % Error:', round(np.mean(error_per), 2), 'per.')
    print('Accuracy:', round(100 - np.mean(error_per), 2), 'per.')
    
    # Get numerical feature importances
    importances = list(xgb.feature_importances_)
    
    # List of tuples with variable and importance
    feature_list = list(x_train.columns)
    feature_importances = [(feature, round(importance, 2)) for feature, importance in zip(feature_list, importances)]
    
    # Sort the feature importances by most important first
    feature_importances = sorted(feature_importances, key = lambda x: x[1], reverse = True)
    
    return (xgb,feature_importances)

def pred_ints(model, x_test):
    vals = []
    num_features = len(x_test.columns)
    
    for x in range(len(x_test)):
        preds = []
        for pred in model.estimators_:
            preds.append(pred.predict(np.reshape(x_test.iloc[x,:].array,(1,num_features)))[0])  # 72 is # of features (change if needed)              
        vals.extend(preds)
    return vals
   


def save_model(model, OUTPUT_PATH_PICKLES,MODEL_FILE_NAME):  
    """Save passed model as pickle object in the specified file path"""
    
    #Pickle the model 
    joblib.dump(model, (OUTPUT_PATH_PICKLES+MODEL_FILE_NAME))
    
    print("Model object and results saved in directory")


def run_model():
    
    """
    This function runs the hyper parameter tuned model end to end using multiprocessing
    """
    
    # Load data
    df = clean_model_data(DB_READ,schemas,all_tables)
    
    split_date = datetime.date(datetime(2019,4,1))
    
    n_estimators = 5000
    all_predictions_df = pd.DataFrame()
    fi = pd.DataFrame()
    
    while split_date < datetime.date(datetime(2019,11,2)):
        
        test_start_dt = split_date
        test_end_dt = split_date + relativedelta(months=1)
        
        (x_train,y_train,x_test,y_test,train_key,test_key) = split_train_test(df,SEEDER,test_start_dt,test_end_dt)
        
        print('Running model now')
        start = time.time()
#         model,feature_importances = randomforest_model(x_train,y_train,x_test,y_test,n_estimators)
        model,feature_importances = xgboost_model(x_train,y_train,x_test,y_test,n_estimators)
        
        fi = fi.append(pd.DataFrame(feature_importances), ignore_index = True)
        end = time.time()
        print('Model took '+ str(end-start))
        
        print('Predicting on model now for ' + str(test_end_dt))
        start = time.time()
        predictions = pd.DataFrame({'Predicted_price': model.predict(x_test)})
        end = time.time()
        print('Predictions took '+ str(end-start))
        print('###########################################################################')

#         op = pred_ints(model, x_test)
#         op = pd.DataFrame(np.reshape(np.array(op),(len(x_test.index),n_estimators)))
#         prob = pd.concat([test_key.reset_index(drop=True),
#                           x_test[['total_qty_ty']].reset_index(drop=True),
#                           y_test.reset_index(drop=True),
#                           predictions.reset_index(drop=True),
#                           op.reset_index(drop=True)], axis=1)
        prob = pd.concat([test_key.reset_index(drop=True),
                          x_test[['total_qty_ty']].reset_index(drop=True),
                          y_test.reset_index(drop=True),
                          predictions.reset_index(drop=True)], axis=1)
        
        
        all_predictions_df = all_predictions_df.append(prob, ignore_index = True)
        
        all_predictions_df.to_csv("/home/ncod/dashboard_input_data/raw_files/Predictions_20200206.csv",index=False)
        fi.to_csv("/home/ncod/dashboard_input_data/raw_files/FeatureImportance_20200206.csv",index=False)
        
        split_date = test_end_dt
        
#     predictions_df.to_sql('bid_scoring','scoring_data',con=engine,index=False,if_exists ='replace')
#     all_predictions_df.to_csv("/home/ncod/dashboard_input_data/raw_files/Predictions_20200206.csv",index=False)

    return(model,all_predictions_df)

