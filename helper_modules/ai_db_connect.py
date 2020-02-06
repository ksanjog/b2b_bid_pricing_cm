from sqlalchemy import create_engine
import psycopg2
from int_dir.config_file import *

def conn_eng():
    engine = create_engine('postgresql+psycopg2://'+user+':'+password+'@'+host+':'+port+'/'+dbname)
    return engine
