import pandas as pd
import numpy as np


# used to bring in env.py
from env import host, user, password


'''
*------------------*
|                  |
|     ACQUIRE      |
|                  |
*------------------*
'''


def get_connection(db, user=user, host=host, password=password):
    '''
    This function uses my info from my env file to
    create a connection url to access the Codeup db.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

    
 
    
#____________________________________________________    
# fill in the blanks __ to have
# SQL query
# and __ db in "get_connection"
def get_data(sql_query, db):
    '''
    This function reads in the data from the Codeup db
    and returns a pandas DataFrame with all columns.
    '''
<<<<<<< HEAD
    sql_query = 'SELECT * FROM __'
    return pd.read_sql(sql_query, get_connection('__'))
       
=======

    return pd.read_sql(sql_query, get_connection(db))
    
    
    
    
>>>>>>> 10c170553991f113d83c2ceb9e89a456c45ae790
    
def cached_data(file, cached=False):
    '''
    This function reads in data from Codeup database 
    - writes data to a csv file 
    - if cached == False or if cached == True reads in file from
    a csv file, 
    
    returns df.
    '''
    if cached == False or os.path.isfile(file) == False:
        
        # Read fresh data from db into a DataFrame.
        df = get_data()
        
        # Write DataFrame to a csv file.
        df.to_csv('file')
        
    else:
        
        # If csv file exists or cached == True, read in data from csv.
        df = pd.read_csv(file, index_col=0)
        
    return df  
#_________________________________________________




'''
*------------------*
|                  |
|     PREPARE      |
|                  |
*------------------*
'''