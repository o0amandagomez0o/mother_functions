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

def get_data(sql_query, db):
    '''
    sql_query should be in triple quotation """ """
    This function reads in the data from the Codeup db
    and returns a pandas DataFrame with all columns.
    '''
    
    return pd.read_sql(sql_query, get_connection(db))
    
    
    
    
    
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

def drop_based_on_pct(df, pc, pr):
    """
    drop_based_on_pct takes in: 
    - dataframe, 
    - threshold percent of non-null values for columns(# between 0-1), 
    - threshold percent of non-null values for rows(# between 0-1)
    
    Returns: a dataframe with the columns and rows dropped as indicated.
    """
    
    tpc = 1-pc
    tpr = 1-pr
    
    df.dropna(axis = 1, thresh = tpc * len(df.index), inplace = True)
    
    df.dropna(axis = 0, thresh = tpr * len(df.columns), inplace = True)
    
    return df





def remove_columns(df, cols_to_remove):  
    """
    This function removes columns listed in arguement
    - cols_to_remove = ["col1", "col2", "col3", ...]
    returns DF w/o the columns.
    """
    df = df.drop(columns=cols_to_remove)
    return df





def handle_missing_values(df, prop_required_column = .5, prop_required_row = .70):
	#function that will drop rows or columns based on the percent of values that are missing:\

    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df





def outlier(df, feature, m):
    '''
    outlier will take in a dataframe's feature:
    - calculate it's 1st & 3rd quartiles,
    - use their difference to calculate the IQR
    - then apply to calculate upper and lower bounds
    - using the `m` multiplier
    '''
    q1 = df[feature].quantile(.25)
    q3 = df[feature].quantile(.75)
    
    iqr = q3 - q1
    
    multiplier = m
    upper_bound = q3 + (multiplier * iqr)
    lower_bound = q1 - (multiplier * iqr)
    
    return upper_bound, lower_bound





def get_object_cols(df):
    '''
    This function takes in a dataframe and identifies the columns that are object types
    and returns a list of those column names. 
    '''
    # create a mask of columns whether they are object type or not
    mask = np.array((df.dtypes == "object") | (df.dtypes == "category"))

        
    # get a list of the column names that are objects (from the mask)
    object_cols = df.iloc[:, mask].columns.tolist()
    
    return object_cols





def get_numeric_X_cols(X_train, object_cols):
    '''
    takes in a dataframe and list of object column names
    and returns a list of all other columns names, the non-objects. 
    '''
    numeric_cols = [col for col in X_train.columns.values if col not in object_cols]
    
    return numeric_cols





'''
*------------------*
|                  |
|     SCALING      |
|                  |
*------------------*
'''

def min_max_scaler(train, valid, test):
    '''
    Uses the train & test datasets created by the split function
    Returns 3 items: mm_scaler, train_scaled_mm, test_scaled_mm
    This is a linear transformation. Values will lie between 0 and 1
    '''
    num_vars = list(train.select_dtypes('number').columns)
    scaler = MinMaxScaler(copy=True, feature_range=(0,1))
    train[num_vars] = scaler.fit_transform(train[num_vars])
    valid[num_vars] = scaler.transform(valid[num_vars])
    test[num_vars] = scaler.transform(test[num_vars])
    return scaler, train, valid, test





'''
*------------------*
|                  |
|    SPLITTING     |
|                  |
*------------------*
'''





'''
*------------------*
|                  |
|     MODELING     |
|                  |
*------------------*
'''

def get_metrics(mod, X, y):
    """
    This function takes in:
    - mod: model being used
    - X: X_train/validate/test: split features
    - y: target
    
    """
    baseline_accuracy = (train.churn == 0).mean()
    y_pred = mod.predict(X)
    accuracy = mod.score(X, y)
    conf = confusion_matrix(y, y_pred)
    prfs = pd.DataFrame(precision_recall_fscore_support(y, y_pred), index=['precision', 'recall', 'f1-score', 'support'])
    
    print(f'''
    BASELINE accuracy is: {baseline_accuracy:.2%}
    The accuracy for our model is: {accuracy:.2%} 
    ''')
    return prfs