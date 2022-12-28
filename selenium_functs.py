from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time as t
from selenium.webdriver.common.action_chains import ActionChains
import os
import zipfile as z
import shutil
from datetime import datetime
from pathlib import Path
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
import smtplib

import pandas as pd
import pyodbc


'''
*------------------*
|                  |
|     VARIABLES    |
|                  |
*------------------*
'''

#These are Environment Variables saved directly in VM System Properties

username = os.getenv('EdH_USERNAME')
pswd = os.getenv('EdH_PASSWORD')

user =  os.getenv('USERNAME')
passw = os.getenv('PASSWORD')
host =  os.getenv('HOST')
port =  os.getenv('PORT')

DATABASE_NAME = 'DatabaseName'

source = '/Users/***/Downloads/incidents.zip'
targetpath = '/Users/***/Downloads'
destination = '/Users/***/NewLocationDirectory'


RECIPIENTS = [
                {
                 'address' : 'name@email.org', 
                 'subject' : 'EdHandbook Error',
                 'confirm' : 'EdHandbook Confirmation',
                 'message' : 'Something went wrong'
                 }
#                ,    
#                {
#                'address' : 'name@email.org', 
#                'subject' : EdHandbook Error',
#                'confirm' : EdHandbook Confirmation',
#                'message' : 'Something went wrong'
#                }

             ]




'''
*------------------*
|                  |
|     FUNCTIONS    |
|                  |
*------------------*
'''
def send_email(error='', table='', logfile=''):

    dt = datetime.now()
    sender_email = os.getenv('EMAIL_ADDRESS')
    password = os.getenv('EMAIL_PASSWORD')
    smtpObj = smtplib.SMTP('smtp-mail.outlook.com', 587)
    smtpObj.starttls()
    smtpObj.login(sender_email, password)

    for person in RECIPIENTS:

        if table and error:
            msg = '''Subject: {}\n\n
                     {}\n One or both of these two errors may have occured.\n
                     There was an error loading this table: 
                     {}\n
                     Python returned this error message:\n
                     {}\n
                     The time of occurance was: {}'''.format(person['subject'], person['message'], table, error, dt)
        elif table:
            msg = '''Subject: {}\n\n
                     {}\n
                     There was an error loading this table: 
                     {}\n
                     The time of occurance was: {}'''.format(person['subject'], person['message'], table, dt)
        
        elif error:
            msg = '''Subject: {}\n\n
                     {}\n
                     Python returned this error message:\n
                     {}\n
                     The time of occurance was: {}'''.format(person['subject'], person['message'], error, dt)

        else:
            msg = '''Subject: {}\n\n
                     {}\n
                     The data for these request returned an error:\n
                     {}'''.format(person['subject'], person['message'], logfile)

        smtpObj.sendmail(sender_email, person['address'], msg)
    
    smtpObj.quit()

    

def send_confirmation_email():

    dt = datetime.now()
    sender_email = os.getenv('EMAIL_ADDRESS')
    password = os.getenv('EMAIL_PASSWORD')
    smtpObj = smtplib.SMTP('smtp-mail.outlook.com', 587)
    smtpObj.starttls()
    smtpObj.login(sender_email, password)

    for person in RECIPIENTS:

        msg = '''Subject: {}\n\n
        The python script ran'''.format(person['confirm'])

        smtpObj.sendmail(sender_email, person['address'], msg)

    smtpObj.quit()
  



def download_csv(username, pswd):
    '''
    this function will log into EducatorsHandbook and download a compressed zip file of incidents. Then closes the webpage.
    '''
    
    # identify location of chromedriver
    PATH ="C:\Program Files (x86)\chromedriver.exe"
    
    # Establish Chrome as the browser to use
    driver = webdriver.Chrome(PATH)
    
    #Opens website
    driver.get("https://incidents.educatorshandbook.com/")
    
    #Locates username input & types in
    
    driver.find_element_by_name("email").send_keys(username)
    #Locates pswd input, types in & hits return to login
    driver.find_element_by_name("pass").send_keys(pswd)
    driver.find_element_by_name("pass").send_keys(Keys.RETURN)
    
    #wait
    t.sleep(8)
    
    #downloads CSV zip file to downloads
    driver.find_element_by_class_name("download").click()
    #wait
    t.sleep(8)
    
    #Exits out of webpage
    driver.quit()
        
 

def unzip_file(source, destination):
    '''
    This function will:
    - move zip file out of downloads folder to Teams Discipline folder
    - extract only 'actions.csv' from zip 
    - delete remaining zip folder
    - check if prev version of actions.csv is in Discipline/Actions folder & removes if it is
    - rename and replace with new version of actions.csv
    - 
    '''
    #Moves zip file from downloads to ITDS Admin Teams folder
    try:
        if os.path.exists(destination+"/incidents.zip"):
            #shutil.move(source, destination)
            ##NEED TO CORRECT TO REPLACE EXISTING FILE
            print("ERROR: File already exists")
        else:
            shutil.move(source, destination)
            print(source + " was moved")
    
    except FileNotFoundError:
        print(source + " was not found")
        
    #Extract all files from zipfolder
    root = z.ZipFile(destination+"/incidents.zip")
    root.extract('actions.csv', destination)
    t.sleep(5)
    root.close()
    t.sleep(5)
    #delete zip file
    os.remove(destination+"/incidents.zip")
    
    date= datetime.now().strftime("%m-%d-%Y")
    dest = destination+"/actions"
    
    for f in Path(destination).iterdir():
        #checks if file not folder
        if f.is_file():
            for c in os.listdir(dest):
                #deletes prev version of actions file
                if c.startswith('ITDS_actions'):
                    os.remove(dest+"/"+c)
                    file = c
            
            if f.name == 'actions.csv':
                #renames and replaces old actions file
                full_dest = os.path.join(dest, file)
                file_name = os.path.splitext(os.path.split(f)[1])[0]
                file_ext = os.path.splitext(os.path.split(f)[1])[1]
                new_name= f"{file_name}_{date}{file_ext}"
                
                new_file_path = Path(dest).joinpath(new_name)
                
                f.rename(new_file_path)
                
                return new_file_path, file_name
 


def start_connection():
    user =  os.getenv('USERNAME')
    passw = os.getenv('PASSWORD')
    host =  os.getenv('HOST')
    port =  os.getenv('PORT')

    # Connect to the SQL Database
    engine = db.create_engine('mssql+pyodbc://{}:{}@{}:{}/{}?driver=ODBC+Driver+17+for+SQL+Server'.format(user, passw, host, port, DATABASE_NAME))
    connection = engine.connect()
    metadata = db.MetaData()
    Session = sessionmaker(bind=engine)
    session = Session()
    return connection, engine, metadata, session


    
def load_pd_df_to_sql(t, sch):
    '''
    This function will:
        - t: 'table_name' & sch: 'schema' of the SQL server table we wish to write into
        - connect to DB
        - insert
    '''

    # Variable
    date= datetime.now().strftime("%m-%d-%Y")

    #establish df and it's uniform datatype to match SQL Server
    # read CSV into pandas 
    df = pd.read_csv(new_file_path)
    # add fillename column
    df['filename'] = f'{file_name}_{date}'
    df = df.astype(str)
    
    #use Pandas to load to SQL while "truncating"
    df.to_sql(t, ENGINE, schema= sch, if_exists="replace", index=False) 
