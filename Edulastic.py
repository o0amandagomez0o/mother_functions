'''
*------------------*
|                  |
|     IMPORTS      |
|                  |
*------------------*
'''
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

import time as t
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
import smtplib

import pandas as pd
import pyodbc
import re


'''
*------------------*
|                  |
|     VARIABLES    |
|                  |
*------------------*
'''
username = os.getenv('SR_USERNAME')
pswd = os.getenv('SR_PSWD')

user =  os.getenv('EK12_USERNAME')
passw = os.getenv('EK12_PASSWORD')
host =  os.getenv('EK12_HOST')
port =  os.getenv('EK12_PORT')

DATABASE_NAME = os.getenv('EK12_DATABASE')

source = '/Users/***/Downloads/Performance by Students.csv'
destination = '/Users/***/Edulastic/Common Assessments/'

#SchoolYear Index: [0]=School Year, [1]=2022-23, [2]=2021-2022, [3]=2020-21, [4]=2019-20......[10]=2013-14
SYindex = 1



RECIPIENTS = [
                {
                 'address' : 'name@email.org', 
                 'subject' : 'Edulastic Error',
                 'confirm' : 'Edulastic Confirmation',
                 'message' : 'Something went wrong'
                 }
]

dateran = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
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

    
    
def start_connection_with_EK12(user,passw,host,port):
    
    # Connect to the EK12 Database
    engine = db.create_engine('mssql+pyodbc://{}:{}@{}:{}/{}?driver=ODBC+Driver+17+for+SQL+Server'.format(user, passw, host, port, DATABASE_NAME))
    connection = engine.connect()
    metadata = db.MetaData()
    Session = sessionmaker(bind=engine)
    session = Session()
    return connection, engine, metadata, session     

    
def edulastic_login(driver, username, pswd):
    '''
    This function will: 
        - log into Edulastic
    '''
    # Establish Chrome as the browser to use
    #driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    
    #Opens website
    driver.get("https://app.edulastic.com/login")

    creds = WebDriverWait(driver,20).until(EC.presence_of_element_located((By.ID, "email")))

    #Locates username input & types in
    creds.send_keys(username)

    #Locates pswd input, types in & hits return to login
    driver.find_element(By.ID, "password").send_keys(pswd)
    driver.find_element(By.ID, "password").send_keys(Keys.RETURN)

    #driver.maximize_window()
    driver.set_window_size(1400, 1000)
    
    #wait
    t.sleep(4)
    
      
    
def nav_to_assessments(driver, username, pswd):
    '''
    '''
    #Selects Insights
    LNav = WebDriverWait(driver,20).until(EC.presence_of_element_located((By.XPATH, "//li[2]")))
    LNav.click()
    
    #Selects Performance by Students
    PSicon = WebDriverWait(driver,20).until(EC.presence_of_element_located((By.XPATH, "//a[6]")))
    PSicon.click()

    
    
def download_csv(driver, SYindex, username, pswd):
    '''
    This function will: 
        - log into Edulastic
        - download a CSV
        - rename and move the file to the correct space 
    ''' 
    print("Logging On To Edulastic...................................")
    edulastic_login(driver, username, pswd)

    print("Navigating to Assessments Page...................................")
    nav_to_assessments(driver, username, pswd)
    
    print('Downloading CSVs...................................')
    #wait
    t.sleep(4)
    
    #Clicks to reveal School Year dd
    CAfilter = WebDriverWait(driver,20).until(EC.presence_of_element_located((By.XPATH, "//span[@class ='ant-select-selection__choice__remove']")))
    CAfilter.click()
    
    #Clicks to reveal School Year dd
    syarrow = WebDriverWait(driver,20).until(EC.presence_of_element_located((By.XPATH, "//button[@class='ant-btn ant-dropdown-trigger']")))
    syarrow.click()

    #Creates a list of all elements under SCHOOL YEAR dd
    SYdd = WebDriverWait(driver,20).until(EC.presence_of_element_located((By.XPATH, "//*[@class='ant-dropdown ant-dropdown-placement-bottomLeft']/ul")))
    SYddlist = SYdd.find_elements(By.TAG_NAME, "li")

    #Saves most recent SY for naming format
    #currently saving [2] for 2021-2022; MUST CHANGE TO [1] ONCE LIVE
    SYselection = SYddlist[SYindex].text
    #Selects first non-selected opt from SYdd
    SYddlist[SYindex].click()

    #Clicks to reveal TEST dd
    TESTdd = WebDriverWait(driver,20).until(EC.presence_of_element_located((By.XPATH, "//span[@class='ant-select-search__field ant-input-affix-wrapper']")))
    TESTdd.click() 
    #wait
    t.sleep(3)

    Testddlist=driver.find_elements(By.XPATH, "//li[@role='option']")
    TestListLen = len(Testddlist)

    #close filters
    filter = WebDriverWait(driver,20).until(EC.presence_of_element_located((By.XPATH,"//button[@data-cy='filters']")))
    filter.click()

    for i in range(TestListLen):
        print(f"___________________Test {i+1} of {TestListLen}___________________")
        #wait
        t.sleep(5)

        #opens filters
        driver.find_element(By.XPATH,"//button[@data-cy='filters']").click()
        t.sleep(5)

        #Clicks to reveal TEST dd
        driver.find_element(By.XPATH, "//input[@class='ant-input']").click()
        t.sleep(10)

            #slice is the index we are looping through
            #locates the test dropdown
        slice = f'//li[@role=\'option\'][{i+1}]'
        TEST = driver.find_element(By.XPATH, slice)
            #Saves the Test Name plus ID number in cases of dup TestNames
        TestSelection = f'{TEST.text[:-11]}-ID-{TEST.text[-6:-1]}'
        t.sleep(3)
            #Selects the test
        TEST.click()
        t.sleep(3)

        #Click [Apply]
        ApplyFilter = driver.find_element(By.XPATH, "//button[@data-cy='applyFilter']")
        # WebDriverWait(driver,20).until(EC.presence_of_element_located((By.XPATH, "//button[@data-cy='applyFilter']")))    
        ApplyFilter.click()
        t.sleep(3)

        #Checks if there is data to download       
        try:
            driver.find_element(By.XPATH, "//h3[@class='styled__StyledH3-sc-uraf6j-11 hEYlMq']")
            t.sleep(3)
            #downloads the CSV
            DownloadCSV = driver.find_element(By.XPATH, "//button[@data-cy='download-csv']")
            # WebDriverWait(driver,20).until(EC.presence_of_element_located((By.XPATH, "//button[@data-cy='download-csv']")))
            DownloadCSV.click()       
            t.sleep(15)

            #checks if file is already in destination folder    
            if os.path.exists(destination+"/Performance by Students.csv") == FileNotFoundError:
                print(f"Performance by Students.csv was not found in {destination}")


            if os.path.exists(destination+"/Performance by Students.csv"):
                print("ERROR: File already exists")
                send_email(error=error_message)

            #if not, moves it    
            else:
                shutil.move(source, destination)
                print(source + " was moved")


            date= datetime.now().strftime("%m-%d-%Y")

            #Loops through all files in `destination`-including folders
            CommonAssessments = Path("/Users/Eagle/EmpowerK12/Eagle-EK12 - General/Edulastic/Common Assessments/")
            for f in CommonAssessments.iterdir():
                directory = f.parent
                extension = f.suffix            

                #checks directory is a file-vs folder
                if f.is_file():           
#                    if extension == ".csv": 
#                        #Archives previous downloads
#                        if f.name.endswith("_"+(datetime.now() - timedelta(days=1)).strftime("%m-%d-%Y")+".csv"):
#                            archivedest = '/Users/Eagle/EmpowerK12/Eagle-EK12 - General/Edulastic/Common Assessments/ArchivedCommonAssessments/'
#                            shutil.move(destination + f.name, archivedest + f.name)
#                            print(f"Archived {f.name}")

                    #Renames new download
                    if f.name == "Performance by Students.csv":
                        print(f)
                        print(CommonAssessments)
                        #renames and replaces original assessment file
                        TestSelection = re.sub("[\\/:*?<>|\"]", "-", TestSelection)
                        new_name= f"SY{SYselection}_{TestSelection}_{date}{extension}"

                        os.rename(f, f"{CommonAssessments}\{new_name}")
                        print(f'{new_name} Downloaded')
                        t.sleep(15)

                else: 
                    print(f"{f.name}: Path to a file does not exist.")
                    continue

        except: 
            NoData = driver.find_element(By.XPATH, "//div[@class='styled__NoDataContainer-sc-uraf6j-29 idmswl']").text
            print(f'{NoData}: {TestSelection} has no download available')
            #opens filters
            driver.find_element(By.XPATH,"//button[@data-cy='filters']").click()    
            continue
            t.sleep(3)
            
    #Exits out of webpage
    driver.close()
    driver.quit()
            
            
def archive_files(destination):
    '''
    This function will:
    - loop through the Common Assessments folder
    - only loop through files, not folders
    - ID if CSV file
        - ID date at end of filename
        - if file's date is before today's date:
            - if so, then Archive the file
                ----Excessively long filenames will be shortened before archiving to avoid errors. 
                        Usually Windows can handle up to 260 characters, but is erroring at over 150
    '''
    #Loops through all files in `destination`-including folders
    CommonAssessments = Path("/Users/***/Edulastic/Common Assessments/")
    os.chdir(CommonAssessments)
    for f in CommonAssessments.iterdir():
        #splitting pathname
        directory = f.parent
        extension = f.suffix
        
        #checks for files vs. folders/dirs
        if f.is_file():
            #ID CSVs
            if extension == ".csv":
                #ID date and convert str to datetime for comparison
                date_str = re.search(r'\d{2}-\d{2}-\d{4}',f.name)
                file_date = datetime.strptime(date_str.group(), "%m-%d-%Y").date()
                print(f"Date of {f.name.split('_')[2]}: {file_date}")
                
                #ID files to be archived
                if file_date < datetime.now().date():
                    #where I want to send archived files
                    archivedest = '/Users/***/Edulastic/Common Assessments/ArchivedCommonAssessments/'
                    #Need to shorten excessively long filenames
                    if len(f.name.split('.')[0]) > 150:
                        renamef = f"{f.name.split('_')[0]}_{f.name.split('_')[1]}_ID-{f.name.split('-ID-')[-1]}"
                        os.rename(f.name, renamef)
                        print(f"****************{f.name} → {renamef}")
                        #move file
                        shutil.move(f'{destination}{renamef}', f'{archivedest}')
                        print(f"Archived {renamef}") 
                        continue
                    else:
                        #move file
                        shutil.move(destination + f.name, archivedest + f.name)
                        print(f"Archived {f.name}")
                 
    

def get_loading_times(DATABASE_NAME, tablename, api, dt, ds, de):
    '''
    This function takes in the DB Name, tablename (format: sch.TableName), dt:What data is being loaded, ds: LoadingStartTime, de:LoadingEndTime,
    calculates the length of time this section took
    and uploads this to SSMS tablename provided in input
    '''

    query = f'''
                INSERT INTO {tablename} (LEA, API, DataTable, DateStart, DateEnd, LenTimeHrs)
                VALUES ('{DATABASE_NAME}', '{api}', '{dt}', '{ds}', '{de}', (cast(DATEDIFF(mi, '{ds}', '{de}') as float)/60))
                '''
    CONNECTION.execute(query)

                       


                    
                           

# Main Program ------------------------------------------------------------------------------------------
start = t.time()
s_Main = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

CONNECTION, ENGINE, METADATA, SESSION = start_connection_with_EK12(user,passw,host,port)

try:
    ERROR_LOG_FILENAME = "ErrorLog {}.txt".format(str(datetime.now()).strip().replace(':', '_'))
    print('Beginning Program')
    download_csv(webdriver.Chrome(service=Service(ChromeDriverManager().install())), SYindex, username, pswd)           
except Exception as error_message:
    print(f'Error Downloading CSVs with Selenium:{error_message}')     
    send_email(error=error_message)
    
try:
    print('Achiving Deprecated Files')
    archive_files(destination)
        
except Exception as error_message:
    print(f'Archiving Files:{error_message}')     
    send_email(error=error_message)

end = t.time()
e_Main = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
print(end - start)
get_loading_times(DATABASE_NAME, 'sch.TableName', 'Edulastic', 'Full Py Script', s_Main, e_Main)

try:
    ERROR_LOG_FILENAME = "ErrorLog {}.txt".format(str(datetime.now()).strip().replace(':', '_'))
    CONNECTION, ENGINE, METADATA, SESSION = start_connection_with_EK12(user,passw,host,port)
    print('Connecting to SSMS')  
   
except Exception as error_message:
    send_email(error=error_message, table = 'Connecting to SSMS')

if os.path.exists(ERROR_LOG_FILENAME):
    with open(ERROR_LOG_FILENAME,'r') as f:
        request_errors = f.readlines()
    f.close()
    request_errors = ''.join(request_errors)
    send_email(logfile=request_errors)
    errorquery = f'''
                UPDATE sch.TableName
                SET LEA = '***'
                 , Status = '{request_errors}'
                 , [Date] = '{dateran}'
                 , DailyFlag = '1'
                WHERE [Type] = 'Edulastic API'
                '''
    CONNECTION.execute(errorquery)
    
else:
    query = f'''
                UPDATE sch.TableName
                SET LEA = '***'
                 , Status = 'Success'
                 , [Date] =  '{dateran}'
                 , DailyFlag = '1'
                WHERE [Type] = 'Edulastic API'
                '''
    CONNECTION.execute(query)