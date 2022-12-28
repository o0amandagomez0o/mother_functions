import time as t
import requests
import pyodbc
import os
import sqlalchemy as db
import smtplib
import pandas as pd
import numpy as np
import itertools

from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta

'''
*------------------*
|                  |
|     VARIABLES    |
|                  |
*------------------*
'''

#These are Environment Variables saved directly in VM System Properties

STARTOFSY = '2022-07-01' ## Will need annual update

MAIN_URL = 'https://***.schoolrunner.org/api/v1'
username = os.getenv('SR_USERNAME')
pswd = os.getenv('SR_PASSWORD')

#auth = '(username, pswd)'

DATABASE_NAME =  os.getenv('EK12_USERNAME')
user =  os.getenv('EK12_USERNAME')
passw = os.getenv('EK12_PASSWORD')
host =  os.getenv('EK12_HOST')
port =  os.getenv('EK12_PORT')

RECIPIENTS = [
                {
                 'address' : 'name@email.org', 
                 'subject' : 'SchoolRunner Error',
                 'confirm' : 'SchoolRunner Confirmation',
                 'message' : 'Something went wrong'
                 }
            ]

dateran = (datetime.now() + timedelta(days=0)).strftime("%Y/%m/%d %H:%M:%S")
    
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

    

def get_all_results(url, username, pswd, params={}, extra=''):
    r = requests.get('{}{}{}'.format(MAIN_URL, url, extra), auth=(username, pswd), data=params)		
    if r.status_code == 200:
        values_list = r.json()
        while 'next' in r.links:
            r = requests.get(r.links['next']['url'], headers=headers, data=params)
            values_list.extend(r.json())
        return values_list
    elif r.status_code != 404:
        request_error = '{} - {}'.format(r.url, r.status_code)
        print(request_error)
        log_this(request_error)
        return None
    else:
        return None


    
def start_connection_with_EK12(user, passw, host, port ):

    # Connect to the EK12 Database
    engine = db.create_engine('mssql+pyodbc://{}:{}@{}:{}/{}?driver=ODBC+Driver+17+for+SQL+Server'.format(user, passw, host, port, DATABASE_NAME))
    connection = engine.connect()
    metadata = db.MetaData()
    Session = sessionmaker(bind=engine)
    session = Session()
    return connection, engine, metadata, session



def declare_database_table(schema, tablename):
    t = db.Table(tablename, METADATA, autoload=True, autoload_with=ENGINE, schema=schema)
    return t



def delete_this_table_data(tablename):
    CONNECTION.execution_options(autocommit=True).execute("""TRUNCATE TABLE {}""".format(tablename))

        

def insert_data_into_database(tablename, data, t):
    CONNECTION.execute(t.insert(), data)

    
    
def get_terms(STARTOFSY, username, pswd):
    ''' 
    This function will: 
    - take in the current start of SY and SchoolRunner credentials 
    - input all terms for the HS/MS for the current SY into staging.SchoolRunner_Terms in SSMS
    - output a list of termids for reference
    '''
    url = f'/term-bins?active=1&school_ids=2,3&min_date={STARTOFSY}'
    response = get_all_results(url, username, pswd)
    terms = []
    termids = []
    
    if response:
        delete_this_table_data('staging.SchoolRunner_Terms')
        t = declare_database_table('staging','SchoolRunner_Terms')
        
        for item in response['results']['term_bins']:
            terms ={ 'term_bin_id'  : item['term_bin_id']
                    ,'term_id' : item['term_id']
                    ,'term_bin_type_id' : item['term_bin_type_id']
                    ,'school_id' : item['school_id']
                    ,'short_name' : item['short_name']
                    ,'long_name' : item['long_name']
                    ,'start_date' : item['start_date']
                    ,'end_date' : item['end_date']
                    ,'for_grade' : item['for_grade']   
                    ,'external_id' : item ['external_id']
                    ,'is_locked' : item ['is_locked'] 
                    ,'from_date': item['from_date']
                    ,'thru_date': item['thru_date']
                    ,'active' : item['active']          
                    }
            
            termids.append(item['term_bin_id'])
            
            insert_data_into_database('staging.SchoolRunner_Terms', terms, t)
            
        return termids

    
    
def get_active_courses(STARTOFSY,username, pswd):
    '''
    This function will:
    - take in SchoolRunner credentials
    - output a list of all currently active courses
    '''
    termids = (STARTOFSY, username, pswd)
    active_courses = []

    for termid in termids:
        url = f'/courses?school_ids=2,3&active=1&term_bin_ids={termid}'
        a_courses = get_all_results(url, username, pswd)
        
        if a_courses:
            for course in a_courses['results']['courses']:
                if course['course_id'] not in active_courses:
                    active_courses.append(course['course_id'])
                    
    return active_courses



def get_courses(STARTOFSY, username, pswd):
    '''
    This function will:
    - take in SchoolRunner credentials
    - input all currently active courses into staging.SchoolRunner_Courses in SSMS
    '''
    termids = get_terms(STARTOFSY, username, pswd)
    courses = []

    for termid in termids:
        url = f'/courses?school_ids=2,3&active=1&term_bin_ids={termid}'
        response = get_all_results(url, username, pswd)
            
        if response:
            t = declare_database_table('staging','SchoolRunner_Courses')
    
            for item in response['results']['courses']:
                #This if stmt is to confirm course is not a dup
                if item['course_id'] not in [sub['course_id'] for sub in courses]: 
                    courses.append({ 'course_id' : item['course_id']
                                    ,'name' : item['name']
                                    ,'department_id' : item['department_id']
                                    ,'school_id' : item['school_id']
                                    ,'course_type_id': item['course_type_id']
                                    ,'term_bin_type_id' : item['term_bin_type_id']
                                    ,'grade_level_id' : item['grade_level_id']
                                    ,'eoy_test_id' : item['eoy_test_id']
                                    ,'default_grading_scale_set_id' : item['default_grading_scale_set_id']
                                    ,'standards_based_grading' : item['standards_based_grading']
                                    ,'show_on_report_card' : item['show_on_report_card']
                                    ,'credits' : item['credits']
                                    ,'exclude_missing' : item['exclude_missing']
                                    ,'enter_grades_by_level' : item['enter_grades_by_level']
                                    ,'teacher_sections_only' : item['teacher_sections_only']
                                    ,'ignore_max_expected_score' : item['ignore_max_expected_score']
                                    ,'order_key' : item['order_key']
                                    ,'from_date': item['from_date']
                                    ,'thru_date': item['thru_date']
                                    ,'active' : item['active']
                                    ,'display_name' : item['display_name']
                                })
    if len(courses) > 0:
        delete_this_table_data('staging.SchoolRunner_Courses')
        insert_data_into_database('staging.SchoolRunner_Courses', courses, t)
    

    
def get_coursegrades(STARTOFSY, username, pswd):
    '''
    This function will:
    - take in SchoolRunner credentials
    - input all grades for currently active courses into staging.SchoolRunner_CourseGrades in SSMS
    ''' 
    #Lists
    #termids = get_terms(STARTOFSY, username, pswd)
    active_courses = get_active_courses(STARTOFSY,username, pswd)
    
    coursegrades = []

    #Loop through all combinations of terms and active courses
    for termid, active_course in list(itertools.product(termids,active_courses)):
        url = f'/course-grades?school_ids=2,3&active=1&term_bin_ids={termid}&course_ids={active_course}'
        response = get_all_results(url, username, pswd)
      
        if response:
            t = declare_database_table('staging','SchoolRunner_CourseGrades')
    
            for item in response['results']['course_grades']:                
                coursegrades.append({'course_grade_id' : item['course_grade_id' ]
                                    ,'course_id' : item['course_id' ]
                                    ,'student_id' : item['student_id' ]
                                    ,'term_bin_id' : item['term_bin_id' ]
                                    ,'ps_termbin_id' : item['ps_termbin_id' ]
                                    ,'score' : item['score' ]
                                    ,'score_override' : item['score_override' ]
                                    ,'grading_scale_level_id' : item['grading_scale_level_id']
                                    ,'asof' : item['asof' ]
                                    ,'external_id' : item['external_id' ]
                                    ,'from_date' : item['from_date' ]
                                    ,'thru_date' : item['thru_date' ]
                                    ,'active' : item['active' ]
                                    ,'display_name' : item['display_name' ]
                                })
    if len(coursegrades) > 0:
        delete_this_table_data('staging.SchoolRunner_CourseGrades')
        insert_data_into_database('staging.SchoolRunner_CourseGrades', coursegrades, t)
        
        
        
def get_coursedefinitions(STARTOFSY, username, pswd):
    '''
    This function will:
    - take in SchoolRunner credentials
    - input all course-definitions for currently active courses into staging.SchoolRunner_CourseDefinitions in SSMS
    ''' 
    #Lists
    termids = get_terms(STARTOFSY, username, pswd)
    course_definitions = []
    
    for termid in termids:
        url = f'/course_definitions?school_ids=2,3&active=1&term_bin_ids={termid}'
        response = get_all_results(url, username, pswd)    
            
        if response:
            t = declare_database_table('staging','SchoolRunner_CourseDefinitions')
    
            for item in response['results']['course_definitions']:
                if item['course_definition_id'] not in [sub['course_definition_id'] for sub in course_definitions]:
                    course_definitions.append({ 'course_definition_id' : item['course_definition_id']
                                        ,'name' : item['name']
                                        ,'number' : item['number']
                                        ,'credits ' : item['credits']
                                        ,'school_id' : item['school_id']
                                        ,'external_id' : item['external_id']
                                        ,'from_date' : item['from_date']
                                        ,'thru_date' : item['thru_date']
                                        ,'active' : item['active']
                                        ,'display_name' : item['display_name']
                            })
                    
    if len(course_definitions) > 0:  
        delete_this_table_data('staging.SchoolRunner_CourseDefinitions')
        insert_data_into_database('staging.SchoolRunner_CourseDefinitions', course_definitions, t)

        
        
def get_schools(username, pswd):
    '''
    This function will:
    - take in SchoolRunner credentials
    - input all active schools into staging.SchoolRunner_Schools in SSMS
    '''
    schools = []

    url = f'/schools?&active=1'
    response = get_all_results(url, username, pswd)
        
    if response:
        t = declare_database_table('staging','SchoolRunner_Schools')
        for item in response['results']['schools']:
            if item['school_id'] not in [sub['school_id'] for sub in schools]:
                schools.append({ 'school_id' : item['school_id']
                                ,'long_name' : item['long_name']
                                ,'short_name' : item['short_name'] 
                                ,'district_id' : item['district_id']
                                ,'external_id' : item['external_id']
                                ,'min_grade_number' : item['min_grade_number']
                                ,'max_grade_number' : item['max_grade_number']
                                ,'suffix' : item['suffix']
                                ,'street' : item['street']
                                ,'city' : item['city']
                                ,'state' : item['state']
                                ,'zip' : item['zip']
                                ,'country' : item['country']
                                ,'phone' : item['phone']
                                ,'fax' : item['fax']
                                ,'principal_name' : item['principal_name']
                                ,'principal_phone' : item['principal_phone']
                                ,'principal_email' : item['principal_email']
                                ,'asst_principal_name' : item['asst_principal_name']
                                ,'asst_principal_phone' : item['asst_principal_phone']
                                ,'asst_principal_email' : item['asst_principal_email']
                                ,'from_date' : item['from_date']
                                ,'thru_date' : item['thru_date']
                                ,'active' : item['active']
                                ,'display_name' : item['display_name'] 
                                })
    if len(schools) > 0:  
        delete_this_table_data('staging.SchoolRunner_Schools')
        insert_data_into_database('staging.SchoolRunner_Schools', schools, t)

        
        
def get_students(username, pswd):
    '''
    This function will:
    - take in SchoolRunner credentials
    - input all active students into staging.SchoolRunner_Students in SSMS
    '''
    students = []

    url = f'/students?&active=1'
    response = get_all_results(url, username, pswd)
        
    if response:
        t = declare_database_table('staging','SchoolRunner_Students')
        for item in response['results']['students']:
            if item['school_id'] not in [sub['student_id'] for sub in students]:
                students.append({'student_id' : item['student_id']
                                ,'user_id' : item['user_id']
                                ,'first_name' : item['first_name']
                                ,'middle_name' : item['middle_name']
                                ,'last_name' : item['last_name']
                                ,'suffix' : item['suffix']
                                ,'school_id' : item['school_id']
                                ,'grade_level_id' : item['grade_level_id']
                                ,'external_id' : item['external_id']
                                ,'school_id_code' : item['school_id_code']
                                ,'sis_id' : item['sis_id']
                                ,'state_id' : item['state_id']
                                ,'from_date' : item['from_date']
                                ,'thru_date' : item['thru_date']
                                ,'active' : item['active']
                                ,'display_name' : item['display_name']
                                })
    if len(students) > 0:  
        delete_this_table_data('staging.SchoolRunner_Students')
        insert_data_into_database('staging.SchoolRunner_Students', students, t)

        
        
def get_gradingscales(username, pswd):
    '''
    This function will:
    - take in SchoolRunner credentials
    - input all grading-scales into staging.SchoolRunner_GradingScales in SSMS
    '''
    gradescales = []
    
    url = f'/grading-scales'
    response = get_all_results(url, username, pswd)
        
    if response:
        t = declare_database_table('staging','SchoolRunner_GradingScales')
        for item in response['results']['grading_scales']:
            if item['grading_scale_id'] not in [sub['grading_scale_id'] for sub in gradescales]:
                gradescales.append({'grading_scale_id' : item['grading_scale_id']
                                        ,'school_id' : item['school_id']
                                        ,'name' : item['name']
                                        ,'order_key' : item['is_pct']
                                        ,'is_pct' : item['order_key']
                                        ,'from_date' : item['from_date']
                                        ,'thru_date' : item['thru_date']
                                        ,'active' : item['active']
                                        ,'display_name' : item['display_name']
                                })
    
    if len(gradescales) > 0:  
        delete_this_table_data('staging.SchoolRunner_GradingScales')
        insert_data_into_database('staging.SchoolRunner_GradingScales', gradescales, t)

        
        
def get_gradingscalelevels(username, pswd):
    '''
    This function will:
    - take in SchoolRunner credentials
    - input all grading scale levels from each grading-scales into staging.SchoolRunner_GradingScaleLevels in SSMS
    '''
    gradingscalelevels = []
    
    url = f'/grading-scales?expand=grading_scale_levels'
    response = get_all_results(url, username, pswd)
    
        
    if response:
        t = declare_database_table('staging','SchoolRunner_GradingScaleLevels')
        #This gets a list of all the grading_scales dicts in the list of results
        for item in response['results']['grading_scales']:
            #This loops thorough that list to get all expanded grading_scale_levels dicts to append to original empty list
            for nest in item['grading_scale_levels']:
                gradingscalelevels.append({'grading_scale_level_id' : nest['grading_scale_level_id']
                                            ,'grading_scale_id' : nest['grading_scale_id']
                                            ,'name' : nest['name']
                                            ,'abbreviation' : nest['abbreviation']
                                            ,'comments' : nest['comments']
                                            ,'min_value' : nest['min_value']
                                            ,'max_value' : nest['max_value']
                                            ,'gpa_points' : nest['gpa_points']
                                            ,'color_override' : nest['color_override']
                                            ,'from_date' : nest['from_date']
                                            ,'thru_date' : nest['thru_date']
                                            ,'active' : nest['active']
                                            ,'display_name' : nest['display_name']
                                            ,'num_levels' : nest['num_levels']
                                            ,'index' : nest['index']
                                          })
   
    if len(gradingscalelevels) > 0:  
        delete_this_table_data('staging.SchoolRunner_GradingScaleLevels')
        insert_data_into_database('staging.SchoolRunner_GradingScaleLevels', gradingscalelevels, t)
        


# Main Program ------------------------------------------------------------------------------------------
start = t.time()
s_Main = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

try:
    ERROR_LOG_FILENAME = "ErrorLog {}.txt".format(str(datetime.now()).strip().replace(':', '_'))
    
    CONNECTION, ENGINE, METADATA, SESSION = start_connection_with_EK12()
    
    print('..............................Getting Terms..................................')
    termids= get_terms(STARTOFSY, username, pswd)

except Exception as error_message:
    send_email(error=error_message, table = 'Terms')
    
try:
    print('............................Getting Courses..................................')
    get_courses(STARTOFSY, username, pswd)

except Exception as error_message:
    send_email(error=error_message, table = 'Courses')
    
try:
    print('.........................Getting Course Grades...............................')
    get_coursegrades(STARTOFSY, username, pswd)

except Exception as error_message:
    send_email(error=error_message, table = 'Course Grades')
    
try:
    print('.......................Getting Course Definitions............................')
    get_coursedefinitions(STARTOFSY, username, pswd)

except Exception as error_message:
    send_email(error=error_message, table = 'Course Definition')
    
try:
    print('............................Getting Schools..................................')
    get_schools(username, pswd)

except Exception as error_message:
    send_email(error=error_message, table = 'Schools')
    
try:
    print('............................Getting Students.................................')
    get_students(username, pswd)

except Exception as error_message:
    send_email(error=error_message, table = 'Students')
    
try:
    print('.........................Getting Grading Scales..............................')
    get_gradingscales(username, pswd)

except Exception as error_message:
    send_email(error=error_message, table = 'Grading Scales')
    
try:
    print('........................Getting Grading Scale Levels.........................')
    get_gradingscalelevels(username, pswd)

except Exception as error_message:
    send_email(error=error_message, table = 'Grading Scale Levels')

end = t.time()    
e_Main = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
print(end - start)

if os.path.exists(ERROR_LOG_FILENAME):
    with open(ERROR_LOG_FILENAME,'r') as f:
        request_errors = f.readlines()
    f.close()
    request_errors = ''.join(request_errors)
    errorquery = f'''
                UPDATE sch.Table
                SET LEA = '***'
                 , Status = '{request_errors}'
                 , [Date] = '{dateran}'
                 , DailyFlag = '0'
                WHERE [Type] = 'SchoolRunner API'
                '''
    CONNECTION.execute(errorquery)
    
else:
    query = f'''
                UPDATE sch.Table
                SET LEA = '***'
                 , Status = 'Success'
                 , [Date] =  '{dateran}'
                 , DailyFlag = '0'
                WHERE [Type] = 'SchoolRunner API'
                '''
    CONNECTION.execute(query)