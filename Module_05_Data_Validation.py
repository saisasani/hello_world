########Python Library Import#########
import boto3
import snowflake.connector
from datetime import datetime
from configparser import ConfigParser
import time
import logging
import os
from os import path
import sys
import db_connect
import pandas as pd
import errno
import udf_record_count

""" Version 
Name           : Module_04_Snowflake_Data_To_S3
Description    : Date Comparison and Data Validation of SQL and Snowflake
Date           : 05/20/2019
Authors        : Kuldeep Sharma, Sai Aindla
Version        : Initial 1.0
"""


########Input Parameters Setup - mention database name in param.ini file#########
def param(filename='param/param.ini', section='parameters'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    input = {}
    # get section, default to mssql
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            input[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return input


parameters = param()

db = sys.argv[1]
#########Logging Section########
dyn_dir = datetime.today().strftime('%Y_%m_%d_%H')
inner_dir = str(parameters['output_path']) + "Logs\\" + "Validation_Data_Output_" + db + "_" + dyn_dir + ".log"


#########Directory Creation locally########
def create_dir(path):
    if not os.path.exists(os.path.dirname(path)):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


create_dir(inner_dir)

logger = logging.getLogger('root')
logger.setLevel(logging.INFO)

#########create a file handler########
handler = logging.FileHandler(inner_dir)
handler.setLevel(logging.INFO)

#########create a logging format########
formatter = logging.Formatter('[%(asctime)s] %(filename)s:%(lineno)d} %(levelname)s - %(message)s', "%m-%d-%Y %H:%M%p")
handler.setFormatter(formatter)

#########add the handlers to the logger########
logger.addHandler(handler)

#########AWS connect setup - mention database credentials in db_connect.py########
try:
    aws_conn_param = db_connect.config_aws()
except:
    logger.critical("AWS Connection Failure")

#########Snowflake Database connect setup - mention database credentials in db_connect.py########
try:
    sf_conn = db_connect.connect_sf()
    logger.info("Snowflake Database Connection Success")
except:
    logger.critical("Snowflake Database Connection Failure")

#########AWS Bucket Name########
AWS_BUCKET_NAME = aws_conn_param['awsbucketname']
start = time.time()

#########Snowflake Database Used########
SNOWFLAKE_DB = sf_conn['sf_database']

logger.info("Database Comparison Started for " + db)
print("Database Comparison Started for " + db)

local_path = str(
    parameters['output_path'])
Validation_path = str(
    parameters['output_path']) + "Data_Validation\\" + db + "\\Database_" + db + "_Data_Comparsion_report.xlsx"

try:
    create_dir(Validation_path)
except:
    logger.error("Directory Creation Failed")

########Database connect setup - mention database credentials in db_connect.py########
try:
    db_sql_conn = db_connect.dblist_sql_conn(db)
    logger.info("MsSql Database Connection Success")
except:
    logger.critical("MsSql Database Connection Failure")

s3 = boto3.client('s3', aws_access_key_id=aws_conn_param['aws_access_key_id'],
                  aws_secret_access_key=aws_conn_param['aws_secret_access_key'])

table_list = """SELECT DISTINCT a.TABLE_SCHEMA,a.TABLE_NAME
            FROM INFORMATION_SCHEMA.COLUMNS a
            LEFT OUTER JOIN INFORMATION_SCHEMA.VIEWS b ON a.TABLE_CATALOG = b.TABLE_CATALOG
            AND a.TABLE_SCHEMA = b.TABLE_SCHEMA
            AND a.TABLE_NAME = b.TABLE_NAME WHERE CASE WHEN b.TABLE_NAME is not null then 'view' else 'table' end='table'
            AND a.TABLE_CATALOG = """ + "'" + db + "'"

tbllist_df = pd.read_sql(table_list, db_connect.dblist_sql_conn(db))

count = 1  # Variable indicator for data append to validation file
custom_table_list = parameters['table_list']
compare_data = []
if (custom_table_list != 'All'):
    # i = 1
    for table in custom_table_list.split(","):
        i = 1
        table_name = table.upper()
        print(table_name)
        _PREFIX = "Data_Validation\\" + db + "\\" + table_name + "\\"
        list = os.listdir(local_path + _PREFIX)
        number_files = len(list)
        count_is_match = ''
        sql_rows = 0
        sf_rows = 0
        unmatch_data = ''
        is_data_match = ''
        if (number_files / 2 == 1):
            number_files = 2
        else:
            number_files = (number_files / 2) + 1
        for i in range(1, int(number_files)):
            print(i)
            sql_file_name = table_name + "_" + str(i) + ".csv"
            print(sql_file_name)
            sf_file_name = table_name + "_SF" + "_" + str(i) + ".csv"
            print(sf_file_name)
            schema_sf = db
            sql_rows = udf_record_count.rawcount(local_path + _PREFIX + sql_file_name)
            sf_rows = udf_record_count.rawcount(local_path + _PREFIX + sf_file_name)
            Count_difference = udf_record_count.rawcount(
                local_path + _PREFIX + sql_file_name) - udf_record_count.rawcount(
                local_path + _PREFIX + sf_file_name)
            print(Count_difference)
            if (Count_difference == 0):
                count_is_match = 'TRUE'
            else:
                count_is_match = 'FALSE'

            ##########Data Comparision##########
            with open(local_path + _PREFIX + sql_file_name, 'r') as t1, open(local_path + _PREFIX + sf_file_name,
                                                                             'r') as t2:
                sql_data = t1.readlines()
                sf_data = t2.readlines()

                for line in sf_data:
                    if line.replace('.0', '') not in sql_data:
                        unmatch_data = unmatch_data + line  # + '\n'

            if not unmatch_data:  # if string is empty
                is_data_match = "TRUE"
                compare_data.append({'MSSQL_DATABASE_NAME': db.upper(),
                                     'MSSQL_TABLE_NAME': table_name.upper(),
                                     'SF_DATABASE_NAME': SNOWFLAKE_DB.upper(),
                                     'SF_SCHEMA_NAME': schema_sf,
                                     'SF_TABLE_NAME': table_name.upper(),
                                     'MSSQL_COUNT': sql_rows,
                                     'SF_COUNT': sf_rows,
                                     'IS_COUNT_MATCH': count_is_match,
                                     'IS_DATA_MATCH': is_data_match,
                                     'UN_MATCH_DATA_LOCATION': ''})
                os.remove(local_path + _PREFIX + sql_file_name)
                os.remove(local_path + _PREFIX + sf_file_name)
            else:
                unmatched_file = local_path + _PREFIX + table_name + '_Unmatch_Data.csv'
                f = open(unmatched_file, 'w')
                f.write(unmatch_data)  # Give your csv text here.
                ##########Python will convert \n to os.linesep##########
                f.close()
                is_data_match = "FALSE"
                compare_data.append({'MSSQL_DATABASE_NAME': db.upper(),
                                     'MSSQL_TABLE_NAME': table_name.upper(),
                                     'SF_DATABASE_NAME': SNOWFLAKE_DB.upper(),
                                     'SF_SCHEMA_NAME': schema_sf,
                                     'SF_TABLE_NAME': table_name.upper(),
                                     'MSSQL_COUNT': sql_rows,
                                     'SF_COUNT': sf_rows,
                                     'IS_COUNT_MATCH': count_is_match,
                                     'IS_DATA_MATCH': is_data_match,
                                     'UN_MATCH_DATA_LOCATION': ''})
                os.remove(local_path + _PREFIX + sql_file_name)
                os.remove(local_path + _PREFIX + sf_file_name)
        _PREFIX = "Data_Validation\\" + db + "\\"
        filename = 'Database_' + db + '_DataValidation_Report.csv'
        validation_report = pd.DataFrame(compare_data)
        validation_report.to_csv(local_path + _PREFIX + filename,
                                 columns=["MSSQL_DATABASE_NAME", "MSSQL_TABLE_NAME", "SF_DATABASE_NAME",
                                          "SF_SCHEMA_NAME", "SF_TABLE_NAME", "MSSQL_COUNT", "SF_COUNT",
                                          "IS_COUNT_MATCH", "IS_DATA_MATCH", "UN_MATCH_DATA_LOCATION"],
                                 index=False)
        print('Table ' + sql_file_name.replace('.csv', '') + ' Completed')
        logger.info('Table ' + sql_file_name.replace('.csv', '') + ' Completed')

if (custom_table_list == 'All'):

    for index, table in tbllist_df.iterrows():

        table_name = table[1].upper()
        print(table_name)
        _PREFIX = "Data_Validation\\" + db + "\\" + table_name + "\\"

        list = os.listdir(local_path + _PREFIX)
        number_files = len(list)
        count_is_match = ''
        sql_rows = 0
        sf_rows = 0
        unmatch_data = ''
        is_data_match = ''
        if (number_files / 2 == 1):
            number_files = 2
        else:
            number_files = (number_files / 2) + 1

        for i in range(1, int(number_files)):
            print(i)
            sql_file_name = table_name + "_" + str(i) + ".csv"
            print(sql_file_name)
            sf_file_name = table_name + "_SF" + "_" + str(i) + ".csv"
            print(sf_file_name)
            schema_sf = db
            sql_rows = udf_record_count.rawcount(local_path + _PREFIX + sql_file_name)
            sf_rows = udf_record_count.rawcount(local_path + _PREFIX + sf_file_name)
            Count_difference = udf_record_count.rawcount(
                local_path + _PREFIX + sql_file_name) - udf_record_count.rawcount(
                local_path + _PREFIX + sf_file_name)
            print(Count_difference)
            if (Count_difference == 0):
                count_is_match = 'TRUE'
            else:
                count_is_match = 'FALSE'

            ###########Data Comparision##########
            with open(local_path + _PREFIX + sql_file_name, 'r') as t1, open(local_path + _PREFIX + sf_file_name,
                                                                             'r') as t2:
                sql_data = t1.readlines()
                sf_data = t2.readlines()
                for line in sf_data:
                    if line.replace('.0', '') not in sql_data:
                        unmatch_data = unmatch_data + line

            if not unmatch_data:  # if string is empty
                is_data_match = "TRUE"
                compare_data.append({'MSSQL_DATABASE_NAME': db.upper(),
                                     'MSSQL_TABLE_NAME': table_name.upper(),
                                     'SF_DATABASE_NAME': SNOWFLAKE_DB.upper(),
                                     'SF_SCHEMA_NAME': schema_sf,
                                     'SF_TABLE_NAME': table_name.upper(),
                                     'MSSQL_COUNT': sql_rows,
                                     'SF_COUNT': sf_rows,
                                     'IS_COUNT_MATCH': count_is_match,
                                     'IS_DATA_MATCH': is_data_match,
                                     'UN_MATCH_DATA_LOCATION': ''})
                os.remove(local_path + _PREFIX + sql_file_name)
                os.remove(local_path + _PREFIX + sf_file_name)
            else:
                unmatched_file = local_path + _PREFIX + table_name + '_Unmatch_Data.csv'
                f = open(unmatched_file, 'w')
                f.write(unmatch_data)
                #######Python will convert \n to os.linesep#########
                f.close()
                is_data_match = "FALSE"
                compare_data.append({'MSSQL_DATABASE_NAME': db.upper(),
                                     'MSSQL_TABLE_NAME': table_name.upper(),
                                     'SF_DATABASE_NAME': SNOWFLAKE_DB.upper(),
                                     'SF_SCHEMA_NAME': schema_sf,
                                     'SF_TABLE_NAME': table_name.upper(),
                                     'MSSQL_COUNT': sql_rows,
                                     'SF_COUNT': sf_rows,
                                     'IS_COUNT_MATCH': count_is_match,
                                     'IS_DATA_MATCH': is_data_match,
                                     'UN_MATCH_DATA_LOCATION': ''})
                os.remove(local_path + _PREFIX + sql_file_name)
                os.remove(local_path + _PREFIX + sf_file_name)
        _PREFIX = "Data_Validation\\" + db + "\\"
        filename = 'Database_' + db + '_DataValidation_Report.csv'
        validation_report = pd.DataFrame(compare_data)
        validation_report.to_csv(local_path + _PREFIX + filename,
                                 columns=["MSSQL_DATABASE_NAME", "MSSQL_TABLE_NAME", "SF_DATABASE_NAME",
                                          "SF_SCHEMA_NAME", "SF_TABLE_NAME", "MSSQL_COUNT", "SF_COUNT",
                                          "IS_COUNT_MATCH", "IS_DATA_MATCH", "UN_MATCH_DATA_LOCATION"],
                                 index=False)
        print('Table ' + sql_file_name.replace('.csv', '') + ' Completed')
        logger.info('Table ' + sql_file_name.replace('.csv', '') + ' Completed')