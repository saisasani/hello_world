########Python Library Import#########
import pandas as pd
import boto3
import os
import errno
import sys
import time
import subprocess
from configparser import ConfigParser
import logging
from datetime import datetime
import db_connect

""" Version 
Name           : Module_02_MSSql_Data_To_S3
Description    : Export MsSql Table Data to S3
Date           : 05/20/2019
Authors        : Kuldeep Sharma, Sai Aindla
Version        : Initial 1.0
"""


##############Input Parameters Setup - mention database name in param.ini file###############
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

##################Logging Section####################
dyn_dir = datetime.today().strftime('%Y_%m_%d_%H')
inner_dir = str(parameters['output_path']) + "Logs\\" + dyn_dir + ".log"

if not os.path.exists(os.path.dirname(inner_dir)):
    try:
        os.makedirs(os.path.dirname(inner_dir))
    except OSError as exc:  # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

logger = logging.getLogger('root')
logger.setLevel(logging.INFO)

#############create a file handler################
handler = logging.FileHandler(inner_dir)
handler.setLevel(logging.INFO)

#############create a logging format##############
formatter = logging.Formatter('[%(asctime)s] %(filename)s:%(lineno)d} %(levelname)s - %(message)s', "%m-%d-%Y %H:%M%p")
handler.setFormatter(formatter)

#############add the handlers to the logger###########
logger.addHandler(handler)

###########Database connection setup - mention database credentials in db_connect.py############
#############Connection to SQL Server#############
try:
    sql_conn = db_connect.connect_sql()
    logger.info("MsSql Database Connection Success")
except:
    logger.critical("MsSql Database Connection Failure")

##############Connection to Snowflake#############
try:
    sql_conn_param = db_connect.connect_sql_param()
except:
    logger.critical("MsSql Database Parameter input Failure")

#################AWS connection setup - mention database credentials in db_connect.py##############
try:
    aws_conn_param = db_connect.config_aws()
except:
    logger.critical("AWS Connection Failure")

#################Configure output path to store DDL Scripts##################
main_path = str(parameters['output_path'])

#################Database List to process####################
sql_table_catalog_list = str(parameters['database_list'].split(",")).replace("[", "").replace("]", "")

#################Selecting the database from sys database################
db_list = """SELECT DISTINCT (NAME)TABLE_CATALOG FROM sys.databases WHERE NAME IN (""" + sql_table_catalog_list + ") ORDER BY NAME"
dblist_df = pd.read_sql(db_list, sql_conn)
dblist = dblist_df['TABLE_CATALOG']

AWS_BUCKET_NAME = aws_conn_param['awsbucketname']
custom_table_list = parameters['table_list']
custom_schema = parameters['schema']

db = sys.argv[1]
print(db)

start = time.time()
s3 = boto3.client("s3", aws_access_key_id=aws_conn_param['aws_access_key_id'],
                  aws_secret_access_key=aws_conn_param['aws_secret_access_key'])

#############Looping Database one by one which are passed in param file#############
try:
    ddl_create_table_file = ""
    db_sql_conn = db_connect.dblist_sql_conn(db)
    print("Database Extract Started For " + db)
    s3 = boto3.client("s3", aws_access_key_id=aws_conn_param['aws_access_key_id'],
                      aws_secret_access_key=aws_conn_param['aws_secret_access_key'])
    if (custom_table_list == "All"):

        table_list = """SELECT DISTINCT a.TABLE_SCHEMA,a.TABLE_NAME
                       FROM INFORMATION_SCHEMA.COLUMNS a
                       LEFT OUTER JOIN INFORMATION_SCHEMA.VIEWS b ON a.TABLE_CATALOG = b.TABLE_CATALOG
                       AND a.TABLE_SCHEMA = b.TABLE_SCHEMA
                       AND a.TABLE_NAME = b.TABLE_NAME WHERE CASE WHEN b.TABLE_NAME is not null then 'view' else 'table' end='table'
                       AND a.TABLE_CATALOG = """ + "'" + db + "'"
        tbllist_df = pd.read_sql(table_list, db_sql_conn)
        logger.info("Data Export Started for Database " + db)

        ###########Uploading Data output as CSV to S3##########
        for i, j in tbllist_df.iterrows():
            select_query = """SELECT DISTINCT 'SELECT '+ COLUMN_NAME +' FROM """ + db + "." + \
                           j[0] + "." + j[1] + """' FROM 
                                (
                                SELECT DISTINCT STUFF((SELECT N', ' + 
                                                CASE WHEN DATA_TYPE IN ('varchar','nvarchar','char','text') 
                                                THEN 'char(34)+'+'replace(['+p2.COLUMN_NAME+'],''""'',''~""'')'+'+char(34) AS [' + p2.COLUMN_NAME+']'
                                                ELSE '['+p2.COLUMN_NAME+']' END AS COLUMN_NAME
                                FROM INFORMATION_SCHEMA.COLUMNS p2
                                WHERE p2.TABLE_NAME = p.TABLE_NAME 
                                ORDER BY p2.ORDINAL_POSITION
                                FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')  ,1, 2, N'') AS COLUMN_NAME
                            FROM INFORMATION_SCHEMA.COLUMNS p WHERE p.TABLE_NAME= '""" + \
                           j[1] + """' AND p.TABLE_CATALOG='""" + db + """')Y """

            cur = db_sql_conn.cursor()
            res = cur.execute(select_query)
            for r in res:
                select_query_cmd = r[0]
                db = db.upper()
                Output_path = main_path + "Data_Output\\" + db + "\\" + j[1].upper() + ".csv"

                #########Dynamic file name as per table name##########
                sql_file_name = db + "/" + j[1].upper() + ".csv"
                if not os.path.exists(os.path.dirname(Output_path)):
                    try:
                        os.makedirs(os.path.dirname(Output_path))
                    except OSError as exc:  # Guard against race condition
                        if exc.errno != errno.EEXIST:
                            raise

                ########BCP Bulk Copy - Copies the data faster in local and then upload to S3#######
                process = ' Loading... '
                print('Table ' + j[1] + process)
                BCP_query = 'BCP "' + select_query_cmd + '" QUERYOUT ' + Output_path + """ -C iso-8859-1 -S """ + \
                            sql_conn_param['server'] + """ /c /t"|" -r$ -U """ + sql_conn_param['uid'] + ' -P "' + \
                            sql_conn_param['pwd'] + '"'
                subprocess.run(BCP_query)

                ##############Upload csv to S3##############
                try:
                    s3.upload_file(Output_path, AWS_BUCKET_NAME, sql_file_name)
                    process = 'Load Completed and Moved to S3 for '
                    print('Table ' + j[1] + process)
                    os.remove(Output_path)
                except:
                    logger.error("Fail to upload in S3")
                logger.info("Data Export Completed for table " + j[1])
            logger.info("Data Export Completed for Database " + db)

    elif (custom_table_list != "All"):
        for custom_table in custom_table_list.split(","):
            #######concatenation of schema and table schema.table1 for table read#########
            select_query = """SELECT DISTINCT 'SELECT '+ COLUMN_NAME +' FROM """ + db + "." + custom_schema + "." + custom_table + """' FROM 
                                (
                                SELECT DISTINCT STUFF((SELECT N', ' + 
                                                CASE WHEN DATA_TYPE IN ('varchar','nvarchar','char','text') 
                                                THEN 'char(34)+'+'replace(['+p2.COLUMN_NAME+'],''""'',''`""'')'+'+char(34) AS [' + p2.COLUMN_NAME+']'
                                                ELSE '['+p2.COLUMN_NAME+']' END AS COLUMN_NAME
                                FROM INFORMATION_SCHEMA.COLUMNS p2
                                WHERE p2.TABLE_NAME = p.TABLE_NAME 
                                ORDER BY p2.ORDINAL_POSITION
                                FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')  ,1, 2, N'') AS COLUMN_NAME
                            FROM INFORMATION_SCHEMA.COLUMNS p WHERE p.TABLE_NAME= '""" + custom_table + """' AND p.TABLE_CATALOG='""" + db + """')Y """

            cur = db_sql_conn.cursor()
            res = cur.execute(select_query)

            for r in res:
                select_query_cmd = r[0]
                db = db.upper()
                Output_path = main_path + "Data_Output\\" + db + '\\' + custom_table.upper() + '.csv'
                #######dynamic file name as per table name########
                sql_file_name = db + "/" + custom_table.upper() + ".csv"
                if not os.path.exists(os.path.dirname(Output_path)):
                    try:
                        os.makedirs(os.path.dirname(Output_path))
                    except OSError as exc:  # Guard against race condition
                        if exc.errno != errno.EEXIST:
                            raise
                ########BCP Bulk Copy - Copies the data faster in local and then upload to S3
                process = ' Loading... '
                print('Table ' + custom_table + process)

                BCP_query = 'BCP "' + select_query_cmd + '" QUERYOUT ' + Output_path + """ -C iso-8859-1 -S """ + \
                            sql_conn_param['server'] + """ /c /t"|" -r$ -U """ + sql_conn_param['uid'] + ' -P "' + \
                            sql_conn_param['pwd'] + '"'
                subprocess.run(BCP_query)

                try:
                    s3.upload_file(Output_path, AWS_BUCKET_NAME, sql_file_name)
                    process = ' Load Completed and Moved to S3'
                    print('Table ' + custom_table + process)
                    os.remove(Output_path)
                except:
                    logger.error("Fail to upload in S3")
                logger.info("Data Export Completed for table " + custom_table)
    else:
        print("Please Validate the Parameters")
except:
    logger.error("MsSql Data Export Failed")
logger.info("Data Export Completed for Database " + db)

end = time.time()
if ((end - start) < 60):
    Total_time = round(end - start, 3)
    print("Time for " + db + " Export to s3 is %f minutes" % (Total_time))
    logger.info("Time for " + db + " Export to s3 is %f seconds" % (Total_time))
else:
    Total_time = round((end - start) / 60, 3)
    print("Time for " + db + " Export to s3 is %f minutes" % (Total_time))
    logger.info("Time for " + db + " Export to s3 is %f minutes" % (Total_time))

sql_conn.close()