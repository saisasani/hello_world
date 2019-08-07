########Python Library Import#########
import snowflake.connector
from datetime import datetime
from configparser import ConfigParser
import logging
import sys
import boto3
import os
from datetime import timezone
import pytz
import time
import db_connect
import pandas as pd
import errno

""" Version 
Name           : Module_06_Adding_Table_Constraints_Snowflake
Description    : Adding constraints to the tables
Date           : 05/20/2019
Authors        : Kuldeep Sharma, Sai Aindla
Version        : Initial 1.0
"""

start = time.time()
#########Input Parameters Setup - mention database name in param.ini file########
def param(filename='param/param.ini', section='parameters'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    input ={}
    # get section, default to mssql
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            input[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return input


parameters =param()


db = sys.argv[1]
print(db)

#########Logging Section########
dyn_dir = datetime.today().strftime('%Y_%m_%d_%H')
inner_dir = str(parameters['output_path']) + "Logs\\"+"Validation_Data_Output_"+db+"_"+dyn_dir+".log"

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

#########Database connection setup - mention database credentials in db_connect.py########
try:
    sql_conn = db_connect.connect_sql()
    logger.info("MsSql Database Connection Success")
except:
    logger.critical("MsSql Database Connection Failure")

try:
    sf_conn = db_connect.connect_sf()
    logger.info("Snowflake Database Connection Success")
except:
    logger.critical("Snowflake Database Connection Failure")

#########Snowflake database Used BOPS_DB_PROD or BSTRAT_DB_PROD taken from config file########
SNOWFLAKE_DB = sf_conn['sf_database']

#########Snowflake Connection Details taken from config file########
try:
    ctx_snowflake = snowflake.connector.connect(**sf_conn)
except:
    logger.critical("Snowflake Connection Failed")

######### AWS connection setup - mention database credentials in db_connect.py########
try:
    aws_conn_param = db_connect.config_aws()
except:
    logger.critical("AWS Connection Failure")

#########Snowflake user role########
userrole = sf_conn['userole']
#########Snowflake virtual warehouse to run the queries########
usewarehouse = sf_conn['usewarehouse']
#########Running the queries to use role and warehouse in snowflake########
cs_snowflake = ctx_snowflake.cursor()
cs_snowflake.execute("USE ROLE " + userrole)
cs_snowflake.execute("USE WAREHOUSE " + usewarehouse)
cs_snowflake.execute("alter session set timezone = 'America/New_York'")
cs_snowflake.execute("alter session set TIME_OUTPUT_FORMAT = 'HH24:MI:SS.FF3'")


AWS_BUCKET_NAME = aws_conn_param['awsbucketname']

s3 = boto3.client("s3", aws_access_key_id=aws_conn_param['aws_access_key_id'],
                      aws_secret_access_key=aws_conn_param['aws_secret_access_key'])

ddl_script_path = str(parameters['output_path'])
#########Looping through each database in MSSQL and adding constraints to respective schema in Snowflake accordingly########
####MSSQL Connection to the database#####
db_sql_conn = db_connect.dblist_sql_conn(db)
########Query to extract the table_name,constraint_name, constraint_type and columns that are to be added as keys########
query = """SELECT b.astblvw as tableview, COALESCE(details, detail) as details FROM
(
select table_view,
details
from (
select schema_name(t.schema_id) + '.' + t.[name] as table_view, 
    case when t.[type] = 'U' then 'Table'
        when t.[type] = 'V' then 'View'
        end as [object_type],
    case when c.[type] = 'PK' then 'Primary key'
        when c.[type] = 'UQ' then 'Unique constraint'
        when i.[type] = 1 then 'Unique clustered index'
        when i.type = 2 then 'Unique index'
        end as constraint_type, 
    isnull(c.[name], i.[name]) as constraint_name,
    substring(column_names, 1, len(column_names)-1) as [details]
from sys.objects t
    left outer join sys.indexes i
        on t.object_id = i.object_id
    left outer join sys.key_constraints c
        on i.object_id = c.parent_object_id 
        and i.index_id = c.unique_index_id
   cross apply (select col.[name] + ', '
                    from sys.index_columns ic
                        inner join sys.columns col
                            on ic.object_id = col.object_id
                            and ic.column_id = col.column_id
                    where ic.object_id = t.object_id
                        and ic.index_id = i.index_id
                            order by col.column_id
                            for xml path ('') ) D (column_names)
where is_unique = 1
and t.is_ms_shipped <> 1
union all 
select schema_name(fk_tab.schema_id) + '.' + fk_tab.name as foreign_table,
    'Table',
    'Foreign key',
    fk.name as fk_constraint_name,
    schema_name(pk_tab.schema_id) + '.' + pk_tab.name
from sys.foreign_keys fk
    inner join sys.tables fk_tab
        on fk_tab.object_id = fk.parent_object_id
    inner join sys.tables pk_tab
        on pk_tab.object_id = fk.referenced_object_id
    inner join sys.foreign_key_columns fk_cols
        on fk_cols.constraint_object_id = fk.object_id
union all
select schema_name(t.schema_id) + '.' + t.[name],
    'Table',
    'Check constraint',
    con.[name] as constraint_name,
    con.[definition]
from sys.check_constraints con
    left outer join sys.objects t
        on con.parent_object_id = t.object_id
    left outer join sys.all_columns col
        on con.parent_column_id = col.column_id
        and con.parent_object_id = col.object_id
union all
select schema_name(t.schema_id) + '.' + t.[name],
    'Table',
    'Default constraint',
    con.[name],
    col.[name] + ' = ' + con.[definition]
from sys.default_constraints con
    left outer join sys.objects t
        on con.parent_object_id = t.object_id
    left outer join sys.all_columns col
        on con.parent_column_id = col.column_id
        and con.parent_object_id = col.object_id) a
        )c
        full outer JOIN (select TABLE_SCHEMA+'.'+TABLE_NAME astblvw, '1' as detail from information_schema.TABLES )b
        ON c.table_view=b.astblvw"""

#########Creating Dataframe for the query########
sql_constraints = pd.read_sql(query,db_sql_conn)
custom_table_list = parameters['table_list']

for i, j in sql_constraints.iterrows():
    sql_schema_table = j['tableview']
    table_name = j['tableview'].split(".")[1]
    if(custom_table_list != 'All'):
        for table in custom_table_list.split(","):
            if(table == table_name.upper()):
            ######Columns that are to be added as constraints########
                details = j['details']
                Validation_path = ddl_script_path + "\\Data_Validation\\" + db + "\\" + table_name.upper() + '\\'
                if not os.path.exists(os.path.dirname(Validation_path)):
                    try:
                        os.makedirs(os.path.dirname(Validation_path))
                    except OSError as exc:  # Guard against race condition
                        if exc.errno != errno.EEXIST:
                            raise

                select_st = """SELECT DISTINCT STUFF((SELECT N', ' +CASE WHEN DATA_TYPE IN ('datetime','datetime2','smalldatetime')
                                                THEN 'CONVERT(varchar,'+p2.COLUMN_NAME+',20)'
                                                WHEN DATA_TYPE IN ('numeric','decimal','float')
                                                THEN 'CONVERT(BIGINT,'+p2.COLUMN_NAME+') AS ['+p2.COLUMN_NAME+']'
                                                ELSE '['+p2.COLUMN_NAME+']' END AS COLUMN_NAME
                                                FROM INFORMATION_SCHEMA.COLUMNS p2
                                                WHERE p2.TABLE_NAME = p.TABLE_NAME
                                                ORDER BY p2.ORDINAL_POSITION
                                                FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')  ,1, 2, N'') AS COLUMN_NAME
                                                FROM INFORMATION_SCHEMA.COLUMNS p WHERE p.TABLE_NAME= """ + "'" + table_name + "'"

                se = pd.read_sql(select_st, db_sql_conn)

                col_list = se.iat[0, 0]
                sql_chunk_size = 100000
                sql_offset = 0
                dfs = []
                sql_chunk_files_no = 1
                while True:
                    sql_query = "SELECT " + col_list + " FROM " + db + "." + sql_schema_table + " ORDER BY " + details + " offset  %d ROWS FETCH NEXT %d ROWS ONLY" % (
                        sql_offset, sql_chunk_size)
                    dfs.append(pd.read_sql(sql_query, db_sql_conn))
                    sql_offset += sql_chunk_size
                    chunk_file = pd.read_sql(sql_query, db_sql_conn)
                    Output_path = Validation_path + table_name.upper() + "_" + str(sql_chunk_files_no) + ".csv"
                    sql_filename = "Data_Validation/" + db + "/" + table_name.upper() + '/' + table_name.upper() + "_" + str(
                        sql_chunk_files_no) + ".csv"
                    sql_file = chunk_file.to_csv(Output_path, index=False, header=False, sep="|",
                                                 date_format="%Y-%m-%d %H:%M:%S")
                    s3.upload_file(Output_path, AWS_BUCKET_NAME, sql_filename)
                    print(sql_filename + " file uploaded to s3")
                    if len(dfs[-1]) < sql_chunk_size:
                        break
                    sql_chunk_files_no += 1
                print(
                    "Total number of chunk files for sql table " + table_name.upper() + " is/are %d" % (sql_chunk_files_no))
                logger.info(
                    "Total number of chunk files for sql table " + table_name.upper() + " is/are %d" % (sql_chunk_files_no))

                sf_st = """SELECT DISTINCT STUFF((SELECT N', ' +CASE WHEN DATA_TYPE IN ('datetime','datetime2','smalldatetime')
                                        THEN 'TO_VARCHAR('+p2.COLUMN_NAME+',''YYYY-MM-DD HH24:MI:SS'')'
                                        WHEN DATA_TYPE IN ('numeric','decimal','float')
                                        THEN 'CAST('+p2.COLUMN_NAME+' AS INTEGER) AS '+p2.COLUMN_NAME
                                        ELSE p2.COLUMN_NAME END AS COLUMN_NAME
                                        FROM INFORMATION_SCHEMA.COLUMNS p2
                                        WHERE p2.TABLE_NAME = p.TABLE_NAME
                                        ORDER BY p2.ORDINAL_POSITION
                                        FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')  ,1, 2, N'') AS COLUMN_NAME
                                        FROM INFORMATION_SCHEMA.COLUMNS p WHERE p.TABLE_NAME= """ + "'" + table_name + "'"

                sf = pd.read_sql(sf_st, db_sql_conn)

                sf_col_list = sf.iat[0, 0]
				sf_col_list = sf_col_list
                sf_chunk_size = 100000
                sf_offset = 0
                dfs = []
                sf_chunk_files_no = 1
                while True:
                    sf_query = "SELECT " + sf_col_list + " FROM " + SNOWFLAKE_DB + "." + db + "." + table_name + " ORDER BY " + details + " offset  %d ROWS FETCH NEXT %d ROWS ONLY" % (
                        sf_offset, sf_chunk_size)
                    dfs.append(pd.read_sql(sf_query, ctx_snowflake))
                    sf_offset += sf_chunk_size

                    cs_snowflake.execute("alter session set timezone = 'America/New_York'")
                    cs_snowflake.execute(sf_query)
                    chunk_file = pd.DataFrame(cs_snowflake.fetchall())

                    Output_path = Validation_path + table_name.upper() + "_SF_" + str(sf_chunk_files_no) + ".csv"
                    sf_filename = "Data_Validation/" + db + "/" + table_name.upper() + '/' + table_name.upper() + "_SF_" + str(
                        sf_chunk_files_no) + ".csv"
                    sf_file = chunk_file.to_csv(Output_path, index=False, header=False, sep="|",
                                                date_format="%Y-%m-%d %H:%M:%S")
                    s3.upload_file(Output_path, AWS_BUCKET_NAME, sf_filename)
                    print(sf_filename + " file uploaded to s3")
                    if len(dfs[-1]) < sf_chunk_size:
                        break
                    sf_chunk_files_no += 1
                print("Total number of chunk files for snowflake table " + table_name.upper() + " is/are %d" % (sf_chunk_files_no))
                logger.info("Total number of chunk files for snowflake table " + table_name.upper() + " is/are %d" % (sf_chunk_files_no))

    if (custom_table_list == 'All'):
        details = j['details']
        Validation_path = ddl_script_path + "\\Data_Validation\\" + db + "\\" + table_name.upper() + '\\'
        if not os.path.exists(os.path.dirname(Validation_path)):
            try:
                os.makedirs(os.path.dirname(Validation_path))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

        select_st = """SELECT DISTINCT STUFF((SELECT N', ' +CASE WHEN DATA_TYPE IN ('datetime','datetime2','smalldatetime')
                            THEN 'CONVERT(varchar,'+p2.COLUMN_NAME+',20)'
							WHEN DATA_TYPE IN ('numeric','decimal','float')
							THEN 'CONVERT(BIGINT,'+p2.COLUMN_NAME+') AS ['+p2.COLUMN_NAME+']'
                            ELSE '['+p2.COLUMN_NAME+']' END AS COLUMN_NAME
                            FROM INFORMATION_SCHEMA.COLUMNS p2
                            WHERE p2.TABLE_NAME = p.TABLE_NAME
                            ORDER BY p2.ORDINAL_POSITION
                            FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')  ,1, 2, N'') AS COLUMN_NAME
                            FROM INFORMATION_SCHEMA.COLUMNS p WHERE p.TABLE_NAME= """ + "'" + table_name + "'"

        se = pd.read_sql(select_st, db_sql_conn)

        col_list = se.iat[0, 0]
        sql_chunk_size = 100000
        sql_offset = 0
        dfs = []
        sql_chunk_files_no = 1
        while True:
            sql_query = "SELECT "+col_list+" FROM " + db + "." + sql_schema_table + " ORDER BY " + details + " offset  %d ROWS FETCH NEXT %d ROWS ONLY" % (
            sql_offset, sql_chunk_size)
            dfs.append(pd.read_sql(sql_query, db_sql_conn))
            sql_offset += sql_chunk_size
            chunk_file = pd.read_sql(sql_query, db_sql_conn)
            Output_path = Validation_path + table_name.upper() + "_" + str(sql_chunk_files_no) + ".csv"
            sql_filename = "Data_Validation/" + db + "/" + table_name.upper() + '/' + table_name.upper() + "_" + str(
                sql_chunk_files_no) + ".csv"
            sql_file = chunk_file.to_csv(Output_path, index=False, header=False, sep="|",date_format="%Y-%m-%d %H:%M:%S")
            s3.upload_file(Output_path, AWS_BUCKET_NAME, sql_filename)
            print(sql_filename + " file uploaded to s3")
            if len(dfs[-1]) < sql_chunk_size:
                break
            sql_chunk_files_no += 1
        print("Total number of chunk files for sql table " + table_name.upper() + " is/are %d" % (sql_chunk_files_no))
        logger.info("Total number of chunk files for sql table " + table_name.upper() + " is/are %d" % (sql_chunk_files_no))

        sf_st = """SELECT DISTINCT STUFF((SELECT N', ' +CASE WHEN DATA_TYPE IN ('datetime','datetime2','smalldatetime')
                    THEN 'TO_VARCHAR('+p2.COLUMN_NAME+',''YYYY-MM-DD HH24:MI:SS'')'
                    WHEN DATA_TYPE IN ('numeric','decimal','float')
                    THEN 'CAST('+p2.COLUMN_NAME+' AS INTEGER) AS '+p2.COLUMN_NAME
                    ELSE p2.COLUMN_NAME END AS COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS p2
                    WHERE p2.TABLE_NAME = p.TABLE_NAME
                    ORDER BY p2.ORDINAL_POSITION
                    FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')  ,1, 2, N'') AS COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS p WHERE p.TABLE_NAME= """ + "'" + table_name + "'"

        sf = pd.read_sql(sf_st, db_sql_conn)

        sf_col_list = sf.iat[0, 0]
        sf_col_list = sf_col_list
        sf_chunk_size = 100000
        sf_offset = 0
        dfs = []
        sf_chunk_files_no = 1
        while True:
            sf_query = "SELECT "+sf_col_list+" FROM " + SNOWFLAKE_DB + "." + db + "." + table_name + " ORDER BY " + details + " offset  %d ROWS FETCH NEXT %d ROWS ONLY" % (
            sf_offset, sf_chunk_size)
            dfs.append(pd.read_sql(sf_query, ctx_snowflake))
            sf_offset += sf_chunk_size

            cs_snowflake.execute("alter session set timezone = 'America/New_York'")
            cs_snowflake.execute(sf_query)
            chunk_file = pd.DataFrame(cs_snowflake.fetchall())

            Output_path = Validation_path + table_name.upper() + "_SF_" + str(sf_chunk_files_no) + ".csv"
            sf_filename = "Data_Validation/" + db + "/" + table_name.upper() + '/' + table_name.upper() + "_SF_" + str(
                sf_chunk_files_no) + ".csv"
            sf_file = chunk_file.to_csv(Output_path, index=False, header=False, sep="|",date_format="%Y-%m-%d %H:%M:%S")
            s3.upload_file(Output_path, AWS_BUCKET_NAME, sf_filename)
            print(sf_filename + " file uploaded to s3")
            if len(dfs[-1]) < sf_chunk_size:
                break
            sf_chunk_files_no += 1
        print("Total number of chunk files for snowflake table " + table_name.upper() + " is/are %d" % (sf_chunk_files_no))
        logger.info("Total number of chunk files for snowflake table " + table_name.upper() + " is/are %d" % (sf_chunk_files_no))


os.system('python Module_05_Data_Validation.py '+db)
end = time.time()
######Calculating and Printing them in log and console, if time is above 60secs output will be in minutes else will give in seconds########
if((end-start)<60):
    Total_time = round(end-start,3)
    print("Total_time to export "+db+" to S3 and Data Validation is %f seconds"%(Total_time))
    logger.info("Total_time to export "+db+" to S3 and Data Validation is %f seconds"%(Total_time))
else:
    Total_time = round((end-start)/60,3)
    print("Total_time to export " + db + " to S3 and Data Validation is %f minutes" % (Total_time))
    logger.info("Total_time to export " + db + " to S3 and Data Validation is %f minutes" % (Total_time))


