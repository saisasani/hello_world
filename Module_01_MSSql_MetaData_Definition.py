#Python Library Import
import pandas as pd
import os
import errno
import logging
from datetime import datetime
from configparser import ConfigParser
import db_connect

""" Version 
Name           : Module_01_MSSql_MetaData_Definition
Description    : Extract Table Structure form MS SQL server and Generate DDL Script for Snowflake
Date           : 05/20/2019
Authors        : Kuldeep Sharma, Sai Aindla
Version        : Initial 1.0
"""


# Input Parameters Setup - mention database name in param.ini file
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

# Logging Section
dyn_dir = datetime.today().strftime('%Y_%m_%d_%H')
inner_dir = str(parameters['output_path']) + "Logs\\"+dyn_dir+".log"

if not os.path.exists(os.path.dirname(inner_dir)):
    try:
        os.makedirs(os.path.dirname(inner_dir))
    except OSError as exc:  # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

logger = logging.getLogger('root')
logger.setLevel(logging.INFO)

# create a file handler
handler = logging.FileHandler(inner_dir)
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('[%(asctime)s] %(filename)s:%(lineno)d} %(levelname)s - %(message)s', "%m-%d-%Y %H:%M%p")
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)


# Database connect setup - mention database credentials in db_connect.py
try:
    # exec(open("C:/Users/Sai.Aindla/PycharmProjects/Snowflake/Migration_SingleDatabase/config/db_connect.py").read())
    sql_conn = db_connect.connect_sql()
    logger.info("MsSql Database Connection Success")
except:
    logger.critical("MsSql Database Connection Failure")


# Confiure output path to store DDL Scripts
ddl_script_path = str(parameters['output_path'])
sql_table_catalog_list = str(parameters['database_list'].split(",")).replace("[","").replace("]","")

print(sql_table_catalog_list)

db_list = """SELECT DISTINCT (NAME)TABLE_CATALOG FROM sys.databases WHERE NAME IN ("""+sql_table_catalog_list+") ORDER BY NAME"
dblist_df = pd.read_sql(db_list, sql_conn)



dblist = dblist_df['TABLE_CATALOG']

print(dblist)

ddl_create_table_file = ""



try:
    sf_conn = db_connect.connect_sf()
    logger.info("Snowflake Database Connection Success")
except:
    logger.critical("Snowflake Database Connection Failure")

SNOWFLAKE_DB = sf_conn['sf_database']

def constraints(db,table_name):
    query = """select table_view,
        object_type, 
        constraint_type,
        constraint_name,
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
    where table_view = '"""+ table_name +"""' 
    order by table_view, constraint_type, constraint_name"""

    db_sql_conn2 = db_connect.dblist_sql_conn(db)
    constraints = pd.read_sql(query, db_sql_conn2)
    # print(query)
    for i, j in constraints.iterrows():
        # print(j['constraint_type'],j['constraint_name'])
        constraint_type = j['constraint_type']
        constraint_name = j['constraint_name']
        details = j['details']
        table_name = j['table_view'].split(".")[1]
        sf_alter_query = "ALTER TABLE " + SNOWFLAKE_DB + "." + db + "." + table_name + " ADD CONSTRAINT " + constraint_name + " " + constraint_type + " (" + details + "); \n"
        # drop_constraint_query = "AlTER TABLE "+SNOWFLAKE_DB+"."+schema+"."+table_name+" DROP CONSTRAINT "+constraint_name
        # cs_snowflake.execute(sf_alter_query)
        # cs_snowflake.execute(drop_constraint_query)
        # print("Constraint " + constraint_name + " added to table " + table_name)
        return sf_alter_query


# Looping Database one by one which passed as in param file
# try:
for db in dblist:
    try:
        db_sql_conn = db_connect.dblist_sql_conn(db)
    except:
        logger.critical(db+" Connection Failed")
    table_list = """SELECT DISTINCT a.TABLE_SCHEMA,a.TABLE_NAME
        FROM INFORMATION_SCHEMA.COLUMNS a
        LEFT OUTER JOIN INFORMATION_SCHEMA.VIEWS b ON a.TABLE_CATALOG = b.TABLE_CATALOG
        AND a.TABLE_SCHEMA = b.TABLE_SCHEMA
        AND a.TABLE_NAME = b.TABLE_NAME WHERE CASE WHEN b.TABLE_NAME is not null then 'view' else 'table' end='table'
        AND a.TABLE_CATALOG = """ + "'" + db + "'"

    tbllist_df = pd.read_sql(table_list, db_sql_conn)

    tbllist = tbllist_df['TABLE_NAME'].unique()

    #FileName for All table in one file
    cons_filename = ddl_script_path+"""Snowflake_"""+SNOWFLAKE_DB+"""_Create_Table_Script_All_""" + db + '.sql'
    with open(cons_filename, 'w') as file:
        #Looping table one by one for create structure from Information Schema
        logger.info("Database DDL Started for "+db)
        print("Database DDL Started for " + db)
        for schema, table in tbllist_df.iterrows():
            table_structure = []
            schema_and_table = (table[0] + "." + table[1])
            query = """SELECT DISTINCT C.TABLE_CATALOG,
                C.TABLE_SCHEMA,
                C.TABLE_NAME,
                CASE WHEN CAST(C.COLUMN_NAME AS VARCHAR(100))='table'
                     THEN 'tables'
                     WHEN CAST(C.COLUMN_NAME AS VARCHAR(100))='group'
                     THEN 'groups'
                     WHEN CAST(C.COLUMN_NAME AS VARCHAR(100))='from'
                     THEN 'from_medium'
                     WHEN CAST(C.COLUMN_NAME AS VARCHAR(100))='row'
                     THEN 'row_no'
                     ELSE CAST(C.COLUMN_NAME AS VARCHAR(100))
                END AS COLUMN_NAME,
                CASE WHEN C.DATA_TYPE = 'bit' THEN 'tinyint'
                        WHEN C.DATA_TYPE = 'smalldatetime' THEN 'timestamp_ltz'
                        WHEN C.DATA_TYPE = 'money' THEN 'decimal'
                        WHEN C.DATA_TYPE = 'datetime2' THEN 'timestamp_ltz'
                ELSE C.DATA_TYPE END AS DATA_TYPE,
                CASE WHEN CAST(C.CHARACTER_MAXIMUM_LENGTH AS VARCHAR(10)) IS NULL THEN
                (CASE WHEN C.NUMERIC_PRECISION IS NOT NULL AND C.DATA_TYPE in ('decimal','numeric','money')
                THEN (CAST(C.NUMERIC_PRECISION AS VARCHAR(5))+','+CAST(C.NUMERIC_SCALE AS VARCHAR(5)))
                    WHEN C.NUMERIC_PRECISION IS NOT NULL AND C.DATA_TYPE IN ('int','smallint','tinyint','integer','float','bigint')
                    THEN ''
                    WHEN C.DATA_TYPE like '%date%'
                    THEN ''
                    WHEN C.DATA_TYPE = 'time'
                    THEN ''
                    WHEN C.DATA_TYPE = 'bit'
                    THEN ''
                    ELSE CAST(C.NUMERIC_PRECISION AS VARCHAR(5)) END)
                    WHEN CAST(C.CHARACTER_MAXIMUM_LENGTH AS VARCHAR(10)) IS NULL THEN ''
                    WHEN C.DATA_TYPE = 'text' THEN ''
                    ELSE CAST(C.CHARACTER_MAXIMUM_LENGTH AS VARCHAR(10)) END
                    AS CHARACTER_MAXIMUM_LENGTH,
                CASE WHEN LTRIM(RTRIM(C.IS_NULLABLE))='YES' THEN 'NULL' ELSE 'NOT NULL' END  IS_NULLABLE,
                CAST(C.CHARACTER_MAXIMUM_LENGTH AS VARCHAR(10)) AS CHARACTER_MAXIMUM_LENGTH_ORG,
                C.NUMERIC_PRECISION,
                C.NUMERIC_SCALE,
                C.ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS AS C
                OUTER APPLY (
                    SELECT CCU.CONSTRAINT_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
                    JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU
                    ON CCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME
                    WHERE TC.TABLE_SCHEMA = C.TABLE_SCHEMA
                    AND   TC.TABLE_NAME   = C.TABLE_NAME
                    AND   CCU.COLUMN_NAME = C.COLUMN_NAME
                    ) AS Z
                LEFT OUTER JOIN INFORMATION_SCHEMA.VIEWS b ON C.TABLE_CATALOG = b.TABLE_CATALOG
                AND C.TABLE_SCHEMA = b.TABLE_SCHEMA
                AND C.TABLE_NAME = b.TABLE_NAME
                WHERE CASE WHEN b.TABLE_NAME is not null then 'view' else 'table' end='table'
                AND      C.TABLE_CATALOG = """ + "'" + db + "'" + " AND " + "C.TABLE_SCHEMA=" + "'" + table[
                0] + "'" + " AND C.TABLE_NAME = " + "'" + table[1] + "' ORDER BY C.ORDINAL_POSITION "


            logger.info("MetaData Created successfully for Table " + table[1])
            df = pd.read_sql(query, db_sql_conn)
            col_df = []
            #Logic for create table script output table by table in respective Database folder
            for i, j in df.iterrows():
                column_setup = j[3] + " " + j[4] + "(" + j[5] + ") " + j[6] + ",\n"
                col_df.append(column_setup)

                ddl_create = "CREATE OR REPLACE TABLE " + j[0] + '.' + table[1] + "\n( " + ''.join(col_df) + "); \n"

                ddl_constraint = constraints(db,"dbo."+table[1])
                #print(table[1])

                ddl_create_table = ddl_create.replace("()", " ").replace(",)", ")").replace(",\n);", "\n );")
                ddl_replace_keyword = ddl_create_table.replace("Order (", '"Order" (')+ "\n" + str(ddl_constraint)
                ddl_create_table_file = ddl_replace_keyword
                # print(ddl_replace_keyword)
                filename = ddl_script_path+"\\"+SNOWFLAKE_DB+"""_Scripts\\""" + j[0] + '\\' + table[
                    1] + '.sql'
                if not os.path.exists(os.path.dirname(filename)):
                    try:
                        os.makedirs(os.path.dirname(filename))
                    except OSError as exc:  # Guard against race condition
                        if exc.errno != errno.EEXIST:
                            raise
                with open(filename, "w") as f:
                    f.write(ddl_replace_keyword)
            file.write(ddl_create_table_file)
        logger.info("Database DDL Completed for "+db)
        print("Database DDL Completed for "+db)
# except:
#     logger.error("MsSql DDL Script Generation Failed")
#Write the file in sql



file.close()

sql_conn.close()
