# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import upper
from snowflake.snowpark.types import  StructType, StructField, IntegerType, StringType, BooleanType, FloatType, TimestampType, DateType
from numpy import nan
import pandas as pd


dest_database = 'BUSDATA'
dest_schema = 'SF'
object_name = 'Account'

def elt_s3_file(session, table_name, stage_path, object_element_file,file_to_load,table_name_ext):
    
    
    #read data from table
    df_table_snowflake = session.sql(f"""SELECT * FROM {table_name} Limit 1""")

    # read object elemets file
    
    #load object_elements file
    # try:
    #     #object elements sa samo DVE KOLONE
    #     # read object elemets file
        
    #     #load object_elements file
    #     schema_templ = StructType([
    #     StructField("NAME", StringType()),	
    #     StructField("SOAPTYPE", StringType())])
        
    #     df_obj_elements = session.read.schema(schema_templ)\
    #         .option("RECORD_DELIMITER","\n")\
    #         .option("FIELD_DELIMITER", "\t")\
    #         .option('SKIP_HEADER',1)\
    #             .csv(object_element_file)
    

    # except:
    schema_templ = StructType([StructField("LABEL", StringType()), 
    StructField("NAME", StringType()),	
    StructField("SOAPTYPE", StringType()),
    StructField("BYTELENGTH",	StringType()),
    StructField("AS_OF_TIMESTAMP", StringType())])
    
    df_obj_elements = session.read.schema(schema_templ)\
        .option("RECORD_DELIMITER","\n")\
        .option("FIELD_DELIMITER", "\t")\
        .option('SKIP_HEADER',1)\
            .csv(object_element_file)
    
        
    # Extract unique values from the "name" column
    unique_names = [row.NAME for row in df_obj_elements.select("name").collect()]
    unique_soaptypes = [row.SOAPTYPE for row in df_obj_elements.select("soaptype").collect()]
    unique_columns_type_dict = dict(zip(unique_names,unique_soaptypes))

    schema_fields=[]
    
    # TODO: LAMBDA IZBACUJE DATE I DATETIME KAO OBJECT, ZATO CE KREIRATI NOVE KOLONE KAO STRING, POGLEDAJ NEKI WORKAROUND
    # loop throug all rows, get names of columns, add missing columns in both snowflake table and csv file, 
    pd_to_snowflake_dtypes = {'ID':'VARCHAR','Id':'VARCHAR','bool':'BOOLEAN','boolean':'BOOLEAN','object':'STRING','string':'STRING', 'float64':'DOUBLE','double':'DOUBLE','address':'VARCHAR','int':'INT','dateTime':'TIMESTAMP_TZ(9)', 'date':'DATE','location':'STRING'}
    
    
    df_obj_elements_coll = df_obj_elements.collect()

    # for row in df_obj_elements_coll:
    for key in unique_columns_type_dict:
        # #get column name and dtype
        column_name = key.replace('"','') #row['NAME']   
        # print(key)
        column_dtype_pd = unique_columns_type_dict[key].replace('"','')  #row['SOAPTYPE'] #we need this so w could add columns

        # print(column_name, column_dtype_pd, sep=', ')
        if column_dtype_pd==None:            
            column_dtype_pd='string'
            
        column_dtype_snowflake = pd_to_snowflake_dtypes[column_dtype_pd]
        

        #for test purposes, all dtypes are StringType()
        col_dtype = StringType()
        
        # # IntegerType, StringType, BooleanType, FloatType, TimestampType, DateType
        # if (column_dtype_snowflake == 'VARCHAR') | (column_dtype_snowflake == 'STRING'):
        #     col_dtype = StringType()
        # elif column_dtype_snowflake=='DOUBLE':
        #     col_dtype = FloatType()
        # elif column_dtype_snowflake=='INT':
        #     col_dtype = IntegerType()
        # elif column_dtype_snowflake=='TIMESTAMP_NTZ(9)':
        #     col_dtype = TimestampType()           
        # elif column_dtype_snowflake=='DATE':
        #     col_dtype = DateType()                   
        # elif column_dtype_snowflake=='BOOLEAN':
        #     col_dtype = BooleanType()          

        schema_fields.append(StructField(column_name, col_dtype))
        # print(column_name,column_dtype_snowflake,col_dtype)  
    
    # print(schema_fields)
    #TEMP - just for account, untill I fix things in Lambda
    # #append recordtype and as_of_timestamp
    # if ('RecordType' not in unique_names) | ('AccountRecordType' not in unique_names) | ('RECORDTYPE' not in unique_names):
    #     schema_fields.append(StructField('RecordType', StringType())) #StringType VariantType
    
    # if 'as_of_timestamp' not in unique_names:
    #     schema_fields.append(StructField('as_of_timestamp', StringType())) #TimestampType()
    
    dynamic_schema = StructType(schema_fields)

    #load CSV file
    df_csv = session.read.schema(dynamic_schema).option("format_name","CSV_TAB_FILE_FORMAT").csv(file_to_load) 

    
    #add columns that might be missing in snowflake table but are present in csv file
    for col_name in df_csv.columns:
        if col_name not in df_table_snowflake.columns:
            print(f'Adding columns name: {col_name}, table name: {table_name}, column dtype: {column_dtype_snowflake} column to database.')
            alter_table_sql =f"""ALTER TABLE {table_name}
                    ADD  {col_name} {column_dtype_snowflake}"""
            # run query
            session.sql(alter_table_sql).collect()
    
            print(f'{col_name} column added to database.')
    
    from snowflake.snowpark.functions import lit 
    # print(df_table_snowflake.columns)

    #read data from table
    df_acc_table_snowflake = session.sql(f"""SELECT * FROM {table_name} Limit 1""")
    db_dtype_dict = dict(df_acc_table_snowflake.dtypes)
    print(db_dtype_dict)
    
    #add columns from database to dataframe generated from csv that do not exist
    for col_name in df_table_snowflake.columns:
        # print(col_name)
        if col_name not in df_csv.columns:
            print(f'{col_name} added to dataframe')
            # try:
            #     df_csv = df_csv.with_column(col_name, lit(nan))
            # except:
            #     df_csv = df_csv.with_column(col_name, lit(pd.NaT))
            if db_dtype_dict[col_name]=='timestamp':
                # df_csv = df_csv.with_column(col_name, lit(pd.NaT))
                df_csv = df_csv.withColumn(col_name, lit(None).cast(TimestampType()))
            else:
                df_csv = df_csv.with_column(col_name, lit(nan))
    
    #requery again to get column layout and reorder dataframe loade from csv


    
    # rearange columns in csv file at the ned
    df_csv = df_csv[list(df_acc_table_snowflake.columns)]
    
    # load csv fil to snowflake table
    

    df_csv = df_csv.replace('None',None)
    print('printing dtypes')
    print(df_csv.dtypes)
    print(df_acc_table_snowflake.dtypes)
    
    # # df_csv.show(20)
    df_table_final = df_csv.write.mode('append').save_as_table(table_name_ext)
    # # #df_table_final = df_csv.write.mode('append').save_as_table('test_acc')
    

    # Return value will appear in the Results tab.
    return df_csv #.select(col('Next_Renewal_Date__c')).dropDuplicates()
    

def main(session,dest_database,dest_schema,object_name): 
    # Your code goes here, inside the "main" handler.
    # tableName = 'information_schema.packages'
    # dataframe = session.table(tableName).filter(col("language") == 'python')

    # # Print a sample of the dataframe to standard output.
    # dataframe.show()
    ###################
    print('Use')
    session.sql(f'USE database {dest_database}')
    session.sql(f'USE schema {dest_schema}')

    
    #Get stage name and name of file with latest timestamp
    table_name = object_name.upper()
    table_path = f'{dest_database}.{dest_schema}'
    table_name_ext = f'{table_path}.{table_name}'
    
    #GET STAGE NAME USED TO QUERY OBJECT
    df_stages = session.sql('SHOW stages').collect()
    
    # Filter stages that match the pattern *_{table_name}
    filtered_stages = [row.name for row in df_stages if row.name and row.name.endswith(f'_{table_name}')]
    print(filtered_stages)
    stage_name = filtered_stages[0]
    print(stage_name)
    stage_path = f'@{table_path}.{stage_name}/'
    object_element_file = f'{stage_path}{object_name}_-_object_elements.csv'
    
    
    #get name of latest updated file
    df_stage = session.sql(f'LIST @{stage_name}').collect()
    print('get stage')
    max_date = pd.to_datetime('1900-01-01 00:00:00')
    full_file_name = ''
    
    for row in range(0,len(df_stage)):
        temp_name = df_stage[row]['name'].split("/")[-1]
        timestamp_temp = pd.to_datetime(df_stage[row]['last_modified'])

        if (temp_name!=f'{object_name}_-_object_elements.csv') \
            and (timestamp_temp>max_date) \
            and df_stage[row]['name'].split('.')[1]=='csv':
    
            
            max_date = timestamp_temp
            full_file_name = temp_name
    
    file_to_load = f'{stage_path}{full_file_name.split("/")[-1]}'

    ##
    print('loading file: ',file_to_load,', stage_path:',stage_path)

    dataframe = elt_s3_file(session, table_name, stage_path, object_element_file,file_to_load,table_name_ext)

    # Return value will appear in the Results tab.
    return dataframe


def test_main(session: snowpark.Session):
    return main(session,dest_database,dest_schema,object_name)
