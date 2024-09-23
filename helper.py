import sys
from datetime import datetime

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

# Initialize Spark session and boto3 client
spark = SparkSession.builder.appName("GlueIcebergMigration").getOrCreate()
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PROD_BUCKET', 'ENTITY'])
s3 = boto3.client('s3')
glue = boto3.client('glue')


def list_s3_folders(bucket, prefix, start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    paginator = s3.get_paginator('list_objects_v2')
    result_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')

    folders = []
    for page in result_iterator:
        for obj in page.get('CommonPrefixes', []):
            folder = obj.get('Prefix')
            folder_date_str = folder.split('date=')[1].split('/')[0]
            folder_date = datetime.strptime(folder_date_str, '%Y-%m-%d')
            if start_date <= folder_date <= end_date:
                folders.append(folder)
    return folders


# Function to alter Glue table and add partitions using Spark SQL
def add_partitions_with_sql(bucket_name, folders, glue_table, glue_db):
    for folder in folders:
        entity = folder.split('entity=')[1].split('/')[0]
        date = folder.split('date=')[1].split('/')[0]
        value = folder.split('value=')[1].split('/')[0]

        # Construct the S3 path and the SQL command
        s3_path = f"s3://{bucket_name}/{folder}"
        sql_cmd = f"""
        ALTER TABLE {glue_db}.{glue_table}
        ADD IF NOT EXISTS PARTITION (entity='{entity}', date='{date}', value='{value}')
        LOCATION '{s3_path}'
        """

        # Execute the SQL command
        spark.sql(sql_cmd)


# Function to query data from the Glue table and overwrite another table
def query_and_overwrite(glue_table, iceberg_table, start_date, end_date):
    sql_query = f"""
    SELECT * FROM {glue_table}
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """

    result_df = spark.sql(sql_query)
    result_df.writeTo(iceberg_table).overwritePartitions()


def get_all_tables(glue_db):
    response = glue.get_tables(DatabaseName=glue_db)
    table_names = [table['Name'] for table in response['TableList']]

    # Pagination handling if there are more tables
    while 'NextToken' in response:
        response = glue.get_tables(DatabaseName=glue_db, NextToken=response['NextToken'])
        table_names.extend([table['Name'] for table in response['TableList']])

    return table_names


def get_table_glue_ddl(glue, glue_db, table_name, glue_base_path):
    response = glue.get_table(DatabaseName=glue_db, Name=table_name)
    columns = response['Table']['StorageDescriptor']['Columns']
    partition_keys = response['Table']['PartitionKeys']

    glue_table_location = f"{glue_base_path}/{table_name}"

    # Build the DDL string
    ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS{glue_db}.{table_name} ("
    ddl += ", ".join([f"{col['Name']} {col['Type']}" for col in columns])

    if partition_keys:
        ddl += ") PARTITIONED BY ("
        ddl += ", ".join([f"{pk['Name']} {pk['Type']}" for pk in partition_keys])

    ddl += f") USING parquet LOCATION '{glue_table_location}'"
    return ddl


def create_iceberg_table(glue_client, glue_db, table_name, iceberg_table_location, partition_keys):
    response = glue.get_table(DatabaseName=glue_db, Name=table_name)
    columns = response['Table']['StorageDescriptor']['Columns']

    storage_descriptor = {
        'Columns': columns,
        'Location': iceberg_table_location}

    # Define the table properties
    table_properties = {
        'table_type': 'iceberg',
        'classification': 'iceberg',
        'write_compression': 'snappy',
        'write.format.default': 'parquet'
    }

    # Define the table definition
    table_definition = {
        'TableInput': {
            'Name': table_name,
            'DatabaseName': glue_db,
            'Owner': 'owner_name',
            'StorageDescriptor': storage_descriptor,
            'PartitionKeys': partition_keys,
            'Parameters': table_properties
        }
    }

    try:
        glue_client.create_table(DatabaseName=glue_db, TableInput=table_definition['TableInput'])
        print(f"Successfully created Iceberg table {table_name} in database {glue_db}.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Table {table_name} already exists in database {glue_db}. Updating the existing table.")
        glue_client.update_table(DatabaseName=glue_db, TableInput=table_definition['TableInput'])
    except Exception as e:
        print(f"Error creating or updating Iceberg table {table_name}: {str(e)}")


def execute_spark_sql(ddl):
    spark.sql(ddl)


def main():
    bucket_name = args["PROD_BUCKET"]
    entity = args["ENTITY"]
    start_date = args["START_DATE"]
    end_date = args["END_DATE"]
    glue_table = args["GLUE_TABLES"] + "_" + entity
    prefix = ""
    glue_db = args["GLUE_DB"]
    iceberg_base_path = args["ICEBERG_PATH"]
    glue_base_path = args["GLUE_BASE_PATH"]

    glue_client = boto3.client('glue')

    glue_ddl = get_table_glue_ddl(glue_client, glue_db, glue_table, glue_base_path)
    print(f"Creating Glue table for {glue_table} with DDL:\n{glue_ddl}")
    execute_spark_sql(glue_ddl)

    folders = list_s3_folders(bucket_name, prefix, start_date, end_date)
    add_partitions_with_sql(bucket_name, folders, glue_table, glue_db)

    create_iceberg_table(glue_client, glue_db, glue_table, iceberg_base_path, "")

    alter_properties_Sql = f"alter table {glue_db}.iceberg_{glue_table}"
    execute_spark_sql(alter_properties_Sql)

    query_and_overwrite(f"{glue_db}.{glue_table}", f"{glue_db}.iceberg_{glue_table}", start_date, end_date)


main()
