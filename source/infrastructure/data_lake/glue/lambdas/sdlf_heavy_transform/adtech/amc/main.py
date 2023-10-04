# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import sys
import awswrangler as wr
import boto3
from botocore.config import Config
import pandas as pd
import numpy as np
from awsglue.utils import getResolvedOptions
import io
import re
import unicodedata
from pandas.api.types import is_numeric_dtype, is_string_dtype
from typing import Tuple
from aws_lambda_powertools import Logger

# create logger
logger = Logger(service="Glue job for AMC dataset", level='INFO', utc=True)

solution_args = getResolvedOptions(sys.argv, ['SOLUTION_ID', 'SOLUTION_VERSION', 'RESOURCE_PREFIX', 'METRICS_NAMESPACE'])
solution_id = solution_args['SOLUTION_ID']
solution_version = solution_args['SOLUTION_VERSION']
resource_prefix = solution_args['RESOURCE_PREFIX']
METRICS_NAMESPACE = solution_args['METRICS_NAMESPACE']


def get_service_client(service_name):
    botocore_config_defaults = {"user_agent_extra": f"AwsSolution/{solution_id}/{solution_version}"}
    amci_boto3_config = Config(**botocore_config_defaults)
    return boto3.client(service_name, config=amci_boto3_config)


def get_service_resource(service_name):
    botocore_config_defaults = {"user_agent_extra": f"AwsSolution/{solution_id}/{solution_version}"}
    amci_boto3_config = Config(**botocore_config_defaults)
    return boto3.resource(service_name, config=amci_boto3_config)

print('boto3 version')
print(boto3.__version__)

glue_client = get_service_client('glue')
s3_resource = get_service_resource('s3')
s3_client = get_service_client('s3')
lf_client = get_service_client('lakeformation')

# This map is used to convert Athena datatypes (in upppercase) to pandas Datatypes
data_type_map = {
    "ARRAY": object
    , "BIGINT": np.int64
    , "BINARY": object
    , "BOOLEAN": bool
    , "CHAR": str
    , "DATE": object
    , "DECIMAL": np.float64
    , "DOUBLE": np.float64
    , "FLOAT": np.float64
    , "INTEGER": np.int64
    , "INT": np.int64
    , "MAP": object
    , "SMALLINT": np.int64
    , "STRING": str
    , "STRUCT": object
    , "TIMESTAMP": np.datetime64
    , "TINYINT": np.int64
    , "VARCHAR": str}

pandas_athena_datatypes = {
    "float64": "double",
    "float32": "float",
    "datetime64[ns]": "timestamp",
    "int64": "bigint",
    "int8": "tinyint",
    "int16": "smallint",
    "int32": "int",
    "bool": "boolean"
}

BooleanValueMap = {"false": 0, "False": 0, "FALSE": 0,
                   "true": 1, "True": 1, "TRUE": 1, "-1": 0, -1: 0}

column_datatype_override = {
    ".*_fee[s]*($|_.*)": np.float64,
    "cost[s]*$|.*_cost[s]*($|_.*)|.*_cost[s]*_.*$": np.float64,
    ".*_rate[s]*($|_.*)": np.float64,
    ".*_score[s]*($|_.*)": np.float64,
    ".*_percent[s]*($|_.*)": np.float64,
    ".*_pct[s]*($|_.*)": np.float64,
    "avg($|_.*)|.*_avg[s]*($|_.*)": np.float64,
    "[e]*cpm$|.*_[e]*cpm$|.*_[e]*cpm_.*$": np.float64,
    ".*_id$": str
}

exclude_workflow = ['standard_impressions_by_browser_family', 'standard_impressions-by-browser-family']

def get_bucket_and_key_from_s3_uri(s3_path: str):
    output_bucket, output_key = re.match('s3://([^/]*)/(.*)', s3_path).groups()
    return output_bucket, output_key

def athena_sanitize_name(name: str) -> str:
    name = "".join(c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn")  # strip accents
    return re.sub("\W+", "_", name).lower()  # Replacing non alphanumeric characters by underscore

def add_partitions(outputfilebasepath, silver_catalog, list_partns, target_table_name):
    partn_values = []
    patn_path_value = outputfilebasepath
    for prtns in list_partns:
        patn_path_value = patn_path_value + prtns["orgcolnm"] + "=" + str(prtns["value"]) + "/"
        partn_values.append(str(prtns["value"]))
    print("Partition S3 Path : " + patn_path_value)
    print(str(partn_values))
    try:
        print("Update partitions")
        wr.catalog.add_parquet_partitions(
            database=silver_catalog,
            table=wr.catalog.sanitize_table_name(target_table_name),
            compression='snappy',
            partitions_values={
                patn_path_value: partn_values
            }
        )
    except Exception as e:
        print("Partition exist No need to update")
        print(str(e))


def get_partition_values(source_file_partitioned_path):
    list_partns = []
    cust_hash = ''
    for prtns in source_file_partitioned_path.split('/'):
        if '=' in prtns:
            d = {
                "orgcolnm": str(prtns.split("=")[0]),
                "santcolnm": wr.catalog.sanitize_column_name(str(prtns.split("=")[0])),
                "value": str(prtns.split("=")[1])
            }
            list_partns.append(d)
        if 'customer_hash' in prtns:
            cust_hash = str(prtns.split("=")[1])

    return list_partns, cust_hash

def add_tags_lf(cust_hash_tag_dict, database_name, table_name):
    try:
        get_tag_dtl = lf_client.get_lf_tag(
            TagKey=list(cust_hash_tag_dict.keys())[0]
        )
        print("Existing Tag details :" + list(cust_hash_tag_dict.keys())[0] + " : " + str(get_tag_dtl))
        tag_values = set(get_tag_dtl['TagValues'])
        tag_values.add(list(cust_hash_tag_dict.values())[0])
        print("New Tag values")
        print(tag_values)

        try:
            upd_tag_dtl = lf_client.update_lf_tag(
                TagKey=list(cust_hash_tag_dict.keys())[0],
                TagValuesToAdd=list(tag_values)
            )
            print("Updating existing Tag details :" + list(cust_hash_tag_dict.keys())[0] + " : " + str(upd_tag_dtl))
        except Exception as e:
            print("Exception while updating existing tags : " + str(e))


    except Exception as e:
        print("Exception while retrieving tag details : " + str(e))
        creat_tag_dtl = lf_client.create_lf_tag(
            TagKey=list(cust_hash_tag_dict.keys())[0],
            TagValues=[
                list(cust_hash_tag_dict.values())[0],
            ]
        )
        print("Creating Tag details :" + list(cust_hash_tag_dict.keys())[0] + " : " + str(creat_tag_dtl))

    try:
        add_tag_tbl = lf_client.add_lf_tags_to_resource(
            Resource={
                'Table': {
                    'DatabaseName': database_name,
                    'Name': table_name

                }
            },
            LFTags=[
                {
                    'TagKey': list(cust_hash_tag_dict.keys())[0],
                    'TagValues': [
                        list(cust_hash_tag_dict.values())[0]
                    ]
                },
            ]
        )
        print("Adding tags to table :" + list(cust_hash_tag_dict.keys())[0] + " : " + str(add_tag_tbl))
    except Exception as e:
        print("Exception while adding tags to tables : " + str(e))


def create_update_tbl(csvdf, csv_schema, tbl_schema, silver_catalog, target_table_name, list_partns, outputfilebasepath,
                      table_exist, cust_hash, pandas_athena_datatypes):
    if table_exist == 1:
        extra_cols = list(set(csv_schema.keys()) - set(tbl_schema.keys()))
        print("extra_cols : " + str(extra_cols))

        tbl = glue_client.get_table(
            DatabaseName=silver_catalog,
            Name=wr.catalog.sanitize_table_name(target_table_name)
        )
        print("Existing table")
        print(tbl)

        strg_descrptr = tbl["Table"]["StorageDescriptor"]
        new_cols = []
        if len(extra_cols) > 0:
            print("Adding new columns")
            for col in extra_cols:
                print("New col name : " + col)
                print("New col type : " + pandas_athena_datatypes.get(csv_schema[col].lower(), 'string'))
                col_dict = {}
                col_dict = {
                    'Name': col,
                    'Type': pandas_athena_datatypes.get(csv_schema[col].lower(), 'string')
                }
                new_cols.append(col_dict)

            strg_descrptr["Columns"].extend(new_cols)

            newtbldetails = {
                'Name': wr.catalog.sanitize_table_name(target_table_name),
                'StorageDescriptor': strg_descrptr,
                'PartitionKeys': tbl["Table"]["PartitionKeys"],
                'TableType': tbl["Table"]["TableType"],
                'Parameters': tbl["Table"]["Parameters"]
            }

            print("new table defn")
            print(newtbldetails)

            resp = glue_client.update_table(
                DatabaseName=silver_catalog,
                TableInput=newtbldetails
            )

            print("new table")
            print(resp)
        else:
            print("No change in table")

    else:
        print("Table does not exist. Creating new table")
        col_dict = {}
        cust_hash_tag_dict = {
            'customer_hash': cust_hash
        }

        for colm in csvdf.columns:
            col_dict[colm] = str(pandas_athena_datatypes.get(str(csvdf.dtypes[colm]).lower(), 'string'))
        part_dict = {}
        for prtns in list_partns:
            part_dict[prtns["santcolnm"]] = 'string'

        print("Create Table")
        print("Column Dictionary : " + str(col_dict))
        print("Partition Dictionary : " + str(part_dict))

        wr.catalog.create_parquet_table(
            database=silver_catalog,
            table=wr.catalog.sanitize_table_name(target_table_name),
            path=outputfilebasepath,
            columns_types=col_dict,
            partitions_types=part_dict,
            compression='snappy',
            parameters=cust_hash_tag_dict
        )

def check_filtered_row(csvdf):
    csvdf_filtered_rows = csvdf[csvdf.filtered.str.lower() == "true"]
    # Update the dataset to only include non filtered rows
    csvdf_only_unfiltered_rows = csvdf[csvdf.filtered.str.lower() != "true"]

    # create an out buffer to capture the CSV file output from the to_csv method
    csv_only_unfiltered_out_buffer = io.BytesIO()

    # write the filtered record set as a CSV into the buffer
    csvdf_only_unfiltered_rows.to_csv(csv_only_unfiltered_out_buffer)

    # Reset the buffer location after it is written to
    csv_only_unfiltered_out_buffer.seek(0)

    # create copy of the raw string data with only non filtered rows
    only_unfiltered_csv_file_data = io.StringIO(csv_only_unfiltered_out_buffer.read().decode("UTF8"))

    # reset the string buffer position after it has been written to
    only_unfiltered_csv_file_data.seek(0)

    # close the bytes buffer
    csv_only_unfiltered_out_buffer.close()

    # Log the number of filtered and unfiltered rows
    logger.info(
        f"input data had {csvdf_filtered_rows.shape[0]} filtered rows and {csvdf_only_unfiltered_rows.shape[0]} unfiltered rows")

    # will be true if there is at least 1 row left in the df after filtered rows are removed
    return csvdf_only_unfiltered_rows.shape[0] > 0

def check_override_match(column, csvdf):
    # Check to see if the column matched an override suffix to force a datatype rather than deriving it
    override_matched = False
    for regex_expression_key in column_datatype_override:
        regex_match = re.match(regex_expression_key, column, re.IGNORECASE)
        if regex_match:
            override_datatype = column_datatype_override[regex_expression_key]
            if is_numeric_dtype(override_datatype):
                csvdf[column].fillna(-1, inplace=True)
            csvdf[column] = csvdf[column].astype(override_datatype)
            logger.info(
                f"column {column} matched override regex expression {regex_expression_key} and was casted to {override_datatype}")
            override_matched = True
    return override_matched

def cast_nonstring_column(only_nonstring_schema, csvdf, column):
    derived_datatype_name = only_nonstring_schema[column]
    # since we know the column is not a string, fill blanks with -1
    csvdf[column].fillna(-1, inplace=True)

    # if the nonstring column is boolean than map to 0 or 1 values before casting to boolean to ensure that
    # false, FALSE, and False end up as 0
    if only_nonstring_schema[column] == bool:
        try:
            logger.info(f"column {column} is derived as {derived_datatype_name}, "
                        f"performing boolean mapping and casting")
            csvdf[column] = csvdf[column].map(BooleanValueMap).astype('bool')
            return True
        except (TypeError, ValueError, KeyError) as e:
            logger.info(f'could not cast {column} as {derived_datatype_name} : {e}')

    # Check to see if the derived datatype is numeric
    if is_numeric_dtype(only_nonstring_schema[column]):
        # Convert any derived number columns to Int64 if possible
        try:
            csvdf[column] = csvdf[column].astype('int64')
            logger.info(f'casted {column} derived as {derived_datatype_name} to int64')
            # If we cast successfully then go to the next column
            return True
        except (TypeError, ValueError) as e:
            # Log if we are unable to cast to an int64 then note it in the log
            logger.info(
                f'could not cast {column} derived as {derived_datatype_name} to int64: {str(e)}')

    # Attempt to cast the text to the derived datatype
    try:
        csvdf[column] = csvdf[column].astype(only_nonstring_schema[column])
        logger.info(f'casted derived {column} as {derived_datatype_name}')
    except (TypeError, ValueError) as e:
        logger.info(f'could not cast {column} as {derived_datatype_name} : {e}')

    return False

def read_schema(target_table_name, silver_catalog, csv_schema, csvdf):
    table_schema = {}
    get_table_results = glue_client.get_table(DatabaseName=silver_catalog,
                                        Name=athena_sanitize_name(target_table_name))
    logger.info(f"getting schema for table {silver_catalog}.{athena_sanitize_name(target_table_name)}")

    for table_column in get_table_results['Table']['StorageDescriptor']['Columns']:
        table_schema[table_column['Name']] = table_column['Type']
        logger.info(f"table schema : {table_column['Name']} : {table_column['Type']}")

    for table_column in get_table_results['Table']['StorageDescriptor']['Columns']:
        table_schema[table_column['Name']] = table_column['Type']

    # copy the old csv schema to start the new schema
    new_schema = csv_schema.copy()

    # if the csv schema column matches an existing table schema column, update the csv schema to match match
    # the table's schema
    for column in new_schema:
        if column in table_schema:
            # look up the datatype from the table in our data_type_map (convert the datatype name to uppercase
            # first for the lookup matching)
            new_schema[column] = data_type_map[table_schema[column].upper()]

    logger.info(f'csvSchema:{csv_schema}')
    logger.info(f'newSchema:{new_schema}')

    # convert the CSV dataframe Schema to the new schema that was read from the glue table (if it exists)
    for c in csvdf.columns:
        if csvdf[c].dtype != new_schema[c]:
            logger.info(
                f'{c} datatype in file {csvdf[c].dtype} does not match datatype in table {new_schema[c]}')
            try:
                if new_schema[c] == np.int64:
                    csvdf[c].fillna(-1, inplace=True)
                    csvdf[c].replace('nan', -1, inplace=True)
                csvdf[c] = csvdf[c].astype(new_schema[c])
                logger.info(f'casted {c} as {new_schema[c]} to match table')
                print(f"dtype:{csvdf[c].dtype}")
                print(f"value is:{csvdf[c]}")
            except (TypeError, ValueError) as e:
                logger.info(f'could not cast {c} to {new_schema[c]} to match table: {str(e)}')

    return table_schema

def general_schema_conversion(target_table_name, silver_catalog, csvdf):
    logger.info(
        f'Caught exception, destination table {silver_catalog}.{target_table_name} does not exist, attempting to '
        f'cast all numbers to Int64 if possible')

    # If there are blanks in the data integers will be cast to floats which causes inconsistent parquet schema
    # Convert any numbers to Int64 (as opposed to int64) since Int64 can handle nulls
    for c in csvdf.select_dtypes(np.number).columns:
        try:
            csvdf[c] = csvdf[c].astype('Int64')
            logger.info(f'casted {c} as Int64')
        except (TypeError, ValueError) as e:
            logger.info(f'could not cast {c} to Int64: {str(e)}')

def check_column_type(column, only_string_schema, csvdf, only_nonstring_schema):

    # If the derived types for the ext column is not a nonstring then fill na with blank string (rather than -1)
    if column in only_string_schema:
        # Fill NA values for string type columns with empty strings
        # csvdf[column].fillna('', inplace=True)
        # Explicitly cast string type columns as string
        csvdf[column] = csvdf[column].astype(str)
        return True
    # if the column is derived as a nonstring then we need to try to cast it to the appropriate type with rules
    if column in only_nonstring_schema:
        if cast_nonstring_column(only_nonstring_schema=only_nonstring_schema,column=column,csvdf=csvdf):
            return True
    
    return False

def iterate_csvdf_cols(csvdf, only_string_schema, only_nonstring_schema):
    # Iterate over the text only columns
    for column in csvdf.columns:
        
        if check_override_match(column, csvdf):
            continue

        if check_column_type(column=column, only_string_schema=only_string_schema, csvdf=csvdf,only_nonstring_schema=only_nonstring_schema):
            continue


def process_files(source_locations, output_location, kms_key, silver_catalog):
    record_metric("SdlfHeavyTransformJob-num_files", len(source_locations))
    for key in source_locations:  # added for batching
        logger.info(f"Processing Key: {key}")  # added for batching
        source_location = key
        source_bucket, source_key = get_bucket_and_key_from_s3_uri(source_location)

        try:
            source_s3_object = s3_resource.Object(source_bucket, source_key).get()
            logger.info(f"metadata:{source_s3_object['Metadata']}")
        except Exception as e:
            print(f'Error: {e}')
            continue

        source_file_partitioned_path = source_s3_object['Metadata']['partitionedpath']
        source_file_base_name = source_s3_object['Metadata']['filebasename']
        source_file_workflow_name = source_s3_object['Metadata']['workflowname']

        target_table_name = source_file_partitioned_path.split('/')[0]

        if source_file_workflow_name in exclude_workflow:
            continue

        # Read the bytes of the csv file once so we can process it with pandas twice, only reading from S3 once.
        csv_file_data = io.StringIO(source_s3_object['Body'].read().decode("UTF8").replace('\\"', "'"))

        # create filtered copy of the data that will be used to derive the schema in case there is no filter fields
        only_unfiltered_csv_file_data = csv_file_data

        # reload the csv data forcing string (object) datatypes
        csvdf = pd.read_csv(csv_file_data, header=0, skip_blank_lines=True, escapechar='\\', dtype=np.dtype('O'))

        # you must reset the buffer location to the beginning after using it in a previous read
        csv_file_data.seek(0)

        # Create a boolean flag to track if file has any unfiltered rows
        has_unfiltered_rows = True

        # If the dataset has a column named filtered check to see how many rows are filtered
        if 'filtered' in csvdf.columns:
            has_unfiltered_rows = check_filtered_row(csvdf)

        if not has_unfiltered_rows:
            logger.info(f'There were no non-filtered rows in the data file, skipping file {key}')
            continue

        # Only use unfiltered text rows string buffer to derive the schema to try to get more accurate data types
        df_derived_schema = pd.read_csv(only_unfiltered_csv_file_data, header=0, skip_blank_lines=True, escapechar='\\')

        # close the buffers as we are now done with them
        only_unfiltered_csv_file_data.close()
        csv_file_data.close()

        # create a dictionary with the column name as the key and the datatype as the value
        derived_schema = dict(zip([*df_derived_schema.columns], [*df_derived_schema.dtypes]))

        # create a filtered copy of the dictionary only containing non string (object) dtypes columns that will need
        # to be cast
        only_nonstring_schema = dict(filter(lambda elem: elem[1] != np.dtype('O'), derived_schema.items()))

        logger.info(f"only_nonstring_schema : {only_nonstring_schema}")

        # create a filtered copy of the dictionary only containing non string (object) dtypes columns that will need
        # to be cast
        only_string_schema = dict(filter(lambda elem: is_string_dtype(elem[1]), derived_schema.items()))

        logger.info(f"only_string_schema : {only_string_schema}")

        iterate_csvdf_cols(csvdf=csvdf, only_string_schema=only_string_schema, only_nonstring_schema=only_nonstring_schema)

        s3_output_path = f'{output_location}/{source_file_partitioned_path}/{source_file_base_name}.parquet'

        # create a dictioary that contains the CSV file's casted schema
        csv_schema = dict(zip([*csvdf.columns], [*csvdf.dtypes]))

        # Try to read the schema from the destination table (if it exists) and convert the CSV inferrred schema to
        # match the table schema
        table_exist = 1
        table_schema = None
        try:
            table_schema = read_schema(target_table_name=target_table_name, silver_catalog=silver_catalog, csv_schema=csv_schema, csvdf=csvdf)

        # Catch the exception if the table does not exist and apply the generic logic to try to handle schema
        # conversions
        except glue_client.exceptions.EntityNotFoundException:
            general_schema_conversion(target_table_name=target_table_name, silver_catalog=silver_catalog, csvdf=csvdf)
            table_exist = 0

        logger.info(f'Converted Schema: {csvdf.dtypes}\n')
        logger.info(f'{len(csvdf)} records')

        # write the parquet file using the kms key
        # Note: if writing to parquet and not as a dataset must specify entire path name.
        out_buffer = io.BytesIO()

        csvdf.to_parquet(out_buffer, index=False, compression='snappy')

        output_bucket, output_key = get_bucket_and_key_from_s3_uri(s3_output_path)

        s3_client.put_object(Bucket=output_bucket, Key=output_key, Body=out_buffer.getvalue(),
                             ServerSideEncryption='aws:kms', SSEKMSKeyId=kms_key)

        logger.info(f'Successfully wrote output file to {s3_output_path}')

        # Collect metrics
        response = s3_client.head_object(Bucket=source_bucket, Key=source_key)
        bytes_read = response["ContentLength"]
        record_metric("SdlfHeavyTransformJob-bytes_read", bytes_read)
        response = s3_client.head_object(Bucket=output_bucket, Key=output_key)
        bytes_written = response["ContentLength"]
        record_metric("SdlfHeavyTransformJob-bytes_written", bytes_written)
        num_records = len(csvdf)
        record_metric("SdlfHeavyTransformJob-num_records", num_records)


        csv_schema = {}
        for colm in csvdf.columns:
            csv_schema[colm] = str(csvdf.dtypes[colm])
        print("Final CSV schema : " + str(csv_schema))
        print("Table Schema: " + str(table_schema))

        # get partition values
        list_partns = []
        cust_hash = ''
        list_partns, cust_hash = get_partition_values(source_file_partitioned_path)
        print("Partitions values : " + str(list_partns))

        outputfilebasepath = '{}/{}/'.format(output_location, target_table_name)

        # Create or update table
        create_update_tbl(csvdf, csv_schema, table_schema, silver_catalog, target_table_name, list_partns,
                          outputfilebasepath, table_exist, cust_hash, pandas_athena_datatypes)

        # add partitions
        add_partitions(outputfilebasepath, silver_catalog, list_partns, target_table_name)

def record_metric(metric_name, metric_value):
    logger.info(
        f"Recording metric {metric_name} and value {metric_value} in CloudWatch namespace " + METRICS_NAMESPACE
    )
    cloudwatch_client = get_service_client('cloudwatch')

    cloudwatch_client.put_metric_data(
        Namespace=METRICS_NAMESPACE,
        MetricData=[
            {
                'MetricName': metric_name,
                'Dimensions': [{'Name': 'stack-name', 'Value': resource_prefix}],
                'Value': metric_value,
                'Unit': 'Count'
            }
        ]
    )


if __name__ == '__main__':
    args = getResolvedOptions(
        sys.argv,
        ['JOB_NAME', 'SOURCE_LOCATION', 'SOURCE_LOCATIONS', 'OUTPUT_LOCATION', 'SILVER_CATALOG', 'GOLD_CATALOG',
         'KMS_KEY'])

    job_name = args['JOB_NAME']
    source_location = args['SOURCE_LOCATION']
    source_locations = args['SOURCE_LOCATIONS']
    source_locations = source_locations.split(',')
    output_location = args['OUTPUT_LOCATION']
    silver_catalog = args['SILVER_CATALOG']
    gold_catalog = args['GOLD_CATALOG']
    kms_key = args['KMS_KEY']

    ## Record job run count metric
    record_metric("SdlfHeavyTransformJob-run_count", 1)

    ## Processing the files
    process_files(source_locations, output_location, kms_key, silver_catalog)

