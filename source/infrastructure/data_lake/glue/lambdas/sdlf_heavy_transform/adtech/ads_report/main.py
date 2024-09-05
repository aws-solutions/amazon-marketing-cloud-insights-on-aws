# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
from typing import List, Tuple
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.gluetypes import ChoiceType
from pyspark.sql.types import NumericType, StructType

from utilities import GlueUtilities
solution_args = getResolvedOptions(sys.argv,
                                   ['SOLUTION_ID', 'SOLUTION_VERSION', 'RESOURCE_PREFIX', 'METRICS_NAMESPACE'])
glue_utils = GlueUtilities(solution_args)
logger = glue_utils.logger


def initialize_glue() -> (Job, GlueContext):
    spark_session = SparkSession.builder.config("hive.metastore.client.factory.class",
                                                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()
    sc = spark_session.sparkContext

    glue_context = GlueContext(sc)
    job = Job(glue_context)
    return job, glue_context


def load_source_data_from_s3(glue_context, bucket_name, s3_key) -> DynamicFrame:
    df_dynamic = glue_context.create_dynamic_frame.from_options(
        format_options={
            "multiline": False,
            # ads data is returned as list-structured json [{},{},{}]
            # without this jsonPath, Glue will not properly load the data
            "jsonPath": "$[*]"
        },
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [f"s3://{bucket_name}/{s3_key}"]
        }
    )

    return df_dynamic


def create_or_update_table(glue_context, frame: DynamicFrame, db: str, table: str, dest_path: str,
                           partitions: List[str] = []):
    sink = glue_context.getSink(
        connection_type="s3",
        path=dest_path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partitions
    )
    sink.setFormat("parquet", useGlueParquetWriter=True)
    sink.setCatalogInfo(catalogDatabase=db, catalogTableName=table)
    sink.writeFrame(frame)


def extract_table_name_and_s3_path(object_key: str) -> Tuple[str, str]:
    """
    Parse an S3 object key to extract the table name and generate the output S3 path.
    @param object_key: object_key (str): The S3 object key in the format "pre-stage/<team>/<dataset>/<table_name>/<filename>.json".
    @return: A tuple containing the table name and the output S3 path.
    """
    table_name = object_key.split("/")[3]
    output_s3_path = object_key.replace("pre-stage", "post-stage").removesuffix(".json")
    return table_name, output_s3_path


def is_choice_type_numeric(struct_type: StructType) -> bool:
    return all(isinstance(field.dataType, NumericType) for field in struct_type.fields)


def get_choice_field_names(dynamic_frame: DynamicFrame) -> List[str]:
    """
    Get a list of choice field names from the given DynamicFrame.
    @param dynamic_frame:
    @return: A list of choice field names present in the DynamicFrame.
    """
    df_dynamic_schemas = dynamic_frame.schema()
    return [field.name for field in df_dynamic_schemas.fields if isinstance(field.dataType, ChoiceType)]


def categorize_choice_fields_by_type(
        dynamic_frame: DynamicFrame, fields_with_choice: List[str]) -> Tuple[List[str], List[str]]:
    """
     Categorize the given choice fields into numeric and non-numeric types based on their data types.
    @param dynamic_frame: The DynamicFrame object containing the choice fields.
    @param fields_with_choice:  A list of choice field names to categorize.
    @return: A tuple containing two lists:
            - The first list contains the names of choice fields with numeric data types.
            - The second list contains the names of choice fields with non-numeric data types.
    """
    spark_df = dynamic_frame.toDF()

    fields_with_numeric_type = []
    fields_with_non_numeric_type = []

    for field_name in fields_with_choice:
        for field in spark_df.select(field_name).schema.fields:
            if is_choice_type_numeric(field.dataType):
                fields_with_numeric_type.append(field.name)
            else:
                fields_with_non_numeric_type.append(field.name)
    return fields_with_numeric_type, fields_with_non_numeric_type


def get_resolve_choice_specs(dynamic_frame: DynamicFrame, fields_with_choice: List[str]) -> List[Tuple[str, str]]:
    """
    Generate a list of specifications for resolving choice fields in a DynamicFrame.
    @param dynamic_frame: The DynamicFrame object containing the choice fields.
    @param fields_with_choice: A list of choice field names to resolve.
    @return: A list of tuples, where each tuple contains the field name and the action for resolving the choice field.
    """
    fields_with_numeric_type, fields_with_non_numeric_type = categorize_choice_fields_by_type(dynamic_frame,
                                                                                              fields_with_choice)
    # The action is "cast:double" for numeric choice fields and "cast:string" for non-numeric choice fields.
    return [(field, "cast:double") for field in fields_with_numeric_type] + [(field, "cast:string") for field in
                                                                             fields_with_non_numeric_type]


if __name__ == '__main__':
    args = getResolvedOptions(
        sys.argv, ['JOB_NAME',
                   'STAGE_BUCKET',
                   'DATABASE_NAME',
                   'SOURCE_S3_OBJECT_KEYS',
                   ])
    stage_bucket = args['STAGE_BUCKET']
    source_s3_object_keys = args['SOURCE_S3_OBJECT_KEYS'].split(",")
    database = args['DATABASE_NAME']
    job_name = args['JOB_NAME']

    job, glue_context = initialize_glue()

    job.init(job_name, args)

    destination_s3_object_paths = []
    for source_s3_object_key in source_s3_object_keys:
        logger.info(f"Processing file {source_s3_object_key}")
        table_name, output_s3_path = extract_table_name_and_s3_path(source_s3_object_key)

        df_dynamic = load_source_data_from_s3(glue_context, stage_bucket, source_s3_object_key)
        df_dynamic.printSchema()

        fields_with_choice = get_choice_field_names(df_dynamic)

        logger.info("Casting Choice type")

        specs = get_resolve_choice_specs(df_dynamic, fields_with_choice)
        logger.info(f"Resolve choice specs: {specs}")
        if specs:
            df_dynamic = df_dynamic.resolveChoice(specs=specs)
        
        timestamp_str = glue_utils.return_timestamp(bucket_name=stage_bucket, s3_key=source_s3_object_key)
        df_dynamic = df_dynamic.map(f = lambda record, timestamp=timestamp_str: GlueUtilities.map_fixed_value_column(record, col_name="timestamp", col_val=timestamp))
        df_dynamic = df_dynamic.resolveChoice(specs=[("timestamp", "cast:timestamp")])

        logger.info(f"Writing Dynamic Frame into table {table_name}")
        df_dynamic.printSchema()
        logger.info(f"Number of rows: {df_dynamic.count()}")
        
        # <bucket-name>/<team>/<dataset>/<table_name>/<source_file_name>/output.parquet
        create_or_update_table(glue_context, df_dynamic, database, table_name,
                               f"s3://{stage_bucket}/{output_s3_path}")
        
        destination_s3_object_paths.append(output_s3_path)
        
    glue_utils.record_glue_metrics(
        source_bucket=stage_bucket,
        source_keys=source_s3_object_keys,
        destination_bucket=stage_bucket,
        destination_paths=destination_s3_object_paths
    )

    job.commit()