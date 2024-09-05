# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
from typing import List, Dict, Set, Tuple
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode
from dataclasses import dataclass
from pyspark.sql.types import NumericType, StructType
from awsglue.gluetypes import ChoiceType, NullType

from utilities import GlueUtilities
solution_args = getResolvedOptions(sys.argv,
                                   ['SOLUTION_ID', 'SOLUTION_VERSION', 'RESOURCE_PREFIX', 'METRICS_NAMESPACE'])
glue_utils = GlueUtilities(solution_args)
logger = glue_utils.logger


@dataclass
class Report:
    name: str
    dataframe: DataFrame


@dataclass
class SourceData:
    dynamic_frame: DynamicFrame
    spark_dataframe: DataFrame


@dataclass
class ReportChoiceFields:
    fields_with_numeric_choice: Set[str]
    fields_with_non_numeric_choice: Set[str]


SP_REPORT_KEY_IN_JSON_FILE = "examples"
REPORT_SPECIFICATION_KEY_IN_JSON_FILE = "reportSpecification"


def load_source_data_from_s3(glue_context, bucket_name, s3_key) -> SourceData:
    df_dynamic = glue_context.create_dynamic_frame.from_options(
        format_options={
            "multiline": False,
        },
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [f"s3://{bucket_name}/{s3_key}"]
        }
    )
    df_spark = df_dynamic.toDF()
    return SourceData(df_dynamic, df_spark)


def create_or_update_table(glue_context, frame: DynamicFrame, db: str, table: str, dest_path: str,
                           partitions: List[str] = []) -> None:
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


def parse_s3_object_key(object_key: str) -> Tuple[str, str]:
    """
    Parse an S3 object key to extract the table name and generate the output S3 path.
    @param object_key: object_key (str): The S3 object key in the format "pre-stage/<team>/<dataset>/<table_name>/<filename>.json".
    @return: A tuple containing the table name and the output S3 path.
    """
    table_name = object_key.split("/")[3]
    output_s3_path = object_key.replace("pre-stage", "post-stage").removesuffix(".json")
    return table_name, output_s3_path


def extract_sp_reports(dynamic_df: DynamicFrame) -> List[Report]:
    """
    Explode nested columns in a DynamicFrame and create a list of Report objects.
    @param dynamic_df: The input DynamicFrame containing nested columns.
    @return:
    """
    spark_df = dynamic_df.toDF()
    reports = []

    for col_name in spark_df.columns:
        exploded_df = spark_df.select(explode(col_name).alias(col_name))
        if not exploded_df.isEmpty():
            exploded_df = exploded_df.select(f"{col_name}.*")
            reports.append(Report(name=col_name, dataframe=exploded_df))
        else:
            logger.info("The report is empty")

    return reports


def is_struct_type_numeric(struct_type: StructType) -> bool:
    return all(isinstance(field.dataType, NumericType) for field in struct_type.fields)


def get_sp_report_columns(spark_dataframe: DataFrame, columns_to_exclude: List[str]) -> List[str]:
    return [col_name for col_name in spark_dataframe.columns if col_name not in columns_to_exclude]


def initialize_glue() -> (Job, GlueContext):
    spark_session = SparkSession.builder.config("hive.metastore.client.factory.class",
                                                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()
    sc = spark_session.sparkContext
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    return job, glue_context


def categorize_choice_fields(report_dataframe: DataFrame, choice_fields: List[str]) -> ReportChoiceFields:
    """
    Categorize the given choice fields into numeric and non-numeric fields.
    @param report_dataframe: A dataframe that the array column is already exploded.
    @param choice_fields: A list of choice field names.
    @return: ReportChoiceFields: A named tuple containing sets of numeric and non-numeric field names.
    """
    numeric_fields = set()
    non_numeric_fields = set()
    for choice_field in choice_fields:
        # Each choice_field represents a single column in the report_dataframe DataFrame Therefore, we can directly access the
        # StructField object representing that column by using index 0.
        struct_field = report_dataframe.select(choice_field).schema.fields[0]
        if is_struct_type_numeric(struct_field.dataType):
            numeric_fields.add(struct_field.name)
        else:
            non_numeric_fields.add(struct_field.name)
    return ReportChoiceFields(numeric_fields, non_numeric_fields)


def categorize_choice_fields_by_report(spark_df: DataFrame,
                                       report_choice_mapping: Dict[str, List[str]]) -> Dict[str, ReportChoiceFields]:
    """
    Categorize choice fields into numeric and non-numeric fields for each report in the mapping.
    @param spark_df:
    @param report_choice_mapping: A dictionary mapping report names to lists of choice field names.

    @return: A dictionary mapping report names to ReportChoiceFields objects containing sets of numeric and non-numeric field names.
    """
    categorized_reports = {}
    for report_name, choice_fields in report_choice_mapping.items():
        report_df = spark_df.select(explode(report_name).alias(report_name)).select(f"{report_name}.*")
        categorized_reports[report_name] = categorize_choice_fields(report_df, choice_fields)

    return categorized_reports


def get_choice_fields_by_report(dynamic_frame: DynamicFrame) -> Dict[str, List[str]]:
    """
    Get a mapping of report names to lists of choice field names in the given DynamicFrame.
    @param dynamic_frame:
    @return: A dictionary mapping report names to lists of choice field names.
    """
    # Example SP data with Choice field.
    # root
    # | -- dataByAsin: array
    # | | -- element: struct
    # | | | -- asin: string
    # | | | -- combinationPct: choice
    # | | | | -- double
    # | | | | -- int

    # Example empty SP data.
    # root
    # | -- dataByAsin: array(nullable=true)
    # | | -- element: void(containsNull=true)

    choice_fields_by_report: Dict[str, List[str]] = {}
    for field in dynamic_frame.schema().fields:
        choice_fields: List[str] = []
        if not isinstance(field.dataType.elementType, NullType):
            for struct_field in field.dataType.elementType:
                if isinstance(struct_field.dataType, ChoiceType):
                    choice_fields.append(struct_field.name)
        if choice_fields:
            choice_fields_by_report[field.name] = choice_fields

    return choice_fields_by_report


def get_resolve_choice_specs(report_to_choice_fields: Dict[str, ReportChoiceFields]) -> List[Tuple[str, str]]:
    """
    Generate specs for resolving choice fields in a DynamicFrame.
    @param report_to_choice_fields:  dictionary mapping report names to ReportChoiceFields namedtuples,
        which contain sets of numeric and non-numeric choice field names.
    @return: A list of tuples, where each tuple contains the field path and action for resolving the choice field.
    """
    cast_specs = []
    for report_name, choice_fields in report_to_choice_fields.items():
        numeric_cast_specs = [(f"{report_name}[].{field}", "cast:double") for field in
                              choice_fields.fields_with_numeric_choice]
        non_numeric_cast_specs = [(f"{report_name}[].{field}", "cast:string") for field in
                                  choice_fields.fields_with_non_numeric_choice]
        cast_specs.extend(numeric_cast_specs + non_numeric_cast_specs)
    return cast_specs


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

    job, glue_context = initialize_glue()
    job.init(args['JOB_NAME'], args)

    destination_s3_object_paths = []
    for source_s3_object_key in source_s3_object_keys:
        logger.info(f"processing {source_s3_object_key}")
        table_name, output_s3_path = parse_s3_object_key(source_s3_object_key)
        source_data = load_source_data_from_s3(glue_context, stage_bucket, source_s3_object_key)

        report_columns = get_sp_report_columns(source_data.spark_dataframe, [REPORT_SPECIFICATION_KEY_IN_JSON_FILE])
        reports_dynamic_df = source_data.dynamic_frame.select_fields(paths=report_columns)
        reports_dynamic_df.printSchema()

        choice_fields_by_report = get_choice_fields_by_report(reports_dynamic_df)

        report_choice_fields = categorize_choice_fields_by_report(source_data.spark_dataframe, choice_fields_by_report)
        logger.info(f"Reports' numeric and non-numeric choice fields: {report_choice_fields}")

        specs = get_resolve_choice_specs(report_choice_fields)
        logger.info(f"Resolve choice specs: {specs}")

        if specs:
            reports_dynamic_df = reports_dynamic_df.resolveChoice(specs=specs)

        reports = extract_sp_reports(reports_dynamic_df)

        for report in reports:
            report_table_name = f"{table_name}_{report.name}"
            report_output_s3_path = output_s3_path.replace(table_name, report_table_name)

            logger.info(f"Writing Dynamic Frame into table {report_table_name}")
            report_dynamic_frame = DynamicFrame.fromDF(report.dataframe, glue_context, "dynamic_frame_report")
            
            timestamp_str = glue_utils.return_timestamp(bucket_name=stage_bucket, s3_key=source_s3_object_key)
            report_dynamic_frame = report_dynamic_frame.map(f = lambda record, timestamp=timestamp_str: GlueUtilities.map_fixed_value_column(record, col_name="timestamp", col_val=timestamp))
            report_dynamic_frame = report_dynamic_frame.resolveChoice(specs=[("timestamp", "cast:timestamp")])
            
            report_dynamic_frame.printSchema()
            logger.info(f"Number of rows: {report_dynamic_frame.count()}")

            # <bucket-name>/<team>/<dataset>/<table_name>/<source_file_name>/output.parquet
            create_or_update_table(glue_context, report_dynamic_frame, database, report_table_name,
                                   f"s3://{stage_bucket}/{report_output_s3_path}")
            
            destination_s3_object_paths.append(report_output_s3_path)
            
    glue_utils.record_glue_metrics(
        source_bucket=stage_bucket,
        source_keys=source_s3_object_keys,
        destination_bucket=stage_bucket,
        destination_paths=destination_s3_object_paths
    )

    job.commit()