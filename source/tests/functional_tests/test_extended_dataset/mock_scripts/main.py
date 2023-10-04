# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import sys
from typing import List
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import boto3
from pyspark.sql import SparkSession

args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'SOURCE_LOCATION', 'OUTPUT_LOCATION', 'DATABASE_NAME'])
source = args['SOURCE_LOCATION']
destination = args['OUTPUT_LOCATION']
database = args['DATABASE_NAME']

spark_session = SparkSession.builder.config("hive.metastore.client.factory.class",
                                            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()
sc = spark_session.sparkContext

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

memberships = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ['{}/{}'.format(source, 'memberships_parsed.json')]
    },
    format_options={
        "withHeader": False
    },
    transformation_ctx="path={}".format('memberships_df')
)


def overwrite_table(frame: DynamicFrame, db: str, table: str, dest_path: str, partitions: List[str] = []):
    if spark.sql(f"SHOW TABLES FROM {db} LIKE '{table}'").count() == 1:
        glueContext.purge_s3_path(dest_path, options={"retentionPeriod": 0})
        glue_client = boto3.client('glue')
        glue_client.delete_table(DatabaseName=db, Name=table)

    sink = glueContext.getSink(
        connection_type="s3",
        path=dest_path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partitions
    )
    sink.setFormat("parquet", useGlueParquetWriter=True)
    sink.setCatalogInfo(catalogDatabase=db, catalogTableName=table)

    sink.writeFrame(frame)


overwrite_table(memberships, database, "memberships", f"{destination}/memberships")

job.commit()
