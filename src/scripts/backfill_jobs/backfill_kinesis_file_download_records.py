"""This script executed by a Glue job for back-filling the kinesis file download records. The job take the file download
records data from S3 which is in parquet format and process it. Processed data is stored in S3 in a table"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime

args = getResolvedOptions(sys.argv,
                          ["JOB_NAME", "SOURCE_DATABASE_NAME", "SOURCE_TABLE_NAME","YEAR", "DESTINATION_DATABASE_NAME", "DESTINATION_TABLE_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

predicate = "(year ==" + args["YEAR"] +")"

def transform(dynamic_record):
    try:
        dynamic_record["downloaded_file_handle_id"] = None
        dynamic_record["record_date"] = dynamic_record["timestamp"].date()
        return dynamic_record
    except Exception as e:
        print("Exception in transform of kinesis file download record:")
        print(str(dynamic_record))
        print(e)
        print("Exception type :", type(e).__name__)


# Script generated for node S3 bucket
input_frame = glueContext.create_dynamic_frame.from_catalog(
    database=args["SOURCE_DATABASE_NAME"],
    table_name=args["SOURCE_TABLE_NAME"],
    transformation_ctx="input_frame",
    push_down_predicate=predicate
)
input_frame.printSchema()

mapped_frame = ApplyMapping.apply(
    frame=input_frame,
    mappings=[
        ("userid", "bigint", "user_id", "bigint"),
        ("timestamp", "timestamp", "timestamp", "timestamp"),
        ("projectid", "bigint", "project_id", "bigint"),
        ("filehandleid", "string", "file_handle_id", "string"),
        ("associatetype", "string", "association_object_type", "string"),
        ("associateid", "string", "association_object_id", "string"),
        ("stack", "string", "stack", "string"),
        ("instance", "string", "instance", "string"),
    ],
    transformation_ctx="mapped_frame",
)
mapped_frame.printSchema()

transformed_frame = mapped_frame.map(f=transform)
transformed_frame.printSchema()
if transformed_frame.stageErrorsCount() > 0 or mapped_frame.stageErrorsCount() > 0:
    raise Exception("Error in job! See the log!")
repartitioned_frame = transformed_frame.repartition(1)

output_frame = repartitioned_frame.resolveChoice(choice='match_catalog', database=args['DESTINATION_DATABASE_NAME'],
                                                 table_name=args['DESTINATION_TABLE_NAME'])
if output_frame.count() > 0:
    glueContext.write_dynamic_frame.from_catalog(
        frame=output_frame,
        database=args["DESTINATION_DATABASE_NAME"],
        table_name=args["DESTINATION_TABLE_NAME"],
        additional_options={"partitionKeys": ["record_date"]}
    )

job.commit()
