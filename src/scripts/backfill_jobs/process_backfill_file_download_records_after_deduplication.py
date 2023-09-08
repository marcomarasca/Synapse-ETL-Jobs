"""This script executed by a Glue job for processing deduplicated data and store it in json format"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "FORMAT", "S3_DESTINATION_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def transform(dynamic_record):
    try:
        datetime_object = datetime.strptime(dynamic_record["timestamp"], "%Y-%m-%d %H:%M:%S.%f")
        # need to store timestamp in milliseconds
        dynamic_record["timestamp"] = int(datetime_object.timestamp() * 1000)
        date = datetime_object.date()
        dynamic_record["year"] = date.year
        dynamic_record["month"] = '%02d' % date.month
        dynamic_record["day"] = '%02d' % date.day
        return dynamic_record
    except Exception as e:
        print("Exception in transform method :")
        print(str(dynamic_record))
        print(e)
        print("Exception type :", type(e).__name__)


# Read data from s3
input_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format=args["FORMAT"],
    connection_options={
        "paths": [args["S3_SOURCE_PATH"]],
        "recurse": True,
    },
    transformation_ctx="input_frame",
)

# Apply mapping
mapped_frame = ApplyMapping.apply(
    frame=input_frame,
    mappings=[
        ("stack", "string", "stack", "string"),
        ("instance", "string", "instance", "string"),
        ("timestamp", "string", "timestamp", "string"),
        ("user_id", "bigint", "payload.userId", "bigint"),
        ("file_handle_id", "string", "payload.fileHandleId", "string"),
        ("project_id", "bigint", "payload.projectId", "bigint"),
        ("downloaded_file_handle_id", "string", "payload.downloadedFileHandleId", "string"),
        ("association_object_id", "string", "payload.associateId", "string"),
        ("association_object_type", "string", "payload.associateType", "string")
    ],
    transformation_ctx="mapped_frame",
)
mapped_frame.printSchema()
transformed_frame = mapped_frame.map(f=transform)
transformed_frame.printSchema()
if transformed_frame.stageErrorsCount() > 0 or mapped_frame.stageErrorsCount() > 0:
    raise Exception("Error in job! See the log!")

# write to back to s3
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=transformed_frame,
    connection_type="s3",
    format=args["FORMAT"],
    connection_options={
        "path": args["S3_DESTINATION_PATH"],
        "compression": "gzip",
        "partitionKeys": ["year", "month", "day"],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
