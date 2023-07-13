"""
This script executed by a Glue job for back-filling the node snapshots. The job take the node snapshot data from S3
which is in csv format and process it. Processed data stored in S3 in a json format and partitioned by record date of
snapshot as  year / month / day.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import input_file_name
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from dateutil import parser
from datetime import datetime
import json

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "S3_DESTINATION_PATH", "RELEASE_NUMBER"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# get data from S3
input_frame = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": False,
        "separator": ",",
        "multiline": True,
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [args["S3_SOURCE_PATH"] + args["RELEASE_NUMBER"] + "/noderecord/"],
        "recurse": True,
    },
    transformation_ctx="input_frame",
)

# Create a DataFrame and add a new column in the containing the file name of every DataRecord
data_frame = input_frame.toDF().withColumn("filename", input_file_name())

# Convert the DataFrame back to DynamicFrame
dynamic_frame_with_filename = DynamicFrame.fromDF(data_frame, glueContext, "dynamic_frame_with_filename")

# Apply Mapping
mapped_frame = ApplyMapping.apply(
    frame=dynamic_frame_with_filename,
    mappings=[
        ("col0", "string", "changeTimestamp", "bigint"),
        ("col2", "string", "snapshot", "string"),
        ("filename", "string", "filename", "string")
    ],
    transformation_ctx="mapped_frame",
)


# populate fields which is not present and fill calculative fields
def populate_fields(dynamic_record):
    jsn = json.loads(dynamic_record["snapshot"])
    required_fields = ["id", "name", "description", "parentId", "createdByPrincipalId", "createdOn",
                       "modifiedByPrincipalId", "modifiedOn", "nodeType", "versionNumber", "versionComment",
                       "versionLabel", "isLatestVersion", "activityId", "fileHandleId", "columnModelIds", "scopeIds",
                       "items", "reference", "alias", "isSearchEnabled", "definingSQL", "isPublic", "isRestricted",
                       "isControlled", "benefactorId", "projectId", "etag"]
    for field in required_fields:
        if field not in jsn:
            jsn[field] = None

    dynamic_record["objectType"] = "ENTITY"
    dynamic_record["stack"] = "dev"
    dynamic_record["instance"] = (args["RELEASE_NUMBER"]).lstrip("0")
    dynamic_record["userId"] = jsn["modifiedByPrincipalId"]

    modified_on = parser.parse(jsn["modifiedOn"])
    created_on = parser.parse(jsn["createdOn"])
    time_delta = (modified_on - created_on).total_seconds()
    if time_delta >= 1:
        dynamic_record["changeType"] = "UPDATE"
    else:
        dynamic_record["changeType"] = "CREATE"

    jsn["modifiedOn"] = int(modified_on.timestamp() * 1000)
    jsn["createdOn"] = int(created_on.timestamp() * 1000)

    dynamic_record["snapshot"] = jsn
    return dynamic_record


# add year,month and day for partitioning
def add_partition_fields(dynamic_record):
    file_name = dynamic_record["filename"]
    print("filename " + file_name)
    last_slash_index = file_name.rfind("/")
    second_last_index = file_name.rfind("/", 0, last_slash_index - 1)
    date_string = file_name[second_last_index + 1: last_slash_index]
    date = datetime.strptime(date_string, '%Y-%m-%d')
    dynamic_record["snapshotTimestamp"] = int(date.timestamp() * 1000)
    dynamic_record["year"] = date.year
    dynamic_record["month"] = '%02d' % date.month
    dynamic_record["day"] = '%02d' % date.day
    return dynamic_record


populated_fields_frame = mapped_frame.map(f=populate_fields).map(f=add_partition_fields)
dropped_filed_frame = populated_fields_frame.drop_fields(paths=["filename"]).repartition(1)

if (dropped_filed_frame.count() > 0):
    output_frame = glueContext.write_dynamic_frame.from_options(
        frame=dropped_filed_frame,
        connection_type="s3",
        format="json",
        connection_options={
            "path": args["S3_DESTINATION_PATH"],
            "compression": "gzip",
            "partitionKeys": ["year", "month", "day"],
        },
        transformation_ctx="output_frame",
    )

job.commit()
