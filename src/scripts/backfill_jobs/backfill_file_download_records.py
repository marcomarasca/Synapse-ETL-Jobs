"""This script executed by a Glue job for back-filling the file download records. The job take the file download
records data from S3 which is in csv format and process it. Processed data stored in S3 in a json format and
partitioned by record date  as  year / month / day. """

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
from dateutil import parser
from datetime import datetime
import gs_explode

from backfill_utils import *

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "S3_DESTINATION_PATH", "FILE_DOWNLOAD_TYPE", "STACK",
                                     "RELEASE_NUMBER"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# In case of bulk download, there is fileSummary array. create single file download record from each array element
def transform_bulk_download(dynamic_record):
    try:
        jsn = json.loads(dynamic_record["col2"])
        fileInfo = []
        for key in jsn:
            if key == "fileSummary":
                for f in jsn[key]:
                    info = {
                        "userId": int(jsn["userId"]),
                        "downloadedFileHandleId": jsn["resultZipFileHandleId"],
                        "fileHandleId": f["fileHandleId"],
                        "associateId": f["associateObjectId"],
                        "associateType": f["associateObjectType"],
                        "projectId": None
                    }
                    fileInfo.append(info)
        dynamic_record["payloads"] = fileInfo
        dynamic_record = add_common_fields(dynamic_record)
        return dynamic_record
    except Exception as e:
        print("Printing exception:")
        print(str(dynamic_record))
        print(e)
        print("exception in partition", type(e).__name__)


def transform_download(dynamic_record):
    try:
        jsn = json.loads(dynamic_record["col2"])
        payload = {}
        for key in jsn:
            if key == "userId":
                payload["userId"] = int(jsn["userId"])
            if key == "downloadedFile":
                payload["fileHandleId"] = jsn["downloadedFile"]["fileHandleId"]
                payload["associateId"] = jsn["downloadedFile"]["associateObjectId"]
                payload["associateType"] = jsn["downloadedFile"]["associateObjectType"]
                payload["downloadedFileHandleId"] = None
                payload["projectId"] = None
        dynamic_record["payload"] = payload
        dynamic_record = add_common_fields(dynamic_record)
        return dynamic_record
    except Exception as e:
        print("Printing exception:")
        print(str(dynamic_record))
        print(e)
        print("exception in partition", type(e).__name__)


def add_common_fields(dynamic_record):
    dynamic_record["stack"] = args["STACK"]
    dynamic_record["instance"] = remove_padded_leading_zeros(args["RELEASE_NUMBER"])
    dynamic_record["timestamp"] = int(dynamic_record["col0"])
    date = ms_to_partition_date(dynamic_record["timestamp"])
    dynamic_record["year"] = str(date.year)
    dynamic_record["month"] = '%02d' % date.month
    dynamic_record["day"] = '%02d' % date.day
    return dynamic_record


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
        "paths": [args["S3_SOURCE_PATH"] + args["RELEASE_NUMBER"] + "/" + args["FILE_DOWNLOAD_TYPE"] + "/"],
        "recurse": True,
    },
    transformation_ctx="input_frame",
)

if args["FILE_DOWNLOAD_TYPE"] == "bulkfiledownloadresponse":
    transformed_frame = input_frame.map(f=transform_bulk_download)
    if transformed_frame.stageErrorsCount() > 0:
        raise Exception("Error in job! See the log!")
    mapped_frame = ApplyMapping.apply(
        frame=transformed_frame,
        mappings=[
            ("timestamp", "bigint", "timestamp", "bigint"),
            ("stack", "string", "stack", "string"),
            ("instance", "string", "instance", "string"),
            ("year", "string", "year", "string"),
            ("month", "string", "month", "string"),
            ("day", "string", "day", "string"),
            ("payloads", "array", "payloads", "array")
        ],
        transformation_ctx="mapped_frame",
    )

    # Explode method creates separate row for each payload
    exploded_frame = mapped_frame.gs_explode(
        colName="payloads", newCol="payload"
    )

    final_frame = ApplyMapping.apply(
        frame=exploded_frame,
        mappings=[
            ("timestamp", "bigint", "timestamp", "bigint"),
            ("stack", "string", "stack", "string"),
            ("instance", "string", "instance", "string"),
            ("year", "string", "year", "string"),
            ("month", "string", "month", "string"),
            ("day", "string", "day", "string"),
            ("payload", "struct", "payload", "struct")
        ],
        transformation_ctx="final_frame",
    )
    repartitioned_frame = final_frame.repartition(1)

if args["FILE_DOWNLOAD_TYPE"] == "filedownloadrecord":
    transformed_frame = input_frame.map(f=transform_download)
    if transformed_frame.stageErrorsCount() > 0:
        raise Exception("Error in job! See the log!")
    final_frame = ApplyMapping.apply(
        frame=transformed_frame,
        mappings=[
            ("timestamp", "bigint", "timestamp", "bigint"),
            ("stack", "string", "stack", "string"),
            ("instance", "string", "instance", "string"),
            ("year", "string", "year", "string"),
            ("month", "string", "month", "string"),
            ("day", "string", "day", "string"),
            ("payload", "struct", "payload", "struct")
        ],
        transformation_ctx="final_frame",
    )
    repartitioned_frame = final_frame.repartition(1)

if (repartitioned_frame.count() > 0):
    output_frame = glueContext.write_dynamic_frame.from_options(
        frame=repartitioned_frame,
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
