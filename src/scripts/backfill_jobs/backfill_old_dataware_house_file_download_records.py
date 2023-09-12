import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_explode
import json
from backfill_utils import *

args = getResolvedOptions(sys.argv,
                          ["JOB_NAME", "DESTINATION_DATABASE_NAME", "SOURCE_DATABASE_NAME", "DESTINATION_TABLE_NAME", "SOURCE_TABLE_NAME", "FILE_DOWNLOAD_TYPE", "STACK",
                           "RELEASE_NUMBER", "START_DATE", "END_DATE"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

predicate = "(release_number == " + args["RELEASE_NUMBER"] + " and ( record_date>='" + args["START_DATE"] + "' and record_date<='" + args["END_DATE"] + "'))"

def transform_bulk_download(dynamic_record):
    try:
        jsn = json.loads(dynamic_record["json"])
        file_info = []
        file_summary_array = get_key_from_json_payload(jsn, "fileSummary")
        for file in file_summary_array:
            file_summary = {
                "file_handle_id": get_key_from_json_payload(file, "fileHandleId"),
                "association_object_id": get_key_from_json_payload(file, "associateObjectId"),
                "association_object_type": get_key_from_json_payload(file, "associateObjectType")
            }
            file_info.append(file_summary)
        dynamic_record["payloads"] = file_info
        dynamic_record = add_common_fields(jsn, dynamic_record)
        return dynamic_record
    except Exception as e:
        print("Exception in transform_bulk_download method:")
        print(str(dynamic_record))
        print(e)
        print("Exception type:", type(e).__name__)


def get_key_from_json_payload(json_payload, key):
    if key in json_payload:
        return json_payload[key]
    return None


def transform_download(dynamic_record):
    try:
        jsn = json.loads(dynamic_record["json"])
        for key in jsn:
            if key == "downloadedFile":
                dynamic_record["file_handle_id"] = jsn["downloadedFile"]["fileHandleId"]
                dynamic_record["association_object_id"] = jsn["downloadedFile"]["associateObjectId"]
                dynamic_record["association_object_type"] = jsn["downloadedFile"]["associateObjectType"]
        dynamic_record = add_common_fields(jsn, dynamic_record)
        return dynamic_record
    except Exception as e:
        print("Exception in transform_download method:")
        print(str(dynamic_record))
        print(e)
        print("exception in partition", type(e).__name__)


def add_common_fields(json_payload, dynamic_record):
    try:
        dynamic_record["stack"] = args["STACK"]
        dynamic_record["instance"] = args["RELEASE_NUMBER"].lstrip("0")
        dynamic_record["timestamp"] = int(dynamic_record["timestamp"])
        dynamic_record["user_id"] = get_key_from_json_payload(json_payload, "userId")
        dynamic_record["downloaded_file_handle_id"] = get_key_from_json_payload(json_payload, "resultZipFileHandleId")
        dynamic_record["project_id"] = None
        date = ms_to_formatted_date(dynamic_record["timestamp"], "%Y-%m-%d")
        dynamic_record["record_date"] = date
        return dynamic_record
    except Exception as e:
        print("Exception in add_common_fields method:")
        print(str(dynamic_record))
        print(e)
        print("exception in partition", type(e).__name__)


# Script generated for node S3 bucket
input_frame = glueContext.create_dynamic_frame.from_catalog(
    database=args["SOURCE_DATABASE_NAME"],
    table_name=args["SOURCE_TABLE_NAME"],
    transformation_ctx="input_frame",
    push_down_predicate=predicate
)

if args["FILE_DOWNLOAD_TYPE"] == "bulkfiledownloadresponse":
    transformed_frame = input_frame.map(f=transform_bulk_download)
    mapped_frame = ApplyMapping.apply(
        frame=transformed_frame,
        mappings=[
            ("timestamp", "bigint", "timestamp", "bigint"),
            ("stack", "string", "stack", "string"),
            ("instance", "string", "instance", "string"),
            ("record_date", "date", "record_date", "date"),
            ("project_id", "bigint", "project_id", "bigint"),
            ("user_id", "bigint", "user_id", "bigint"),
            ("downloaded_file_handle_id", "bigint", "downloaded_file_handle_id", "bigint"),
            ("payloads", "array", "payloads", "array")
        ],
        transformation_ctx="mapped_frame",
    )

    # Explode method creates separate row for each correction
    exploded_frame = mapped_frame.gs_explode(
        colName="payloads", newCol="payload"
    )


    final_frame = ApplyMapping.apply(
        frame=exploded_frame,
        mappings=[
            ("timestamp", "bigint", "timestamp", "timestamp"),
            ("stack", "string", "stack", "string"),
            ("instance", "string", "instance", "string"),
            ("record_date", "date", "record_date", "date"),
            ("user_id", "string", "user_id", "bigint"),
            ("project_id", "string", "project_id", "bigint"),
            ("downloaded_file_handle_id", "string", "downloaded_file_handle_id", "string"),
            ("payload.file_handle_id", "string", "file_handle_id", "string"),
            ("payload.association_object_id", "string", "association_object_id", "string"),
            ("payload.association_object_type", "string", "association_object_type", "string")
        ],
        transformation_ctx="final_frame",
    )

    if final_frame.stageErrorsCount() > 0 or exploded_frame.stageErrorsCount() > 0 or mapped_frame.stageErrorsCount() > 0 or transformed_frame.stageErrorsCount() > 0:
        raise Exception("Error in job! See the log!")

    repartitioned_frame = final_frame.repartition(1)

if args["FILE_DOWNLOAD_TYPE"] == "filedownloadrecord":
    transformed_frame = input_frame.map(f=transform_download)
    final_frame = ApplyMapping.apply(
        frame=transformed_frame,
        mappings=[
            ("timestamp", "bigint", "timestamp", "timestamp"),
            ("stack", "string", "stack", "string"),
            ("instance", "string", "instance", "string"),
            ("record_date", "date", "record_date", "date"),
            ("user_id", "string", "user_id", "bigint"),
            ("project_id", "string", "project_id", "bigint"),
            ("downloaded_file_handle_id", "string", "downloaded_file_handle_id", "string"),
            ("file_handle_id", "string", "file_handle_id", "string"),
            ("association_object_id", "string", "association_object_id", "string"),
            ("association_object_type", "string", "association_object_type", "string")
        ],
        transformation_ctx="final_frame",
    )

    if final_frame.stageErrorsCount() > 0 or transformed_frame.stageErrorsCount() > 0:
        raise Exception("Error in job! See the log!")

    repartitioned_frame = final_frame.repartition(1)
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
