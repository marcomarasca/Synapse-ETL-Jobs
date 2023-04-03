"""
This script executed by a Glue job. The job take the access record data from S3 and process it.
 Processed data stored in S3 in a parquet file partitioned by timestamp of record as  year / month / day.
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def get_dynamic_frame(connection_type, file_format, source_path, glue_context):
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        format_options={"multiline": True},
        connection_type=connection_type,
        format=file_format,
        connection_options={
            "paths": [source_path],
            "recurse": True,
        },
        transformation_ctx="dynamic_frame")
    return dynamic_frame

def apply_mapping(dynamic_frame):
    
    mapped_dynamic_frame = ApplyMapping.apply(
        frame=dynamic_frame,
        mappings=[
            ("changeType",                      "string",   "change_type",          "string"),
            ("changeTimestamp",                 "bigint",   "change_timestamp",     "bigint"),
            ("changeUserId",                    "bigint",   "change_user_id",       "bigint"),
            ("snapshotTimestamp",               "bigint",   "snapshot_timestamp",   "bigint"),
            ("snapshot.id",                     "string",   "id",                   "bigint"),
            ("snapshot.benefactorId",           "string",   "benefactor_id",        "bigint"),
            ("snapshot.projectId",              "string",   "project_id",           "bigint"),
            ("snapshot.parentId",               "string",   "parent_id",            "bigint"),
            ("snapshot.nodeType",               "string",   "node_type",            "string"),
            ("snapshot.createdOn",              "bigint",   "created_on",           "bigint"),
            ("snapshot.createdByPrincipalId",   "bigint",   "created_by",           "bigint"),
            ("snapshot.modifiedOn",             "bigint",   "modified_on",          "bigint"),
            ("snapshot.modifiedByPrincipalId",  "bigint",   "modified_by",          "bigint"),
            ("snapshot.versionNumber",          "bigint",   "version_number",       "bigint"),
            ("snapshot.fileHandleId",           "string",   "file_handle_id",       "bigint"),
            ("snapshot.name",                   "string",   "name",                 "string"),
            ("snapshot.isPublic",               "boolean",  "is_public",            "boolean"),
            ("snapshot.isControlled",           "boolean",  "is_controlled",        "boolean"),
            ("snapshot.isRestricted",           "boolean",  "is_restricted",        "boolean"),
            ("change_date",                     "string",   "change_date",          "string"),
        ],
        transformation_ctx="mapped_dynamic_frame"
    )

    return mapped_dynamic_frame

def strip_syn_prefix(input_string):
    if input_string is None:
        return input_string

    if input_string.startswith('syn'):
        return input_string[len('syn'):]

    return input_string

def transform(dynamic_record):
    date = datetime.utcfromtimestamp(dynamic_record["changeTimestamp"] / 1000.0)
    
    # Add the partition date from the timestamp of the change
    dynamic_record["change_date"] = date.strftime("%Y-%m-%d")
    
    # The records come in with the syn prefix, we need to remove that
    dynamic_record["snapshot"]["id"] = strip_syn_prefix(dynamic_record["snapshot"]["id"])
    dynamic_record["snapshot"]["benefactorId"] = strip_syn_prefix(dynamic_record["snapshot"]["benefactorId"])
    dynamic_record["snapshot"]["projectId"] = strip_syn_prefix(dynamic_record["snapshot"]["projectId"])
    dynamic_record["snapshot"]["parentId"] = strip_syn_prefix(dynamic_record["snapshot"]["parentId"])
    dynamic_record["snapshot"]["fileHandleId"] = strip_syn_prefix(dynamic_record["snapshot"]["fileHandleId"])

    return dynamic_record

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "DATABASE_NAME", "TABLE_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    dynamic_frame = get_dynamic_frame("s3", "json", args["S3_SOURCE_PATH"], glue_context)
    transformed_dynamic_frame = dynamic_frame.map(f=transform)
    mapped_dynamic_frame = apply_mapping(transformed_dynamic_frame)

    glue_context.write_dynamic_frame.from_catalog(
        frame=mapped_dynamic_frame,
        database=args["DATABASE_NAME"],
        table_name=args["TABLE_NAME"],
        additional_options={"partitionKeys": ["change_date"]},
        transformation_ctx="write_dynamic_frame"
    )

    job.commit()

if __name__ == "__main__":
    main()