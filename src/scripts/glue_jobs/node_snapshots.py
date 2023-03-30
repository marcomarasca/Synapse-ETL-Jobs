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

# Get access record from source and create dynamic frame for futher processing
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
            ("snapshot.id",                     "bigint",   "id",                   "bigint"),
            ("snapshot.benefactorId",           "bigint",   "benefactor_id",        "bigint"),
            ("snapshot.projectId",              "bigint",   "project_id",           "bigint"),
            ("snapshot.parentId",               "bigint",   "parent_id",            "bigint"),
            ("snapshot.nodeType",               "string",   "node_type",            "string"),
            ("snapshot.createdOn",              "bigint",   "created_on",           "bigint"),
            ("snapshot.createdByPrincipalId",   "bigint",   "created_by",           "bigint"),
            ("snapshot.modifiedOn",             "bigint",   "modified_on",          "bigint"),
            ("snapshot.modifiedByPrincipalId",  "bigint",   "modified_by",          "bigint"),
            ("snapshot.versionNumber",          "bigint",   "version_number",       "bigint"),
            ("snapshot.fileHandleId",           "bigint",   "file_handle_id",       "bigint"),
            ("snapshot.name",                   "string",   "name",                 "string"),
            ("snapshot.isPublic",               "boolean",  "is_public",            "boolean"),
            ("snapshot.isControlled",           "boolean",  "is_controlled",        "boolean"),
            ("snapshot.isRestricted",           "boolean",  "is_restricted",        "boolean"),
        ],
        transformation_ctx="mapped_dynamic_frame")
    return mapped_dynamic_frame


# process the access record
def transform(dynamic_record):
    date = datetime.utcfromtimestamp(dynamic_record["change_timestamp"] / 1000.0)
    dynamic_record["change_date"] = date.strftime("%Y-%m-%d")
    return dynamic_record

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "DATABASE_NAME", "TABLE_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    dynamic_frame = get_dynamic_frame("s3", "json", args["S3_SOURCE_PATH"], glue_context)
    mapped_dynamic_frame = apply_mapping(dynamic_frame)
    transformed_dynamic_frame = mapped_dynamic_frame.map(f=transform)

    glue_context.write_dynamic_frame.from_catalog(
        frame=transformed_dynamic_frame,
        database=args["DATABASE_NAME"],
        table_name=args["TABLE_NAME"],
        additional_options={"partitionKeys": ["change_date"]},
        transformation_ctx="write_dynamic_frame"
    )

    job.commit()


if __name__ == "__main__":
    main()