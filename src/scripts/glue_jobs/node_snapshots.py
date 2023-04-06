"""
The job take the node snapshot data from S3 and process it.
Processed data stored in S3 in a parquet file partitioned by the date (%Y-%m-%d pattern) of the change timestamp.
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def strip_syn_prefix(input_string):
    if input_string is None:
        return input_string
    
    if input_string.startswith('syn'):
        return input_string[len('syn'):]
        
    return input_string

def ms_to_athena_timestamp(timestamp_ms):
    # yyyy-MM-dd HH:mm:ss
    return datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat(sep=' ', timespec='milliseconds')

# process the access record
def transform(dynamic_record):
    # This is the partition date
    dynamic_record["snapshot_date"] = datetime.utcfromtimestamp(dynamic_record["snapshot_timestamp"] / 1000.0).strftime("%Y-%m-%d")
    
    # The records come in with the syn prefix, we need to remove that
    dynamic_record["id"] = strip_syn_prefix(dynamic_record["id"])
    dynamic_record["benefactor_id"] = strip_syn_prefix(dynamic_record["benefactor_id"])
    dynamic_record["project_id"] = strip_syn_prefix(dynamic_record["project_id"])
    dynamic_record["parent_id"] = strip_syn_prefix(dynamic_record["parent_id"])
    dynamic_record["file_handle_id"] = strip_syn_prefix(dynamic_record["file_handle_id"])

    # Convert all the timestamps represented as ms to an athena compatible timestamp
    dynamic_record["snapshot_timestamp"] = ms_to_athena_timestamp(dynamic_record["snapshot_timestamp"])
    dynamic_record["change_timestamp"] = ms_to_athena_timestamp(dynamic_record["change_timestamp"])
    dynamic_record["created_on"] = ms_to_athena_timestamp(dynamic_record["created_on"])
    dynamic_record["modified_on"] = ms_to_athena_timestamp(dynamic_record["modified_on"])
    
    return dynamic_record

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "DATABASE_NAME", "TABLE_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    input_frame = glue_context.create_dynamic_frame.from_options(
        format_options={"multiline": True},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [args["S3_SOURCE_PATH"]],
            "recurse": True
        },
        # Note: even though this is optional, job bookmark does not work without it
        transformation_ctx="input_frame"
    )

    # Maps the incoming record to a flatten table
    mapped_frame = input_frame.apply_mapping(
        [
            ("changeType",                      "string",   "change_type",          "string"),
            ("changeTimestamp",                 "bigint",   "change_timestamp",     "bigint"),
            ("userId",                          "bigint",   "change_user_id",       "bigint"),
            ("snapshotTimestamp",               "bigint",   "snapshot_timestamp",   "bigint"),
            ("snapshot.id",                     "string",   "id",                   "string"),
            ("snapshot.benefactorId",           "string",   "benefactor_id",        "string"),
            ("snapshot.projectId",              "string",   "project_id",           "string"),
            ("snapshot.parentId",               "string",   "parent_id",            "string"),
            ("snapshot.nodeType",               "string",   "node_type",            "string"),
            ("snapshot.createdOn",              "bigint",   "created_on",           "bigint"),
            ("snapshot.createdByPrincipalId",   "bigint",   "created_by",           "bigint"),
            ("snapshot.modifiedOn",             "bigint",   "modified_on",          "bigint"),
            ("snapshot.modifiedByPrincipalId",  "bigint",   "modified_by",          "bigint"),
            ("snapshot.versionNumber",          "bigint",   "version_number",       "bigint"),
            ("snapshot.fileHandleId",           "string",   "file_handle_id",       "string"),
            ("snapshot.name",                   "string",   "name",                 "string"),
            ("snapshot.isPublic",               "boolean",  "is_public",            "boolean"),
            ("snapshot.isControlled",           "boolean",  "is_controlled",        "boolean"),
            ("snapshot.isRestricted",           "boolean",  "is_restricted",        "boolean"),
        ]
    )

    # Apply transformations (compute the partition and get rid of syn prefix)
    transformed_frame = mapped_frame.map(f=transform)
    
    # Now cast the "ids" to actual long as well the timestamps
    output_frame = transformed_frame.resolveChoice(
        [
            ("id", "cast:bigint"),
            ("benefactor_id", "cast:bigint"),
            ("project_id", "cast:bigint"),
            ("parent_id", "cast:bigint"),
            ("file_handle_id", "cast:bigint"),
            ("snapshot_timestamp", "cast:timestamp"),
            ("change_timestamp", "cast:timestamp"),
            ("created_on", "cast:timestamp"),
            ("modified_on", "cast:timestamp")
        ]
    )

    # Write only if there is new data (this will error out otherwise)
    if (output_frame.count() > 0):
        glue_context.write_dynamic_frame.from_catalog(
            frame=output_frame,
            database=args["DATABASE_NAME"],
            table_name=args["TABLE_NAME"],
            additional_options={"partitionKeys": ["snapshot_date"]}
        )

    job.commit()

if __name__ == "__main__":
    main()