"""
The job take the file handle snapshot data from S3 and process it.
Processed data stored in S3 in a parquet file partitioned by the date (%Y-%m-%d pattern) of the snapshot timestamp.
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def ms_to_athena_timestamp(timestamp_ms):
    if (timestamp_ms is None):
        return timestamp_ms
    
    # yyyy-MM-dd HH:mm:ss.fff
    return datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat(sep=' ', timespec='milliseconds')

# process the access record
def transform(dynamic_record):
    # This is the partition date
    dynamic_record["snapshot_date"] = datetime.utcfromtimestamp(dynamic_record["snapshot_timestamp"] / 1000.0).strftime("%Y-%m-%d")
   
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
            ("changeType",                  "string",   "change_type",          "string"),
            ("changeTimestamp",             "bigint",   "change_timestamp",     "bigint"),
            ("userId",                      "bigint",   "change_user_id",       "bigint"),
            ("snapshotTimestamp",           "bigint",   "snapshot_timestamp",   "bigint"),
            ("snapshot.id",                 "string",   "id",                   "string"),
            ("snapshot.createdBy",          "string",   "created_by",           "string"),
            ("snapshot.createdOn",          "bigint",   "created_on",           "bigint"),
            ("snapshot.modifiedOn",         "bigint",   "modified_on",          "bigint"),
            ("snapshot.concreteType",       "string",   "concrete_type",        "string"),
            ("snapshot.contentMd5",         "string",   "content_md5",          "string"),
            ("snapshot.fileName",           "string",   "file_name",            "string"),
            ("snapshot.storageLocationId",  "bigint",   "storage_location_id",  "bigint"),
            ("snapshot.contentSize",        "bigint",   "content_size",         "bigint"),
            ("snapshot.bucket",             "string",   "bucket",               "string"),
            ("snapshot.key",                "string",   "key",                  "string"),
            ("snapshot.previewId",          "string",   "preview_id",           "string"),
            ("snapshot.isPreview",          "boolean",  "is_preview",           "boolean"),
            ("snapshot.status",             "string",   "status",               "string")
        ]
    )

    # Apply transformations (compute the partition and convert timstamps)
    transformed_frame = mapped_frame.map(f=transform)
    
    # Now cast the "ids" to actual long as well the timestamps
    output_frame = transformed_frame.resolveChoice(
        [
            ("id", "cast:bigint"),
            ("created_by", "cast:bigint"),
            ("preview_id", "cast:bigint"),
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