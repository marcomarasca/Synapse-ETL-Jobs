"""
The job take the file handle snapshot data from S3 and process it.
Processed data stored in S3 in a parquet file partitioned by the date (%Y-%m-%d pattern) of the snapshot timestamp.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from utils import ms_to_partition_date

# process the access record
def transform(dynamic_record):
    # This is the partition date
    dynamic_record["snapshot_date"] = ms_to_partition_date(dynamic_record["snapshot_timestamp"])
    
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
            ("snapshot.id",                 "string",   "id",                   "bigint"),
            ("snapshot.createdBy",          "string",   "created_by",           "bigint"),
            ("snapshot.createdOn",          "bigint",   "created_on",           "bigint"),
            ("snapshot.modifiedOn",         "bigint",   "modified_on",          "bigint"),
            ("snapshot.concreteType",       "string",   "concrete_type",        "string"),
            ("snapshot.contentMd5",         "string",   "content_md5",          "string"),
            ("snapshot.contentType",        "string",   "content_type",         "string"),
            ("snapshot.fileName",           "string",   "file_name",            "string"),
            ("snapshot.storageLocationId",  "bigint",   "storage_location_id",  "bigint"),
            ("snapshot.contentSize",        "bigint",   "content_size",         "bigint"),
            ("snapshot.bucket",             "string",   "bucket",               "string"),
            ("snapshot.key",                "string",   "key",                  "string"),
            ("snapshot.previewId",          "string",   "preview_id",           "bigint"),
            ("snapshot.isPreview",          "boolean",  "is_preview",           "boolean"),
            ("snapshot.status",             "string",   "status",               "string")
        ]
    )

    # Apply transformations (compute the partition and convert timstamps)
    transformed_frame = mapped_frame.map(f=transform)
    
    # Now cast the "ids" to actual long as well the timestamps
    output_frame = transformed_frame.resolveChoice(
        [
            ("change_user_id",      "cast:bigint"),
            ("id",                  "cast:bigint"),
            ("created_by",          "cast:bigint"),
            ("storage_location_id", "cast:bigint"),
            ("content_size",        "cast:bigint"),
            ("preview_id",          "cast:bigint"),
            ("snapshot_timestamp",  "cast:timestamp"),
            ("change_timestamp",    "cast:timestamp"),
            ("created_on",          "cast:timestamp"),
            ("modified_on",         "cast:timestamp")
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