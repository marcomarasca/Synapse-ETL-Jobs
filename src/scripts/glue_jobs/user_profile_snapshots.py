"""
The job take the user profile snapshot data from S3 and process it.
Processed data stored in S3 in a parquet file partitioned by the date (%Y-%m-%d pattern) of the snapshot timestamp.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from utils import ms_to_partition_date

# process the user profile record
def transform(dynamic_record):
    # This is the partition date
    dynamic_record["snapshot_date"] = ms_to_partition_date(dynamic_record["snapshot_date"])
    # Get the first email if list is not empty and add email as field to dynamic frame
    dynamic_record["email"] = get_email(dynamic_record["emails"])
    return dynamic_record

def get_email(emails):
    if not emails:
        return None
    else:
        return emails[0]
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

    # Maps the incoming record to flatten table
    mapped_frame = input_frame.apply_mapping(
        [
            ("changeTimestamp", "bigint", "change_timestamp", "timestamp"),
            ("changeType", "string", "change_type", "string"),
            ("userId", "bigint", "change_user_id", "bigint"),
            ("snapshotTimestamp", "bigint", "snapshot_timestamp", "timestamp"),
            # Note that we map the same timestamp into a bigint so that we can extract the partition date
            ("snapshotTimestamp", "bigint", "snapshot_date", "bigint"),
            ("snapshot.ownerId", "string", "id", "bigint"),
            ("snapshot.userName", "string", "user_name", "string"),
            ("snapshot.firstName", "string", "first_name", "string"),
            ("snapshot.lastName", "string", "last_name", "string"),
            ("snapshot.emails", "array", "emails", "array"),
            ("snapshot.location", "string", "location", "string"),
            ("snapshot.company", "string", "company", "string"),
            ("snapshot.position", "string", "position", "string"),
            ("snapshot.createdOn", "bigint", "created_on", "timestamp")
        ]
    )

    # Apply transformations
    transformed_frame = mapped_frame.map(f=transform)
    # Filed emails is list , which we need to get first email from list if it's not empty, so drop emails field
    droppedColumn_frame = transformed_frame.drop_fields(paths=["emails"], transformation_ctx="droppedColumn_frame")
    # Use the catalog table to resolve any ambiguity
    output_frame = droppedColumn_frame.resolveChoice(choice='match_catalog', database=args['DATABASE_NAME'], table_name=args['TABLE_NAME'])

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