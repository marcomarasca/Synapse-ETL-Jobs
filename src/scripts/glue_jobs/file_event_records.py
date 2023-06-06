"""
The job take the file event records from S3 and process it.
Processed data stored in S3 in a parquet file partitioned by the date (%Y-%m-%d pattern) of the record timestamp.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from utils import ms_to_partition_date
from utils import syn_id_string_to_int

# process the file event record
def transform(dynamic_record):
    # This is the partition date
    dynamic_record["record_date"] = ms_to_partition_date(dynamic_record["record_date"])

    # The records come in with the syn prefix, we need to remove that
    dynamic_record["association_object_id"] = syn_id_string_to_int(dynamic_record["association_object_id"])

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
            ("payload.userId", "bigint", "user_id", "bigint"),
            ("timestamp", "bigint", "timestamp", "timestamp"),
            # we need to map the same timestamp into a bigint so that we can extract the partition date
            ("timestamp", "bigint", "record_date", "bigint"),
            ("payload.projectId", "bigint", "project_id", "bigint"),
            ("payload.fileHandleId", "string", "file_handle_id", "bigint"),
            ("payload.associateType", "string", "association_object_type", "string"),
            ("payload.associateId", "string", "association_object_id", "string"),
            ("stack", "string", "stack", "string"),
            ("instance", "string", "instance", "string")
        ]
    )

    # Apply transformations
    transformed_frame = mapped_frame.map(f=transform, info='file_transform')

    # Use the catalog table to resolve any ambiguity
    output_frame = transformed_frame.resolveChoice(choice='match_catalog', database=args['DATABASE_NAME'], table_name=args['TABLE_NAME'])

    # Write only if there is new data (this will error out otherwise)
    if (output_frame.count() > 0):
        glue_context.write_dynamic_frame.from_catalog(
            frame=output_frame,
            database=args["DATABASE_NAME"],
            table_name=args["TABLE_NAME"],
            additional_options={"partitionKeys": ["record_date"]}
        )

    job.commit()

if __name__ == "__main__":
    main()