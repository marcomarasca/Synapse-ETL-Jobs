"""
The job take the file download records from S3 and process it.
Processed data stored in S3 in a parquet file partitioned by the date (%Y-%m-%d pattern) of the record timestamp.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from utils import Utils

DOWNLOADED_FILE_HANDLE_ID = "downloaded_file_handle_id"
FILE_HANDLE_ID = "file_handle_id"
RECORD_DATE = "record_date"
ASSOCIATED_OBJECT_ID = "association_object_id"


# process the file download record
def transform(dynamic_record):
    # This is the partition date
    dynamic_record[RECORD_DATE] = Utils.ms_to_partition_date(dynamic_record[RECORD_DATE])
    # The records come in with the syn prefix, we need to remove that
    dynamic_record[ASSOCIATED_OBJECT_ID] = Utils.syn_id_string_to_int(dynamic_record[ASSOCIATED_OBJECT_ID])
    # If downloaded file handle id is not present in the record, or it's null then file handle id should be assigned to it.
    if DOWNLOADED_FILE_HANDLE_ID not in dynamic_record.keys() or dynamic_record[DOWNLOADED_FILE_HANDLE_ID] is None:
        dynamic_record[DOWNLOADED_FILE_HANDLE_ID] = dynamic_record[FILE_HANDLE_ID]

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
            ("payload.downloadedFileHandleId", "string", "downloaded_file_handle_id", "bigint"),
            ("payload.associateType", "string", "association_object_type", "string"),
            ("payload.associateId", "string", "association_object_id", "string"),
            ("payload.sessionId", "string", "session_id", "string"),
            ("stack", "string", "stack", "string"),
            ("instance", "string", "instance", "string")
        ]
    )

    # Apply transformations
    transformed_frame = mapped_frame.map(f=transform, info='file_transform')

    # Use the catalog table to resolve any ambiguity
    output_frame = transformed_frame.resolveChoice(choice='match_catalog', database=args['DATABASE_NAME'],
                                                   table_name=args['TABLE_NAME'])

    # Write only if there is new data (this will error out otherwise)
    if output_frame.count() > 0:
        glue_context.write_dynamic_frame.from_catalog(
            frame=output_frame,
            database=args["DATABASE_NAME"],
            table_name=args["TABLE_NAME"],
            additional_options={"partitionKeys": [RECORD_DATE]}
        )

    job.commit()


if __name__ == "__main__":
    main()
