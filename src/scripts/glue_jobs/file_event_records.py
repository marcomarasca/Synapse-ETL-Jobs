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

from dynamic_frame_mapping import *
from dynamic_record_transformation import *


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "DATABASE_NAME", "TABLE_NAME", "FILE_EVENT_TYPE"])
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

    if args["FILE_EVENT_TYPE"] == "download":
        mapped_frame = file_download_record_mapping(input_frame)
        transformed_frame = mapped_frame.map(f=file_download_record_transformation, info='file_transform')
    else:
        mapped_frame = file_upload_record_mapping(input_frame)
        transformed_frame = mapped_frame.map(f=file_upload_record_transformation, info='file_transform')

    # Use the catalog table to resolve any ambiguity
    output_frame = transformed_frame.resolveChoice(choice='match_catalog', database=args['DATABASE_NAME'],
                                                   table_name=args['TABLE_NAME'])

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