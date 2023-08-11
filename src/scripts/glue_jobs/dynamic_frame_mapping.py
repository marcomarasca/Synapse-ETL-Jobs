"""
DynamicFrame : A Dynamic Frame is similar to an Apache Spark dataframe, which is a data abstraction used to
organize data into rows and columns, except that each record is self-describing so no schema is required initially.
see link: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame.html

DynamicRecord : a DynamicRecord represents a logical record within a DynamicFrame. It is like a row in a Spark
DataFrame. see link: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame.html

Glue reads data from s3 source and creates a dynamic frame. Glue computes the schema for dynamic frame. Apply mapping
methods changes the schema, e.g, field name and datatype. """

from awsglue.transforms import *

def file_download_record_mapping(input_frame):
    mapped_frame = input_frame.apply_mapping(
        [
            ("payload.userId", "bigint", "user_id", "bigint"),
            ("timestamp", "bigint", "timestamp", "timestamp"),
            # we need to map the same timestamp into a bigint so that we can extract the partition date
            ("timestamp", "bigint", "record_date", "bigint"),
            ("payload.projectId", "bigint", "project_id", "bigint"),
            ("payload.fileHandleId", "string", "requested_file_handle_id", "bigint"),
            ("payload.downloadedFileHandleId", "string", "downloaded_file_handle_id", "bigint"),
            ("payload.associateType", "string", "association_object_type", "string"),
            ("payload.associateId", "string", "association_object_id", "string"),
            ("stack", "string", "stack", "string"),
            ("instance", "string", "instance", "string")
        ]
    )
    return mapped_frame

def file_upload_record_mapping(input_frame):
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
    return mapped_frame
