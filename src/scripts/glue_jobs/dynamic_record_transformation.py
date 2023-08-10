"""
DynamicFrame : A Dynamic Frame is similar to an Apache Spark dataframe, which is a data abstraction used to
organize data into rows and columns, except that each record is self-describing so no schema is required initially.
see link: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame.html

DynamicRecord : a DynamicRecord represents a logical record within a DynamicFrame. It is like a row in a Spark
DataFrame. see link: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame.html

This script is used to perform transformation on dynamic record, e.g, computing value for a column.

"""

from utils import *

DOWNLOADED_FILE_HANDLE_ID = "downloaded_file_handle_id"
FILE_HANDLE_ID = "file_handle_id"
RECORD_DATE = "record_date"
ASSOCIATED_OBJECT_ID = "association_object_id"

def file_download_record_transformation(dynamic_record):
    # This is the partition date
    dynamic_record[RECORD_DATE] = ms_to_partition_date(dynamic_record[RECORD_DATE])
    # The records come in with the syn prefix, we need to remove that
    dynamic_record[ASSOCIATED_OBJECT_ID] = syn_id_string_to_int(dynamic_record[ASSOCIATED_OBJECT_ID])
    downloaded_id = None
    if DOWNLOADED_FILE_HANDLE_ID in dynamic_record.keys():
        downloaded_id = dynamic_record[DOWNLOADED_FILE_HANDLE_ID]
    if downloaded_id is None:
        dynamic_record[DOWNLOADED_FILE_HANDLE_ID] = dynamic_record[FILE_HANDLE_ID]

    return dynamic_record


def file_upload_record_transformation(dynamic_record):
    # This is the partition date
    dynamic_record[RECORD_DATE] = ms_to_partition_date(dynamic_record[RECORD_DATE])

    # The records come in with the syn prefix, we need to remove that
    dynamic_record[ASSOCIATED_OBJECT_ID] = syn_id_string_to_int(dynamic_record[ASSOCIATED_OBJECT_ID])

    return dynamic_record
