"""
The job process the file upload records data.
"""

from awsglue.transforms import *
from utils import Utils
from glue_job import GlueJob

RECORD_DATE = "record_date"
ASSOCIATED_OBJECT_ID = "association_object_id"


class FileUploadRecords(GlueJob):

    def __init__(self, mapping_list, partition_key):
        super().__init__(mapping_list, partition_key)

    def execute(self, dynamic_frame):
        return dynamic_frame.map(f=FileUploadRecords.transform)

    # process the file upload record
    @staticmethod
    def transform(dynamic_record):
        # This is the partition date
        dynamic_record[RECORD_DATE] = Utils.ms_to_partition_date(dynamic_record[RECORD_DATE])

        # The records come in with the syn prefix, we need to remove that
        dynamic_record[ASSOCIATED_OBJECT_ID] = Utils.syn_id_string_to_int(dynamic_record[ASSOCIATED_OBJECT_ID])

        return dynamic_record


if __name__ == "__main__":
    mapping_list = [
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
    file_upload_records = FileUploadRecords(mapping_list, RECORD_DATE)
