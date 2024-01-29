"""
The job process the file download records data.
"""

from awsglue.transforms import *
from utils import Utils
from glue_job import GlueJob

DOWNLOADED_FILE_HANDLE_ID = "downloaded_file_handle_id"
FILE_HANDLE_ID = "file_handle_id"
RECORD_DATE = "record_date"
ASSOCIATED_OBJECT_ID = "association_object_id"


class FileDownloadRecords(GlueJob):

    def __init__(self, mapping_list, partition_key):
        super().__init__(mapping_list, partition_key)

    def execute(self, dynamic_frame, logger):
        return dynamic_frame.map(lambda record: FileDownloadRecords.transform(record, logger))

    # process the file download record
    @staticmethod
    def transform(dynamic_record, logger):
        try:
            # This is the partition date
            dynamic_record[RECORD_DATE] = Utils.ms_to_partition_date(dynamic_record[RECORD_DATE])
            # The records come in with the syn prefix, we need to remove that
            dynamic_record[ASSOCIATED_OBJECT_ID] = Utils.syn_id_string_to_int(dynamic_record[ASSOCIATED_OBJECT_ID])
            # If downloaded file handle id is not present in the record, or it's null then file handle id should be assigned to it.
            if DOWNLOADED_FILE_HANDLE_ID not in dynamic_record.keys() or dynamic_record[DOWNLOADED_FILE_HANDLE_ID] is None:
                dynamic_record[DOWNLOADED_FILE_HANDLE_ID] = dynamic_record[FILE_HANDLE_ID]
        except Exception as error:
            logger.error("Error occurred in filedownloadrecords : ", error)

        return dynamic_record


if __name__ == "__main__":
    mapping_list = [
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
    file_download_records = FileDownloadRecords(mapping_list, RECORD_DATE)
