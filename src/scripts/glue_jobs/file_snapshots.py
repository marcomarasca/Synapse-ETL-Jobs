"""
The job process the file handle snapshot data.
"""

from awsglue.transforms import *
from snapshot_glue_job import SnapshotGlueJob
from utils import Utils


class FileSnapshots(SnapshotGlueJob):

    def __init__(self, mapping_list):
        super().__init__(mapping_list)

    def execute(self, dynamic_frame):
        transformed_frame = dynamic_frame.map(f=FileSnapshots.transform)
        if transformed_frame.stageErrorsCount() > 0:
            transformed_frame.errorsAsDynamicFrame().toDF().show()
            raise Exception("Error in job! See the log!")
        return transformed_frame

    # Process the file snapshot record
    @staticmethod
    def transform(dynamic_record):
        dynamic_record["snapshot_date"] = Utils.ms_to_partition_date(dynamic_record["snapshot_date"])
        return dynamic_record


if __name__ == "__main__":
    mapping_List = [
        ("changeType", "string", "change_type", "string"),
        ("changeTimestamp", "bigint", "change_timestamp", "timestamp"),
        ("userId", "bigint", "change_user_id", "bigint"),
        ("snapshotTimestamp", "bigint", "snapshot_timestamp", "timestamp"),
        # Note that we map the same timestamp into a bigint so that we can extract the partition date
        ("snapshotTimestamp", "bigint", "snapshot_date", "bigint"),
        ("snapshot.id", "string", "id", "bigint"),
        ("snapshot.createdBy", "string", "created_by", "bigint"),
        ("snapshot.createdOn", "bigint", "created_on", "timestamp"),
        ("snapshot.modifiedOn", "bigint", "modified_on", "timestamp"),
        ("snapshot.concreteType", "string", "concrete_type", "string"),
        ("snapshot.contentMd5", "string", "content_md5", "string"),
        ("snapshot.contentType", "string", "content_type", "string"),
        ("snapshot.fileName", "string", "file_name", "string"),
        ("snapshot.storageLocationId", "bigint", "storage_location_id", "bigint"),
        ("snapshot.contentSize", "bigint", "content_size", "bigint"),
        ("snapshot.bucket", "string", "bucket", "string"),
        ("snapshot.key", "string", "key", "string"),
        ("snapshot.previewId", "string", "preview_id", "bigint"),
        ("snapshot.isPreview", "boolean", "is_preview", "boolean"),
        ("snapshot.status", "string", "status", "string")
    ]
    file_snapshots = FileSnapshots(mapping_List)
