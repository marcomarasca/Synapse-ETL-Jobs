"""
The job process the node snapshot data.
"""

from awsglue.transforms import *
from glue_job import GlueJob
from utils import Utils

PARTITION_KEY = "snapshot_date"
ID = "id"
BENEFACTOR_ID = "benefactor_id"
PROJECT_ID = "project_id"
PARENT_ID = "parent_id"
FILE_HANDLE_ID = "file_handle_id"


class NodeSnapshots(GlueJob):

    def __init__(self, mapping_list, partition_key):
        super().__init__(mapping_list, partition_key)

    def execute(self, dynamic_frame):
        return dynamic_frame.map(f=NodeSnapshots.transform)

    # Process the node snapshot record
    @staticmethod
    def transform(dynamic_record):
        # This is the partition date
        dynamic_record[PARTITION_KEY] = Utils.ms_to_partition_date(dynamic_record[PARTITION_KEY])

        # The records come in with the syn prefix, we need to remove that
        dynamic_record[ID] = Utils.syn_id_string_to_int(dynamic_record[ID])
        dynamic_record[BENEFACTOR_ID] = Utils.syn_id_string_to_int(dynamic_record[BENEFACTOR_ID])
        dynamic_record[PROJECT_ID] = Utils.syn_id_string_to_int(dynamic_record[PROJECT_ID])
        dynamic_record[PARENT_ID] = Utils.syn_id_string_to_int(dynamic_record[PARENT_ID])
        dynamic_record[FILE_HANDLE_ID] = Utils.syn_id_string_to_int(dynamic_record[FILE_HANDLE_ID])
        return dynamic_record


if __name__ == "__main__":
    mapping_list = [
        ("changeType", "string", "change_type", "string"),
        ("changeTimestamp", "bigint", "change_timestamp", "timestamp"),
        ("userId", "bigint", "change_user_id", "bigint"),
        ("snapshotTimestamp", "bigint", "snapshot_timestamp", "timestamp"),
        # Note that we map the same timestamp into a bigint so that we can extract the partition date
        ("snapshotTimestamp", "bigint", "snapshot_date", "bigint"),
        ("snapshot.id", "string", "id", "string"),
        ("snapshot.benefactorId", "string", "benefactor_id", "string"),
        ("snapshot.projectId", "string", "project_id", "string"),
        ("snapshot.parentId", "string", "parent_id", "string"),
        ("snapshot.nodeType", "string", "node_type", "string"),
        ("snapshot.createdOn", "bigint", "created_on", "timestamp"),
        ("snapshot.createdByPrincipalId", "bigint", "created_by", "bigint"),
        ("snapshot.modifiedOn", "bigint", "modified_on", "timestamp"),
        ("snapshot.modifiedByPrincipalId", "bigint", "modified_by", "bigint"),
        ("snapshot.versionNumber", "bigint", "version_number", "bigint"),
        ("snapshot.fileHandleId", "string", "file_handle_id", "string"),
        ("snapshot.name", "string", "name", "string"),
        ("snapshot.isPublic", "boolean", "is_public", "boolean"),
        ("snapshot.isControlled", "boolean", "is_controlled", "boolean"),
        ("snapshot.isRestricted", "boolean", "is_restricted", "boolean"),
        ("snapshot.effectiveArs", "array", "effective_ars", "array"),
        ("snapshot.annotations", "string", "annotations", "string"),
        ("snapshot.derivedAnnotations", "string", "derived_annotations", "string")
    ]
    node_snapshots = NodeSnapshots(mapping_list, PARTITION_KEY)
