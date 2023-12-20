"""
The job process the node snapshot data.
"""

from awsglue.transforms import *
from snapshot_glue_job import SnapshotGlueJob
from utils import Utils


class NodeSnapshots(SnapshotGlueJob):

    def __init__(self, mapping_list):
        super().__init__(mapping_list)

    def execute(self, dynamic_frame):
        transformed_frame = dynamic_frame.map(f=NodeSnapshots.transform)
        if transformed_frame.stageErrorsCount() > 0:
            self.log_errors(transformed_frame)
        return transformed_frame

    # Process the node snapshot record
    @staticmethod
    def transform(dynamic_record):
        # This is the partition date
        dynamic_record["snapshot_date"] = Utils.ms_to_partition_date(dynamic_record["snapshot_date"])

        # The records come in with the syn prefix, we need to remove that
        dynamic_record["id"] = Utils.syn_id_string_to_int(dynamic_record["id"])
        dynamic_record["benefactor_id"] = Utils.syn_id_string_to_int(dynamic_record["benefactor_id"])
        dynamic_record["project_id"] = Utils.syn_id_string_to_int(dynamic_record["project_id"])
        dynamic_record["parent_id"] = Utils.syn_id_string_to_int(dynamic_record["parent_id"])
        dynamic_record["file_handle_id"] = Utils.syn_id_string_to_int(dynamic_record["file_handle_id"])
        return dynamic_record


if __name__ == "__main__":
    mapping_List = [
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
        ("snapshot.effectiveArs", "array", "effective_ars", "array")
    ]
    node_snapshots = NodeSnapshots(mapping_List)
