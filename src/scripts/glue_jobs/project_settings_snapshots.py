"""
The job process the project settings snapshot.
"""

from awsglue.transforms import *
from snapshot_glue_job import SnapshotGlueJob
from utils import Utils


class ProjectSettingSnapshots(SnapshotGlueJob):

    def __init__(self, mapping_list):
        super().__init__(mapping_list)

    def execute(self, dynamic_frame):
        return dynamic_frame.map(f=ProjectSettingSnapshots.transform)

    # Process the project setting snapshot record
    @staticmethod
    def transform(dynamic_record):
        # This is the partition date
        dynamic_record["snapshot_date"] = Utils.ms_to_partition_date(dynamic_record["snapshot_date"])
        if "project_id" in dynamic_record:
            # The records might come in with the syn prefix, we need to remove that
            dynamic_record["project_id"] = Utils.syn_id_string_to_int(dynamic_record["project_id"])
        return dynamic_record


if __name__ == "__main__":
    mapping_List = [
        ("changeType", "string", "change_type", "string"),
        ("changeTimestamp", "bigint", "change_timestamp", "timestamp"),
        ("userId", "bigint", "change_user_id", "bigint"),
        ("snapshotTimestamp", "bigint", "snapshot_timestamp", "timestamp"),
        # Note that we map the same timestamp into a bigint so that we can extract the partition date
        ("snapshotTimestamp", "bigint", "snapshot_date", "bigint"),
        ("snapshot.concreteType", "string", "concrete_type", "string"),
        ("snapshot.id", "string", "id", "bigint"),
        ("snapshot.projectId", "string", "project_id", "string"),
        ("snapshot.settingsType", "string", "settings_type", "string"),
        ("snapshot.etag", "string", "etag", "string"),
        ("snapshot.locations", "array", "locations", "array"),
    ]
    project_setting_snapshots = ProjectSettingSnapshots(mapping_List)
