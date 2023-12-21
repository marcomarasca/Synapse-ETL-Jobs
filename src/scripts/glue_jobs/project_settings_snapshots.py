"""
The job process the project settings snapshot.
"""

from awsglue.transforms import *
from glue_job import GlueJob
from utils import Utils

PARTITION_KEY = "snapshot_date"
PROJECT_ID = "project_id"

class ProjectSettingSnapshots(GlueJob):

    def __init__(self, mapping_list, partition_key):
        super().__init__(mapping_list, partition_key)

    def execute(self, dynamic_frame):
        return dynamic_frame.map(f=ProjectSettingSnapshots.transform)

    # Process the project setting snapshot record
    @staticmethod
    def transform(dynamic_record):
        # This is the partition date
        dynamic_record[PARTITION_KEY] = Utils.ms_to_partition_date(dynamic_record[PARTITION_KEY])
        if PROJECT_ID in dynamic_record:
            # The records might come in with the syn prefix, we need to remove that
            dynamic_record[PROJECT_ID] = Utils.syn_id_string_to_int(dynamic_record[PROJECT_ID])
        return dynamic_record


if __name__ == "__main__":
    mapping_list = [
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
    project_setting_snapshots = ProjectSettingSnapshots(mapping_list, PARTITION_KEY)
