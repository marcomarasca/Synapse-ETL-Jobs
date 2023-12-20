"""
The process the user group snapshot data.
"""

import sys
from awsglue.transforms import *
from snapshot_glue_job import SnapshotGlueJob
from utils import Utils


class UserGroupSnapshots(SnapshotGlueJob):
    def __init__(self, mapping_list):
        super().__init__(mapping_list)

    def execute(self, dynamic_frame):
        return dynamic_frame.map(f=UserGroupSnapshots.transform)

    # Process the user group snapshot record
    @staticmethod
    def transform(dynamic_record):
        dynamic_record["snapshot_date"] = Utils.ms_to_partition_date(dynamic_record["snapshot_date"])
        return dynamic_record


if __name__ == "__main__":
    mapping_List = [
        ("changeTimestamp", "bigint", "change_timestamp", "timestamp"),
        ("changeType", "string", "change_type", "string"),
        ("userId", "bigint", "change_user_id", "bigint"),
        ("snapshotTimestamp", "bigint", "snapshot_timestamp", "timestamp"),
        # Note that we map the same timestamp into a bigint so that we can extract the partition date
        ("snapshotTimestamp", "bigint", "snapshot_date", "bigint"),
        ("snapshot.id", "string", "id", "bigint"),
        ("snapshot.isIndividual", "boolean", "is_individual", "boolean"),
        ("snapshot.creationDate", "bigint", "created_on", "timestamp"),
    ]
    user_group_snapshots = UserGroupSnapshots(mapping_List)
