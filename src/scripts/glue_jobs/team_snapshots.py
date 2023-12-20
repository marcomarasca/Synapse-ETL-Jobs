"""
The job process the team snapshot data.
"""

from awsglue.transforms import *
from snapshot_glue_job import SnapshotGlueJob
from utils import Utils


class TeamSnapshots(SnapshotGlueJob):
    def __init__(self, mapping_list):
        super().__init__(mapping_list)

    def execute(self, dynamic_frame):
        return dynamic_frame.map(f=TeamSnapshots.transform)

    # Process the team snapshot record
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
        ("snapshot.name", "string", "name", "string"),
        ("snapshot.canPublicJoin", "boolean", "can_public_join", "boolean"),
        ("snapshot.createdOn", "bigint", "created_on", "timestamp"),
        ("snapshot.modifiedOn", "bigint", "modified_on", "timestamp"),
        ("snapshot.createdBy", "string", "created_by", "bigint"),
        ("snapshot.modifiedBy", "string", "modified_by", "bigint"),
    ]
    team_snapshots = TeamSnapshots(mapping_List)
