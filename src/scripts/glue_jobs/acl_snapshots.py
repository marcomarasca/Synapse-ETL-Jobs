"""
The job process the acl snapshot data.
"""

from awsglue.transforms import *
from snapshot_glue_job import SnapshotGlueJob
from utils import Utils


class AclSnapshots(SnapshotGlueJob):

    def __init__(self, mapping_list):
        super().__init__(mapping_list)

    def execute(self, dynamic_frame):
        return dynamic_frame.map(f=AclSnapshots.transform)

    # Process the acl snapshot record
    @staticmethod
    def transform(dynamic_record):
        # This is the partition date
        dynamic_record["snapshot_date"] = Utils.ms_to_partition_date(dynamic_record["snapshot_date"])
        # The records come in with the syn prefix, we need to remove that
        dynamic_record["owner_id"] = Utils.syn_id_string_to_int(dynamic_record["owner_id"])
        return dynamic_record


if __name__ == "__main__":
    mapping_list = [
        ("changeType", "string", "change_type", "string"),
        ("changeTimestamp", "bigint", "change_timestamp", "timestamp"),
        ("snapshotTimestamp", "bigint", "snapshot_timestamp", "timestamp"),
        # Note that we map the same timestamp into a bigint so that we can extract the partition date
        ("snapshotTimestamp", "bigint", "snapshot_date", "bigint"),
        ("snapshot.id", "string", "owner_id", "string"),
        ("snapshot.ownerType", "string", "owner_type", "string"),
        ("snapshot.creationDate", "bigint", "created_on", "timestamp"),
        ("snapshot.resourceAccess", "array", "resource_access", "array")
    ]
    acl_snapshots = AclSnapshots(mapping_list)
