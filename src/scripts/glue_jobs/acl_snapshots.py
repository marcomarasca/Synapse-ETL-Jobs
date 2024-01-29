"""
The job process the acl snapshot data.
"""

from awsglue.transforms import *
from glue_job import GlueJob
from utils import Utils

PARTITION_KEY = "snapshot_date"
OWNER_ID = "owner_id"

class AclSnapshots(GlueJob):

    def __init__(self, mapping_list, partition_key):
        super().__init__(mapping_list, partition_key)

    def execute(self, dynamic_frame, logger):
        return dynamic_frame.map(lambda record: AclSnapshots.transform(record, logger))

    # Process the acl snapshot record
    @staticmethod
    def transform(dynamic_record, logger):
        try:
            # This is the partition date
            dynamic_record[PARTITION_KEY] = Utils.ms_to_partition_date(dynamic_record[PARTITION_KEY])
            # The records come in with the syn prefix, we need to remove that
            dynamic_record[OWNER_ID] = Utils.syn_id_string_to_int(dynamic_record[OWNER_ID])
        except Exception as error:
            logger.error("Error occurred in aclsnapshots : ", error)

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
    acl_snapshots = AclSnapshots(mapping_list, PARTITION_KEY)
