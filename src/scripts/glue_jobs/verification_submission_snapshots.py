"""
The job process the verification submission snapshot data.
"""

from awsglue.transforms import *
from glue_job import GlueJob
from utils import Utils

PARTITION_KEY = "snapshot_date"


class VerificationSubmissionSnapshots(GlueJob):

    def __init__(self, mapping_list, partition_key):
        super().__init__(mapping_list, partition_key)

    def execute(self, dynamic_frame):
        return dynamic_frame.map(f=VerificationSubmissionSnapshots.transform)

    # Process the user profile snapshot record
    @staticmethod
    def transform(dynamic_record):
        # This is the partition date
        dynamic_record[PARTITION_KEY] = Utils.ms_to_partition_date(dynamic_record[PARTITION_KEY])
        return dynamic_record


if __name__ == "__main__":
    mapping_list = [
        ("changeType", "string", "change_type", "string"),
        ("changeTimestamp", "bigint", "change_timestamp", "timestamp"),
        ("snapshotTimestamp", "bigint", "snapshot_timestamp", "timestamp"),
        # Note that we map the same timestamp into a bigint so that we can extract the partition date
        ("snapshotTimestamp", "bigint", "snapshot_date", "bigint"),
        ("snapshot.id", "string", "id", "bigint"),
        ("snapshot.createdOn", "bigint", "created_on", "timestamp"),
        ("snapshot.createdBy", "string", "created_by", "bigint"),
        ("snapshot.stateHistory", "array", "state_history", "array")
    ]
    verification_submission_snapshots = VerificationSubmissionSnapshots(mapping_list, PARTITION_KEY)
