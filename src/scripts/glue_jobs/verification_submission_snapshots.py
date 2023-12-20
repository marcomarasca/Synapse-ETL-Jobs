"""
The job process the verification submission snapshot data.
"""

from awsglue.transforms import *
from snapshot_glue_job import SnapshotGlueJob
from utils import Utils


class VerificationSubmissionSnapshots(SnapshotGlueJob):

    def __init__(self, mapping_list):
        super().__init__(mapping_list)

    def execute(self, dynamic_frame):
        return dynamic_frame.map(f=VerificationSubmissionSnapshots.transform)

    # Process the user profile snapshot record
    @staticmethod
    def transform(dynamic_record):
        # This is the partition date
        dynamic_record["snapshot_date"] = Utils.ms_to_partition_date(dynamic_record["snapshot_date"])
        return dynamic_record


if __name__ == "__main__":
    mapping_List = [
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
    verification_submission_snapshots = VerificationSubmissionSnapshots(mapping_List)
