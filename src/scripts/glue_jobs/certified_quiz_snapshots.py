"""
The job process the certified user passing snapshot data.
"""

from awsglue.transforms import *
from snapshot_glue_job import SnapshotGlueJob
from utils import Utils


class CertifiedQuizSnapshots(SnapshotGlueJob):

    def __init__(self, mapping_list):
        super().__init__(mapping_list)

    def execute(self, dynamic_frame):
        return dynamic_frame.map(f=CertifiedQuizSnapshots.transform)

    # Process the certified quiz snapshot record
    @staticmethod
    def transform(dynamic_record):
        # This is the partition date
        dynamic_record["snapshot_date"] = Utils.ms_to_partition_date(dynamic_record["snapshot_date"])
        return dynamic_record


if __name__ == "__main__":
    mapping_List = [
        ("changeTimestamp", "bigint", "change_timestamp", "timestamp"),
        ("changeType", "string", "change_type", "string"),
        ("snapshotTimestamp", "bigint", "snapshot_timestamp", "timestamp"),
        # Note that we map the same timestamp into a bigint so that we can extract the partition date
        ("snapshotTimestamp", "bigint", "snapshot_date", "bigint"),
        ("stack", "string", "stack", "string"),
        ("instance", "string", "instance", "string"),
        ("snapshot.userId", "string", "user_id", "bigint"),
        ("snapshot.responseId", "bigint", "response_id", "bigint"),
        ("snapshot.passed", "boolean", "passed", "boolean"),
        ("snapshot.passedOn", "bigint", "passed_on", "timestamp")
    ]
    certified_quiz_snapshots = CertifiedQuizSnapshots(mapping_List)
