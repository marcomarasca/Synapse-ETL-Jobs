"""
The job process the certified user passing snapshot data.
"""

from awsglue.transforms import *
from glue_job import GlueJob
from utils import Utils

PARTITION_KEY = "snapshot_date"


class CertifiedQuizSnapshots(GlueJob):

    def __init__(self, mapping_list, partition_key):
        super().__init__(mapping_list, partition_key)

    def execute(self, dynamic_frame):
        return dynamic_frame.map(f=CertifiedQuizSnapshots.transform)

    # Process the certified quiz snapshot record
    @staticmethod
    def transform(dynamic_record):
        # This is the partition date
        dynamic_record[PARTITION_KEY] = Utils.ms_to_partition_date(dynamic_record[PARTITION_KEY])
        
        # The following fields were introduced by https://sagebionetworks.jira.com/browse/PLFM-8365
        # and need default values for older records
        if "revoked" not in dynamic_record or dynamic_record["revoked"] is None:
            dynamic_record["revoked"] = False
        
        if "certified" not in dynamic_record or dynamic_record["certified"] is None:
            dynamic_record["certified"] = dynamic_record["passed"]

        return dynamic_record


if __name__ == "__main__":
    mapping_list = [
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
        ("snapshot.passedOn", "bigint", "passed_on", "timestamp"),
        ("snapshot.revoked", "boolean", "revoked", "boolean"),
        ("snapshot.revokedOn", "bigint", "revoked_on", "timestamp"),
        ("snapshot.certified", "boolean", "certified", "boolean")
    ]
    certified_quiz_snapshots = CertifiedQuizSnapshots(mapping_list, PARTITION_KEY)
