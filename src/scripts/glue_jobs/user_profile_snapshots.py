"""
The process the user profile snapshot data.
"""

from awsglue.transforms import *
from snapshot_glue_job import SnapshotGlueJob
from utils import Utils


class UserProfileSnapshots(SnapshotGlueJob):

    def __init__(self, mapping_list):
        super().__init__(mapping_list)

    def execute(self, dynamic_frame):
        transformed_frame = dynamic_frame.map(f=UserProfileSnapshots.transform)
        if transformed_frame.stageErrorsCount() > 0:
            self.log_errors(dynamic_frame)
        # Filed emails is list , which we need to get first email from list if it's not empty, so drop emails field
        droppedColumn_frame = transformed_frame.drop_fields(paths=["emails"], transformation_ctx="droppedColumn_frame")
        return droppedColumn_frame

    # Process the user profile snapshot record
    @staticmethod
    def transform(dynamic_record):
        # This is the partition date
        dynamic_record["snapshot_date"] = Utils.ms_to_partition_date(dynamic_record["snapshot_date"])
        # Get the first email if list is not empty and add email as field to dynamic frame
        dynamic_record["email"] = UserProfileSnapshots.get_email(dynamic_record["emails"])
        return dynamic_record

    @staticmethod
    def get_email(emails):
        if not emails:
            return None
        else:
            return emails[0]


if __name__ == "__main__":
    mapping_List = [
        ("changeTimestamp", "bigint", "change_timestamp", "timestamp"),
        ("changeType", "string", "change_type", "string"),
        ("userId", "bigint", "change_user_id", "bigint"),
        ("snapshotTimestamp", "bigint", "snapshot_timestamp", "timestamp"),
        # Note that we map the same timestamp into a bigint so that we can extract the partition date
        ("snapshotTimestamp", "bigint", "snapshot_date", "bigint"),
        ("snapshot.ownerId", "string", "id", "bigint"),
        ("snapshot.userName", "string", "user_name", "string"),
        ("snapshot.firstName", "string", "first_name", "string"),
        ("snapshot.lastName", "string", "last_name", "string"),
        ("snapshot.emails", "array", "emails", "array"),
        ("snapshot.location", "string", "location", "string"),
        ("snapshot.company", "string", "company", "string"),
        ("snapshot.position", "string", "position", "string"),
        ("snapshot.createdOn", "bigint", "created_on", "timestamp")
    ]
    user_profile_snapshots = UserProfileSnapshots(mapping_List)
