"""
The process the user profile snapshot data.
"""

from awsglue.transforms import *
from glue_job import GlueJob
from utils import Utils

PARTITION_KEY = "snapshot_date"
EMAIL = "email"
EMAILS = "emails"


class UserProfileSnapshots(GlueJob):

    def __init__(self, mapping_list, partition_key):
        super().__init__(mapping_list, partition_key)

    def execute(self, dynamic_frame):
        transformed_frame = dynamic_frame.map(f=UserProfileSnapshots.transform)
        self.check_and_log_errors(transformed_frame)
        # Filed emails is list , which we need to get first email from list if it's not empty, so drop emails field
        droppedColumn_frame = transformed_frame.drop_fields(paths=[EMAILS], transformation_ctx="droppedColumn_frame")
        return droppedColumn_frame

    # Process the user profile snapshot record
    @staticmethod
    def transform(dynamic_record):
        # This is the partition date
        dynamic_record[PARTITION_KEY] = Utils.ms_to_partition_date(dynamic_record[PARTITION_KEY])
        # Get the first email if list is not empty and add email as field to dynamic frame
        dynamic_record[EMAIL] = UserProfileSnapshots.get_email(dynamic_record[EMAILS])
        return dynamic_record

    @staticmethod
    def get_email(emails):
        if not emails:
            return None
        else:
            return emails[0]


if __name__ == "__main__":
    mapping_list = [
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
        ("snapshot.industry", "string", "industry", "string"),
        ("snapshot.createdOn", "bigint", "created_on", "timestamp"),
        ("snapshot.twoFactorAuthEnabled", "boolean", "is_two_factor_auth_enabled", "boolean"),
        ("snapshot.tosAgreements", "array", "tos_agreements", "array")
    ]
    user_profile_snapshots = UserProfileSnapshots(mapping_list, PARTITION_KEY)
