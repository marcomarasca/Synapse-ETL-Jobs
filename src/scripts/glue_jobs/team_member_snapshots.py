"""
The job process the team member snapshot data.
"""

from awsglue.transforms import *
from glue_job import GlueJob
from utils import Utils

PARTITION_KEY = "snapshot_date"


class TeamMemberSnapshots(GlueJob):

    def __init__(self, mapping_list, partition_key):
        super().__init__(mapping_list, partition_key)

    def execute(self, dynamic_frame, logger):
        return dynamic_frame.map(lambda record: TeamMemberSnapshots.transform(record, logger))

    # Process the team member snapshot record
    @staticmethod
    def transform(dynamic_record, logger):
        try:
            dynamic_record[PARTITION_KEY] = Utils.ms_to_partition_date(dynamic_record[PARTITION_KEY])
        except Exception as error:
            logger.error("Error occurred in teammembersnapshots : ", error)

        return dynamic_record


if __name__ == "__main__":
    mapping_list = [
        ("changeTimestamp", "bigint", "change_timestamp", "timestamp"),
        ("changeType", "string", "change_type", "string"),
        ("userId", "bigint", "change_user_id", "bigint"),
        ("snapshotTimestamp", "bigint", "snapshot_timestamp", "timestamp"),
        # Note that we map the same timestamp into a bigint so that we can extract the partition date
        ("snapshotTimestamp", "bigint", "snapshot_date", "bigint"),
        ("snapshot.teamId", "string", "team_id", "bigint"),
        ("snapshot.member.ownerId", "string", "member_id", "bigint"),
        ("snapshot.isAdmin", "boolean", "is_admin", "boolean"),
    ]
    team_member_snapshot = TeamMemberSnapshots(mapping_list, PARTITION_KEY)
