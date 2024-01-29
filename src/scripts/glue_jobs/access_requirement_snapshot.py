"""
The job process the access requirements snapshot data.
"""

from awsglue.transforms import *
from glue_job import GlueJob
from utils import Utils

PARTITION_KEY = "snapshot_date"
DUC_TEMPLATE_FILE_HANDLE_ID = "duc_template_file_handle_id"


class AccessRequirementSnapshots(GlueJob):

    def __init__(self, mapping_list, partition_key):
        super().__init__(mapping_list, partition_key)

    def execute(self, dynamic_frame, logger):
        return dynamic_frame.map(lambda record: AccessRequirementSnapshots.transform(record, logger))

    # Process the access requirement snapshot record
    @staticmethod
    def transform(dynamic_record, logger):
        try:
            # This is the partition date
            dynamic_record[PARTITION_KEY] = Utils.ms_to_partition_date(dynamic_record[PARTITION_KEY])

            # The "duc_template_file_handle_id" is not present in every type of AR
            if DUC_TEMPLATE_FILE_HANDLE_ID in dynamic_record:
                # The records might come in with the syn prefix, we need to remove that
                dynamic_record[DUC_TEMPLATE_FILE_HANDLE_ID] = Utils.syn_id_string_to_int(
                    dynamic_record[DUC_TEMPLATE_FILE_HANDLE_ID])
        except Exception as error:
            logger.error("Error occurred in accessrequirementsnapshots : ", error)

        return dynamic_record


if __name__ == "__main__":
    mapping_list = [
        ("changeType", "string", "change_type", "string"),
        ("changeTimestamp", "bigint", "change_timestamp", "timestamp"),
        ("userId", "bigint", "change_user_id", "bigint"),
        ("snapshotTimestamp", "bigint", "snapshot_timestamp", "timestamp"),
        # Note that we map the same timestamp into a bigint so that we can extract the partition date
        ("snapshotTimestamp", "bigint", "snapshot_date", "bigint"),
        ("snapshot.id", "bigint", "id", "bigint"),
        ("snapshot.versionNumber", "bigint", "version_number", "bigint"),
        ("snapshot.name", "string", "name", "string"),
        ("snapshot.description", "string", "description", "string"),
        ("snapshot.createdBy", "string", "created_by", "bigint"),
        ("snapshot.modifiedBy", "string", "modified_by", "bigint"),
        ("snapshot.createdOn", "bigint", "created_on", "timestamp"),
        ("snapshot.modifiedOn", "bigint", "modified_on", "timestamp"),
        ("snapshot.accessType", "string", "access_type", "string"),
        ("snapshot.concreteType", "string", "concrete_type", "string"),
        ("snapshot.subjectsDefinedByAnnotations", "boolean", "subjects_defined_by_annotations", "boolean"),
        ("snapshot.subjectIds", "array", "subjects_ids", "array"),
        # Next two properties only for ManagedACTAccessRequirement and SelfSignAccessRequirement
        ("snapshot.isCertifiedUserRequired", "boolean", "is_certified_user_required", "boolean"),
        ("snapshot.isValidatedProfileRequired", "boolean", "is_validated_profile_required", "boolean"),

        ("snapshot.isDUCRequired", "boolean", "is_duc_required", "boolean"),
        ("snapshot.isIRBApprovalRequired", "boolean", "is_irb_approval_required", "boolean"),
        ("snapshot.areOtherAttachmentsRequired", "boolean", "are_other_attachments_required", "boolean"),
        ("snapshot.isIDUPublic", "boolean", "is_idu_public", "boolean"),
        ("snapshot.isIDURequired", "boolean", "is_idu_required", "boolean"),
        ("snapshot.isTwoFaRequired", "boolean", "is_two_fa_required", "boolean"),
        ("snapshot.ducTemplateFileHandleId", "string", "duc_template_file_handle_id", "string"),
        ("snapshot.expirationPeriod", "bigint", "expiration_period", "bigint"),
        # Next property only for  SelfSignAccessRequirement
        ("snapshot.termsOfUse", "string", "terms_of_use", "string"),

        # Next two properties only for ACTAccessRequirement
        ("snapshot.actContactInfo", "string", "act_contact_info", "string"),
        ("snapshot.openJiraIssue", "boolean", "open_jira_issue", "boolean"),

        # Next property only for LockAccessRequirement
        ("snapshot.jiraKey", "string", "jira_key", "string"),

        # Next property only for PostMessageContentAccessRequirement
        ("snapshot.url", "string", "url", "string")
    ]
    access_requirement_snapshots = AccessRequirementSnapshots(mapping_list, PARTITION_KEY)
