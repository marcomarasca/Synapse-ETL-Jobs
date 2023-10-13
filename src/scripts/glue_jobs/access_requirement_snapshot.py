"""
The job take the access requirements snapshot data from S3 and process it.
Processed data stored in S3 in a parquet file partitioned by the date (%Y-%m-%d pattern) of the snapshot timestamp.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from utils import ms_to_partition_date
from utils import syn_id_string_to_int

# process the acl record
def transform(dynamic_record):
    # This is the partition date
    dynamic_record["snapshot_date"] = ms_to_partition_date(dynamic_record["snapshot_date"])
    # The records might come in with the syn prefix, we need to remove that
    dynamic_record["duc_template_file_handle_id"] = syn_id_string_to_int(dynamic_record["duc_template_file_handle_id"])
    return dynamic_record

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "DATABASE_NAME", "TABLE_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    input_frame = glue_context.create_dynamic_frame.from_options(
        format_options={"multiline": True},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [args["S3_SOURCE_PATH"]],
            "recurse": True
        },
        # Note: even though this is optional, job bookmark does not work without it
        transformation_ctx="input_frame"
    )

    # Maps the incoming record to flatten table
    mapped_frame = input_frame.apply_mapping(
        [
            ("changeType", "string", "change_type", "string"),
            ("changeTimestamp", "bigint", "change_timestamp", "timestamp"),
            ("userId", "bigint",   "change_user_id", "bigint"),
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
    )

    # Apply transformations (compute the partition)
    transformed_frame = mapped_frame.map(f=transform)

    # Use the catalog table to resolve any ambiguity
    output_frame = transformed_frame.resolveChoice(choice='match_catalog', database=args['DATABASE_NAME'], table_name=args['TABLE_NAME'])

    # Write only if there is new data (this will error out otherwise)
    if (output_frame.count() > 0):
        glue_context.write_dynamic_frame.from_catalog(
            frame=output_frame,
            database=args["DATABASE_NAME"],
            table_name=args["TABLE_NAME"],
            additional_options={"partitionKeys": ["snapshot_date"]}
        )

    job.commit()

if __name__ == "__main__":
    main()