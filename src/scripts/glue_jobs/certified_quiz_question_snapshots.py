"""
The job process the certified user passing snapshot data.
"""
from awsglue.transforms import *
from glue_job import GlueJob
from utils import Utils
import gs_explode

PARTITION_KEY = "snapshot_date"


class CertifiedQuizQuestionSnapshots(GlueJob):

    def __init__(self, mapping_list, partition_key):
        super().__init__(mapping_list, partition_key)

    def execute(self, dynamic_frame):
        # Apply transformations to compute the partition date and array of questionIndex and isCorrect values
        transformed_frame = dynamic_frame.map(f=CertifiedQuizQuestionSnapshots.transform)
        self.check_and_log_errors(transformed_frame)
        # Explode method creates separate row for each correction
        exploded_frame = transformed_frame.gs_explode(
            colName="corrections", newCol="correction"
        )
        self.check_and_log_errors(exploded_frame)
        # Map each rows into required table record
        mapped_output_frame = exploded_frame.apply_mapping(
            [
                ("changeTimestamp", "bigint", "change_timestamp", "timestamp"),
                ("changeType", "string", "change_type", "string"),
                ("snapshot.userId", "string", "change_user_id", "bigint"),
                ("snapshotTimestamp", "bigint", "snapshot_timestamp", "timestamp"),
                ("stack", "string", "stack", "string"),
                ("instance", "string", "instance", "string"),
                ("snapshot.responseId", "int", "response_id", "bigint"),
                ("correction.questionIndex", "int", "question_index", "bigint"),
                ("correction.isCorrect", "boolean", "is_correct", "boolean"),
                ("snapshot_date", "string", "snapshot_date", "date"),
                ("created_on", "bigint", "created_on", "timestamp")
            ]
        )
        return mapped_output_frame

    # Process the certified quiz question snapshot record
    @staticmethod
    def transform(dynamic_record):
        # Correction array contains questionIndex and isCorrect, which is need for quiz question record
        corrections = dynamic_record["snapshot"]["corrections"]
        correctionInfo = []
        for correction in corrections:
            info = {
                "questionIndex": correction["question"]["questionIndex"],
                "isCorrect": correction["isCorrect"]
            }
            correctionInfo.append(info)

        dynamic_record["corrections"] = correctionInfo
        # This is the partition date
        dynamic_record[PARTITION_KEY] = Utils.ms_to_partition_date(dynamic_record["snapshotTimestamp"])

        # The createdOn field was introduced in https://sagebionetworks.jira.com/browse/PLFM-8788
        # and need default values for older records. We use the deprecated "passedOn" that was actually 
        # matching the creation date of the record and was never used to indicate when the quiz was passed
        if "createdOn" not in dynamic_record["snapshot"] or dynamic_record["snapshot"]["createdOn"] is None:
            createdOn = dynamic_record["snapshot"]["passedOn"]
        else:
            createdOn = dynamic_record["snapshot"]["createdOn"]
        
        # We cannot change nested fields (e.g. dynamic_record["snaphost"]["createdOn"] = createdOn does not work) in the dynamic_record
        # without transforming it to a data frame so we just use a new column
        dynamic_record["created_on"] = createdOn

        return dynamic_record


if __name__ == "__main__":
    # There is no mapping required before processing the certified quiz question record
    certified_quiz_question_snapshots = CertifiedQuizQuestionSnapshots([], PARTITION_KEY)

