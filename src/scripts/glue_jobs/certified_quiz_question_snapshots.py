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
        if transformed_frame.stageErrorsCount() > 0:
            self.log_errors(transformed_frame)
        # Explode method creates separate row for each correction
        exploded_frame = transformed_frame.gs_explode(
            colName="corrections", newCol="correction"
        )
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
        dynamic_record["snapshot_date"] = Utils.ms_to_partition_date(dynamic_record["changeTimestamp"])
        return dynamic_record


if __name__ == "__main__":
    # There is no mapping required before processing the certified quiz question record
    certified_quiz_question_snapshots = CertifiedQuizQuestionSnapshots([], PARTITION_KEY)

