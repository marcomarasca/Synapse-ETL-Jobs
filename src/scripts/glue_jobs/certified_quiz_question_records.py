"""
The job take the certified user passing records from S3 and process it.
Processed data stored in S3 in a parquet file partitioned by the date (%Y-%m-%d pattern) of the snapshot timestamp.
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from utils import ms_to_partition_date
import gs_explode


# process the record
def transform(dynamic_record):
    # Correction array contains questionIndex and isCorrect, which is need for quiz question record
    corrections = dynamic_record["payload"]["corrections"]
    correctionInfo = []
    for correction in corrections:
        info = {
            "questionIndex": correction["question"]["questionIndex"],
            "isCorrect": correction["isCorrect"]
        }
        correctionInfo.append(info)

    dynamic_record["corrections"] = correctionInfo
    # This is the partition date
    dynamic_record["record_date"] = ms_to_partition_date(dynamic_record["timestamp"])
    return dynamic_record


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "DATABASE_NAME", "TABLE_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Read from s3
    input_frame = glue_context.create_dynamic_frame.from_options(
        format_options={"multiline": True},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [args["S3_SOURCE_PATH"]],
            "recurse": True,
        },
        # Note: even though this is optional, job bookmark does not work without it
        transformation_ctx="input_frame",
    )
    # Apply transformations to compute the partition date and array of questionIndex and isCorrect values
    transformed_frame = input_frame.map(f=transform)

    # Drop the unnecessary fields and map new array generated in transformed method
    mapped_frame = transformed_frame.apply_mapping(
        [
            ("stack", "string", "stack", "string"),
            ("record_date", "date", "record_date", "date"),
            ("instance", "string", "instance", "string"),
            ("payload.responseId", "bigint", "response_id", "bigint"),
            ("corrections", "array", "corrections", "array"),
        ]
    )

    # Explode method creates separate row for each correction
    exploded_frame = mapped_frame.gs_explode(
        colName="corrections", newCol="correction"
    )

    # Map each rows into required table record
    output_frame = exploded_frame.apply_mapping(
        [
            ("stack", "string", "stack", "string"),
            ("instance", "string", "instance", "string"),
            ("response_id", "int", "response_id", "bigint"),
            ("correction.questionIndex", "int", "question_index", "bigint"),
            ("correction.isCorrect", "boolean", "is_correct", "boolean"),
            ("record_date", "string", "record_date", "date"),
        ]
    )

    # Use the catalog table to resolve any ambiguity
    resolved_frame = output_frame.resolveChoice(choice='match_catalog', database=args['DATABASE_NAME'],
                                                table_name=args['TABLE_NAME'])

    # Write only if there is new data (this will error out otherwise)
    if (resolved_frame.count() > 0):
        glue_context.write_dynamic_frame.from_catalog(
            frame=resolved_frame,
            database=args["DATABASE_NAME"],
            table_name=args["TABLE_NAME"],
            additional_options={"partitionKeys": ["record_date"]}
        )

    job.commit()


if __name__ == "__main__":
    main()
