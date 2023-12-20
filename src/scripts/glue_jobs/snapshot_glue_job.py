"""
This abstract class executes identical steps needed for jobs. It initiates a job, retrieves raw JSON data from S3,
maps the data into the required data type, and then writes the processed data back to S3. The 'execute()' method within
this abstract class is a template method that needs to be implemented individually by each job to meet its specific
data processing needs.
"""

from abc import abstractmethod

from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import sys
from awsglue.job import Job
from pyspark.context import SparkContext


class SnapshotGlueJob:

    def __init__(self, mapping_list):
        sc = SparkContext()
        self.glue_context = GlueContext(sc)
        self.logger = self.glue_context.get_logger()
        self.args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "DATABASE_NAME", "TABLE_NAME"])
        self.job = self.create_aws_glue_job()
        dynamic_frame = self.create_input_frame_from_s3()
        mapped_frame = self.apply_mappings(mapping_list, dynamic_frame)
        output_frame = self.execute(mapped_frame)
        self.resolve_choice_and_write_output_frame(output_frame)

    # Create a glue job for specified job name.
    def create_aws_glue_job(self):
        job = Job(self.glue_context)
        job_name = self.args["JOB_NAME"]
        job.init(job_name, self.args)
        self.logger.info("Glue job {} started.".format(job_name))
        return job

    # Read raw JSON data from S3
    def create_input_frame_from_s3(self):
        input_frame = self.glue_context.create_dynamic_frame.from_options(
            format_options={"multiline": True},
            connection_type="s3",
            format="json",
            connection_options={
                "paths": [self.args["S3_SOURCE_PATH"]],
                "recurse": True
            },
            # Note: even though this is optional, job bookmark does not work without it
            transformation_ctx="input_frame"
        )
        self.logger.info("Total input records read from s3 for job {} is {}".format(self.args["JOB_NAME"],
                                                                                    str(input_frame.count())))
        return input_frame

    # Apply mapping to the raw JSON data
    def apply_mappings(self, mapping_list, dynamic_frame):
        if mapping_list is not None and len(mapping_list) > 0:
            mapped_frame = dynamic_frame.apply_mapping(mapping_list)
            if mapped_frame.stageErrorsCount() > 0:
                self.log_errors(mapped_frame)
            return mapped_frame
        return dynamic_frame

    @abstractmethod
    def execute(self):
        pass

    # Resolve the data type mismatch by comparing it with table schema and store the processed data back to s3
    def resolve_choice_and_write_output_frame(self, transformed_frame):
        job_name = self.args["JOB_NAME"]
        if transformed_frame.stageErrorsCount() > 0:
            self.log_errors(transformed_frame)

        output_frame = transformed_frame.resolveChoice(choice='match_catalog', database=self.args['DATABASE_NAME'],
                                                       table_name=self.args['TABLE_NAME'])
        self.logger.info("Total output records write to s3 for job {} is {}".format(job_name,
                                                                                    str(output_frame.count())))
        if output_frame.count() > 0:
            self.glue_context.write_dynamic_frame.from_catalog(
                frame=output_frame,
                database=self.args["DATABASE_NAME"],
                table_name=self.args["TABLE_NAME"],
                additional_options={"partitionKeys": ["snapshot_date"]}
            )
        self.job.commit()
        self.logger.info("Glue job {} finished.".format(self.args["JOB_NAME"]))

    def log_errors(self, dynamic_frame):
        job_name = self.args["JOB_NAME"]
        self.logger.info("Error count is {} in job {}".format(str(dynamic_frame.stageErrorsCount()),
                                                              job_name))
        error_record = dynamic_frame.errorsAsDynamicFrame().toDF().head()
        error_fields = error_record["error"]
        for key in error_fields.asDict().keys():
            self.logger.info("\n Job {} has error {} : {}".format(job_name, key, str(error_fields[key])))
        raise Exception("Error in job! See the log!")
