"""
This script executed by a Glue job for back-filling the access record. The job take the access record data from S3
which is in csv format and process it. Processed data stored in S3 in a json format and partitioned by timestamp of
record as  year / month / day.
"""

import sys
import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


# Add new fields stack, instance, timestamp and partition
def transform(dynamic_record):
    dynamic_record["stack"] = dynamic_record["payload"]["stack"]
    dynamic_record["instance"] = dynamic_record["payload"]["instance"]
    timestamp = dynamic_record["payload"]["timestamp"]
    dynamic_record["timestamp"] = timestamp
    date = datetime.datetime.utcfromtimestamp(timestamp / 1000.0)
    dynamic_record["year"] = date.year
    dynamic_record["month"] = '%02d' % date.month
    dynamic_record["day"] = '%02d' % date.day
    return dynamic_record


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_DATABASE_NAME", "SOURCE_TABLE_NAME",
                                         "DESTINATION_DATABASE_NAME", "DESTINATION_TABLE_NAME", "RELEASE_NUMBER",
                                         "RECORD_DATE"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    predicate = "(release_number == " + args["RELEASE_NUMBER"] + " )"

    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
        database=args["SOURCE_DATABASE_NAME"], table_name=args["SOURCE_TABLE_NAME"],
        transformation_ctx="dynamic_frame",
        push_down_predicate=predicate
    )

    # Maps the incoming record
    mapped_dynamic_frame = ApplyMapping.apply(
        frame=dynamic_frame,
        mappings=[
            ("session_id", "string", "payload.sessionId", "string"),
            ("timestamp", "string", "payload.timestamp", "bigint"),
            ("user_id", "string", "payload.userId", "bigint"),
            ("method", "string", "payload.method", "string"),
            ("request_url", "string", "payload.requestURL", "string"),
            ("user_agent", "string", "payload.userAgent", "string"),
            ("host", "string", "payload.host", "string"),
            ("origin", "string", "payload.origin", "string"),
            ("via", "string", "payload.via", "string"),
            ("thread_id", "string", "payload.threadId", "bigint"),
            ("elapse_ms", "string", "payload.elapseMS", "bigint"),
            ("success", "string", "payload.success", "boolean"),
            ("stack", "string", "payload.stack", "string"),
            ("instance", "string", "payload.instance", "string"),
            ("date", "string", "payload.date", "string"),
            ("vm_id", "string", "payload.vmId", "string"),
            ("return_object_id", "string", "payload.returnObjectId", "string"),
            ("query_string", "string", "payload.queryString", "string"),
            ("response_status", "string", "payload.responseStatus", "bigint"),
            ("oauth_client_id", "string", "payload.oauthClientId", "string"),
            ("basic_auth_username", "string", "payload.basicAuthUsername", "string"),
            ("auth_method", "string", "payload.authenticationMethod", "string"),
            ("x_forwarded_for", "string", "payload.xforwardedFor", "string")
        ],
        transformation_ctx="mapped_dynamic_frame",
    )
    # transform the records
    transformed_dynamic_frame = mapped_dynamic_frame.map(f=transform)
    # repartition the dynamic frame as single partition
    repartitioned_frame = transformed_dynamic_frame.repartition(1)
    # write records to destination if available
    if (repartitioned_frame.count() > 0):
        write_dynamic_frame = glue_context.write_dynamic_frame.from_catalog(
            frame=repartitioned_frame,
            database=args["DESTINATION_DATABASE_NAME"],
            table_name=args["DESTINATION_TABLE_NAME"],
            additional_options={"compression": "gzip", "partitionKeys": ["year", "month", "day"]},
            transformation_ctx="write_dynamic_frame"
        )

    job.commit()


if __name__ == "__main__":
    main()
