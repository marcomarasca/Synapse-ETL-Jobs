"""
This script executed by a Glue job. The job take the access record data from S3 and process it.
 Processed data stored in S3 in a parquet file partitioned by the date (%Y-%m-%d pattern) of the timestamp.
"""

import sys
import datetime
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from utils import ms_to_partition_date

WEB_CLIENT = "Synapse-Web-Client"
SYNAPSER_CLIENT = "synapser"
R_CLIENT = "synapseRClient"
PYTHON_CLIENT = "synapseclient"
OLD_JAVA_CLIENT = "Synpase-Java-Client"
JAVA_CLIENT = "Synapse-Java-Client"
COMMAND_LINE_CLIENT = "synapsecommandlineclient"
ELB_CLIENT = "ELB-HealthChecker"
STACK_CLIENT = "SynapseRepositoryStack"

CLIENT_REGEX = "/(\\S+)"
SYNAPSER_CLIENT_PATTERN = SYNAPSER_CLIENT + CLIENT_REGEX
R_CLIENT_PATTERN = R_CLIENT + CLIENT_REGEX
PYTHON_CLIENT_PATTERN = PYTHON_CLIENT + CLIENT_REGEX
WEB_CLIENT_PATTERN = WEB_CLIENT + CLIENT_REGEX
OLD_JAVA_CLIENT_PATTERN = OLD_JAVA_CLIENT + CLIENT_REGEX
JAVA_CLIENT_PATTERN = JAVA_CLIENT + CLIENT_REGEX
COMMAND_LINE_CLIENT_PATTERN = COMMAND_LINE_CLIENT + CLIENT_REGEX
ELB_CLIENT_PATTERN = ELB_CLIENT + CLIENT_REGEX
STACK_CLIENT_PATTERN = STACK_CLIENT + CLIENT_REGEX


# Get access record from source and create dynamic frame for futher processing
def get_dynamic_frame(connection_type, file_format, source_path, glue_context):
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        format_options={"multiline": True},
        connection_type=connection_type,
        format=file_format,
        connection_options={
            "paths": [source_path],
            "recurse": True,
        },
        transformation_ctx="dynamic_frame")
    return dynamic_frame


def apply_mapping(dynamic_frame):
    mapped_dynamic_frame = ApplyMapping.apply(
        frame=dynamic_frame,
        mappings=[
            ("payload.sessionId", "string", "session_id", "string"),
            ("payload.timestamp", "bigint", "timestamp", "timestamp"),
            # we need to map the same timestamp into a bigint so that we can extract the partition date
            ("payload.timestamp", "bigint", "record_date", "bigint"),
            ("payload.userId", "bigint", "user_id", "bigint"),
            ("payload.method", "string", "method", "string"),
            ("payload.requestURL", "string", "request_url", "string"),
            ("payload.userAgent", "string", "user_agent", "string"),
            ("payload.host", "string", "host", "string"),
            ("payload.origin", "string", "origin", "string"),
            ("payload.xforwardedFor", "string", "x_forwarded_for", "string"),
            ("payload.via", "string", "via", "string"),
            ("payload.threadId", "bigint", "thread_id", "bigint"),
            ("payload.elapseMS", "bigint", "elapse_ms", "bigint"),
            ("payload.success", "boolean", "success", "boolean"),
            ("payload.stack", "string", "stack", "string"),
            ("payload.instance", "string", "instance", "string"),
            ("payload.vmId", "string", "vm_id", "string"),
            ("payload.returnObjectId", "string", "return_object_id", "string"),
            ("payload.queryString", "string", "query_string", "string"),
            ("payload.responseStatus", "bigint", "response_status", "bigint"),
            ("payload.oauthClientId", "string", "oauth_client_id", "string"),
            ("payload.basicAuthUsername", "string", "basic_auth_username", "string"),
            ("payload.authenticationMethod", "string", "auth_method", "string"),
        ],
        transformation_ctx="mapped_dynamic_frame")
    return mapped_dynamic_frame


# process the access record
def transform(dynamic_record):
    dynamic_record["normalized_method_signature"] = dynamic_record["method"] + " " + get_normalized_method_signature(
        dynamic_record["request_url"])
    dynamic_record["client"] = get_client(dynamic_record["user_agent"])
    dynamic_record["client_version"] = get_client_version(dynamic_record["client"], dynamic_record["user_agent"])
    dynamic_record["entity_id"] = get_entity_id(dynamic_record["request_url"])
    # This is the partition date
    dynamic_record["record_date"] = ms_to_partition_date(dynamic_record["record_date"])
    return dynamic_record


def get_normalized_method_signature(requesturl):
    url = requesturl.lower()
    prefix_index = url.find('/v1/')
    if prefix_index == -1:
        return "INVALID URL"
    else:
        start = prefix_index + 3
        requesturl = url[start:]
    if requesturl.startswith("/entity/md5"):
        result = "/entity/md5/#"
    elif requesturl.startswith("/evaluation/name"):
        result = "/evaluation/name/#"
    elif requesturl.startswith("/entity/alias"):
        result = "/entity/alias/#"
    else:
        result = re.sub("/(syn\\d+|\\d+)", "/#", requesturl)
    return result


def get_client(user_agent):
    if user_agent is None:
        result = "UNKNOWN"
    elif user_agent.find(WEB_CLIENT) >= 0:
        result = "WEB"
    elif user_agent.find(JAVA_CLIENT) >= 0:
        result = "JAVA"
    elif user_agent.find(OLD_JAVA_CLIENT) >= 0:
        result = "JAVA"
    elif user_agent.find(SYNAPSER_CLIENT) >= 0:
        result = "SYNAPSER"
    elif user_agent.find(R_CLIENT) >= 0:
        result = "R"
    elif user_agent.find(COMMAND_LINE_CLIENT) >= 0:
        result = "COMMAND_LINE"
    elif user_agent.find(PYTHON_CLIENT) >= 0:
        result = "PYTHON"
    elif user_agent.find(ELB_CLIENT) >= 0:
        result = "ELB_HEALTHCHECKER"
    elif user_agent.find(STACK_CLIENT) >= 0:
        result = "STACK"
    else:
        result = "UNKNOWN"
    return result


def get_client_version(client, user_agent):
    if user_agent is None:
        return None
    elif client == "WEB":
        matcher = re.match(WEB_CLIENT_PATTERN, user_agent)
    elif client == "JAVA":
        if user_agent.startswith("Synpase"):
            matcher = re.match(OLD_JAVA_CLIENT_PATTERN, user_agent)
        else:
            matcher = re.match(JAVA_CLIENT_PATTERN, user_agent)
    elif client == "SYNAPSER":
        matcher = re.match(SYNAPSER_CLIENT_PATTERN, user_agent)
    elif client == "R":
        matcher = re.match(R_CLIENT_PATTERN, user_agent)
    elif client == "PYTHON":
        matcher = re.match(PYTHON_CLIENT_PATTERN, user_agent)
    elif client == "ELB_HEALTHCHECKER":
        matcher = re.match(ELB_CLIENT_PATTERN, user_agent)
    elif client == "COMMAND_LINE":
        matcher = re.match(COMMAND_LINE_CLIENT_PATTERN, user_agent)
    elif client == "STACK":
        matcher = re.match(STACK_CLIENT_PATTERN, user_agent)
    else:
        return None
    if matcher is None:
        return None
    else:
        return matcher.group(1)


def get_entity_id(requesturl):
    if requesturl is None:
        return None
    else:
        requesturl = requesturl.lower()
        matcher = re.search("/entity/(syn\\d+|\\d+)", requesturl)
        if matcher is None:
            return None
        else:
            entity_id = matcher.group(1)
            if entity_id.startswith("syn"):
                entity_id = entity_id[3:]
    return entity_id


def main():
    # Get args and setup environment
    args = getResolvedOptions(sys.argv,
                              ["JOB_NAME", "S3_SOURCE_PATH", "DATABASE_NAME", "TABLE_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    dynamic_frame = get_dynamic_frame("s3", "json", args["S3_SOURCE_PATH"], glue_context)
    mapped_frame = apply_mapping(dynamic_frame)
    transformed_frame = mapped_frame.map(f=transform)
    # Use the catalog table to resolve any ambiguity
    type_casted_frame = transformed_frame.resolveChoice(choice='match_catalog', database=args['DATABASE_NAME'], table_name=args['TABLE_NAME'])

    #  Write the processed access records to destination
    glue_context.write_dynamic_frame.from_catalog(
        frame=type_casted_frame,
        database=args["DATABASE_NAME"],
        table_name=args["TABLE_NAME"],
        additional_options={"partitionKeys": ["record_date"]},
        transformation_ctx="write_dynamic_frame")

    job.commit()


if __name__ == "__main__":
    main()
