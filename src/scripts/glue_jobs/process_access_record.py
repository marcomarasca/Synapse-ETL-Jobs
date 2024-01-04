"""
The job process the access record data.
"""

import re
import urllib.parse

from awsglue.transforms import *
from utils import *
from glue_job import GlueJob

WEB_CLIENT = "Synapse-Web-Client"
SYNAPSER_CLIENT = "synapser"
R_CLIENT = "synapseRClient"
PYTHON_CLIENT = "synapseclient"
OLD_JAVA_CLIENT = "Synpase-Java-Client"
JAVA_CLIENT = "Synapse-Java-Client"
COMMAND_LINE_CLIENT = "synapsecommandlineclient"
ELB_CLIENT = "ELB-HealthChecker"
STACK_CLIENT = "SynapseRepositoryStack"
WEB_BROWSER_CLIENT = "(?i)(mozilla|safari|opera|lynx|ucweb|chrome|firefox)"
PARTITION_KEY = "record_date"

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
WEB_BROWSER_CLIENT_PATTERN = WEB_BROWSER_CLIENT + CLIENT_REGEX


class ProcessAccessRecords(GlueJob):

    def __init__(self, mapping_list, partition_key):
        super().__init__(mapping_list, partition_key)

    def execute(self, dynamic_frame):
        transformed_frame = dynamic_frame.map(f=ProcessAccessRecords.transform)
        return transformed_frame.resolveChoice(specs=[("entity_id", "cast:long")])

    # Process the access record
    @staticmethod
    def transform(dynamic_record):
        dynamic_record["normalized_method_signature"] = dynamic_record["method"] + " " + ProcessAccessRecords.get_normalized_method_signature(dynamic_record["request_url"])
        dynamic_record["client"] = ProcessAccessRecords.get_client(dynamic_record["user_agent"])
        dynamic_record["client_version"] = ProcessAccessRecords.get_client_version(dynamic_record["client"],
                                                                                   dynamic_record["user_agent"])
        dynamic_record["entity_id"] = ProcessAccessRecords.get_entity_id(dynamic_record["request_url"])
        # This is the partition date
        dynamic_record["record_date"] = Utils.ms_to_partition_date(dynamic_record["record_date"])
        dynamic_record["instance"] = Utils.remove_padded_leading_zeros(dynamic_record["instance"])
        return dynamic_record

    @staticmethod
    def get_normalized_method_signature(requesturl):
        url = ProcessAccessRecords.decode_url(requesturl.lower())
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
        elif requesturl.startswith("/2fa"):
            result = requesturl
        elif requesturl.startswith("/user/bundle"):
            result = "/user/bundle"
        elif "/access/" in requesturl:
            result = "/objects/#/access/#"
        elif "/schema/type/registered/" in requesturl:
            result = "/schema/type/registered/#"
        else:
            # find and remove substring in url starting from ';' until '/' if present.
            result = re.sub(r';[^/]+', '', requesturl)
            # find and remove any character that is not a word character (letters, digits, or underscores) or slash.
            result = re.sub(r'[^\w\/]', '', result)
            # find and replace substrings with length >=2 in url containing ids with '#'. ID can start with 'syn',
            # 'fh' or digits.
            result = re.sub(r'\b(syn|fh)\d+(\.\d+)?\b|\b\d+(\w+)?[^/]\b', '#', result)
            # The regex provided above doesn't account for substrings with a length of 1.
            # Find and replace substring in url containing only digits.
            result = re.sub(r'/\d+', '/#', result)
        return result

    @staticmethod
    def decode_url(encoded_url):
        if encoded_url is None:
            return None
        decoded_url = urllib.parse.unquote(encoded_url)
        return "".join(decoded_url.split())

    '''
    The order of web and java client matters since some web client call go through Java client, therefore, the 
    USER_AGENT contains both keys for WEB and JAVA client. The order of python and command line client also matters 
    since command line client's USER_AGENT contains python client's key.
    '''

    @staticmethod
    def get_client(user_agent):
        if user_agent is None:
            result = "UNKNOWN"
        elif user_agent.find(WEB_CLIENT) >= 0:
            result = "WEB"
        elif re.search(WEB_BROWSER_CLIENT, user_agent):
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

    ''' 
    In case of WEB_BROWSER_CLIENT_PATTERN re.match find the pattern in the beginning of the string, web browser 
    user agent has mozilla, chrome and safari in same string, so take the first matching pattern.
    Regex has 2 groups first is browser type (mozilla, chrome, safari etc.) and 2nd is version.
    '''

    @staticmethod
    def get_client_version(client, user_agent):
        if user_agent is None:
            return None
        elif client == "WEB":
            if re.search(WEB_BROWSER_CLIENT, user_agent):
                matcher = re.match(WEB_BROWSER_CLIENT_PATTERN, user_agent)
                if matcher is None or matcher.group(2) is None:
                    return None
                else:
                    return matcher.group(2)
            else:
                matcher = re.search(WEB_CLIENT_PATTERN, user_agent)
        elif client == "JAVA":
            if user_agent.startswith("Synpase"):
                matcher = re.search(OLD_JAVA_CLIENT_PATTERN, user_agent)
            else:
                matcher = re.search(JAVA_CLIENT_PATTERN, user_agent)
        elif client == "SYNAPSER":
            matcher = re.search(SYNAPSER_CLIENT_PATTERN, user_agent)
        elif client == "R":
            matcher = re.search(R_CLIENT_PATTERN, user_agent)
        elif client == "PYTHON":
            matcher = re.search(PYTHON_CLIENT_PATTERN, user_agent)
        elif client == "ELB_HEALTHCHECKER":
            matcher = re.search(ELB_CLIENT_PATTERN, user_agent)
        elif client == "COMMAND_LINE":
            matcher = re.search(COMMAND_LINE_CLIENT_PATTERN, user_agent)
        elif client == "STACK":
            matcher = re.search(STACK_CLIENT_PATTERN, user_agent)
        else:
            return None
        if matcher is None:
            return None
        else:
            return matcher.group(1)

    @staticmethod
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


if __name__ == "__main__":
    mapping_list = [
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
        ("payload.authenticationMethod", "string", "auth_method", "string")
    ]
    process_access_records = ProcessAccessRecords(mapping_list, PARTITION_KEY)
