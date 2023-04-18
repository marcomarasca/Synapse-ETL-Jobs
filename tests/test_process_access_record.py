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
        expected_output = "12223809"
        real_output = process_access_record.get_entity_id("/repo/v1/entity/syn12223809")
        self.assertEqual(expected_output, real_output)

    def test_get_entity_id_for_id_without_syn(self):
        expected_output = "1234"
        real_output = process_access_record.get_entity_id("/repo/v1/entity/1234")
        self.assertEqual(expected_output, real_output)

    def test_get_entity_id_without_id(self):
        real_output = process_access_record.get_entity_id("/repo/v1/version")
        self.assertIsNone(real_output)

    def test_get_entity_id_with_none(self):
        real_output = process_access_record.get_entity_id(None)
        self.assertIsNone(real_output)

    def test_get_entity_id_for_url_having_two_syn(self):
        expected_output = "1234"
        real_output = process_access_record.get_entity_id("/repo/v1/entity/syn1234/check/syn123456")
        self.assertEqual(expected_output, real_output)

    def test_get_entity_id_for_case_insensitive(self):
        expected_output = "1234"
        real_output = process_access_record.get_entity_id("/repo/v1/entity/Syn1234/check")
        self.assertEqual(expected_output, real_output)

    def test_get_entity_id_for_with_version(self):
        expected_output = "12345"
        real_output = process_access_record.get_entity_id("/repo/v1/entity/SYN12345.1/check")
        self.assertEqual(expected_output, real_output)
