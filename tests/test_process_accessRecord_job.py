import unittest

from src.scripts.glue_jobs import process_accessRecord_job


class UnitTestProcessMethod(unittest.TestCase):

    def test_NormalizedMethodSignature(self):
        expected_output = "/entity/md5/#"
        real_output = process_accessRecord_job.getNormalizedMethodSignature("repo/v1/entity/md5")
        self.assertEqual(expected_output, real_output)

        expected_output = "/evaluation/name/#"
        real_output = process_accessRecord_job.getNormalizedMethodSignature("repo/v1/evaluation/name")
        self.assertEqual(expected_output, real_output)

        expected_output = "/entity/alias/#"
        real_output = process_accessRecord_job.getNormalizedMethodSignature("repo/v1/entity/alias")
        self.assertEqual(expected_output, real_output)

        expected_output = "/entity/#"
        real_output = process_accessRecord_job.getNormalizedMethodSignature("repo/v1/entity/syn123456")
        self.assertEqual(expected_output, real_output)

        with self.assertRaises(ValueError) as context:
            process_accessRecord_job.getNormalizedMethodSignature("repo/entity/syn123456")
            self.assertTrue("It must start with {optional WAR name}/{any string}/v1/" in str(context.exception))

    def test_getClient(self):
        expected_output = "WEB"
        real_output = process_accessRecord_job.getClient("Synapse-Web-Client/435.0")
        self.assertEqual(expected_output, real_output)

        expected_output = "JAVA"
        real_output = process_accessRecord_job.getClient("Synapse-Java-Client/431.0")
        self.assertEqual(expected_output, real_output)

        expected_output = "JAVA"
        real_output = process_accessRecord_job.getClient("Synpase-Java-Client/434.0")
        self.assertEqual(expected_output, real_output)

        expected_output = "SYNAPSER"
        real_output = process_accessRecord_job.getClient("synapser/0.15.33synapseclient/2.7.0 python-requests/2.28.2")
        self.assertEqual(expected_output, real_output)

        expected_output = "R"
        real_output = process_accessRecord_job.getClient("synapseRClient/test")
        self.assertEqual(expected_output, real_output)

        expected_output = "COMMAND_LINE"
        real_output = process_accessRecord_job.getClient("synapsecommandlineclient/test")
        self.assertEqual(expected_output, real_output)

        expected_output = "PYTHON"
        real_output = process_accessRecord_job.getClient("python/synapseclient/test")
        self.assertEqual(expected_output, real_output)

        expected_output = "ELB_HEALTHCHECKER"
        real_output = process_accessRecord_job.getClient("ELB-HealthChecker/2.0")
        self.assertEqual(expected_output, real_output)

        expected_output = "STACK"
        real_output = process_accessRecord_job.getClient("test/SynapseRepositoryStack/432")
        self.assertEqual(expected_output, real_output)

        expected_output = "UNKNOWN"
        real_output = process_accessRecord_job.getClient("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0)")
        self.assertEqual(expected_output, real_output)

        expected_output = "UNKNOWN"
        real_output = process_accessRecord_job.getClient(None)
        self.assertEqual(expected_output, real_output)

    def test_getClientVersion(self):
        expected_output = "435.0"
        real_output = process_accessRecord_job.getClientVersion("WEB", "Synapse-Web-Client/435.0")
        self.assertEqual(expected_output, real_output)

        expected_output = "431.0"
        real_output = process_accessRecord_job.getClientVersion("JAVA", "Synapse-Java-Client/431.0")
        self.assertEqual(expected_output, real_output)

        expected_output = "434.0"
        real_output = process_accessRecord_job.getClientVersion("JAVA", "Synpase-Java-Client/434.0")
        self.assertEqual(expected_output, real_output)

        expected_output = "0.15.33"
        real_output = process_accessRecord_job.getClientVersion("SYNAPSER", "synapser/0.15.33")
        self.assertEqual(expected_output, real_output)

        expected_output = "2"
        real_output = process_accessRecord_job.getClientVersion("R", "synapseRClient/2")
        self.assertEqual(expected_output, real_output)

        expected_output = "123"
        real_output = process_accessRecord_job.getClientVersion("COMMAND_LINE", "synapsecommandlineclient/123")
        self.assertEqual(expected_output, real_output)

        expected_output = "4.0"
        real_output = process_accessRecord_job.getClientVersion("PYTHON", "synapseclient/4.0")
        self.assertEqual(expected_output, real_output)

        expected_output = "2.0"
        real_output = process_accessRecord_job.getClientVersion("ELB_HEALTHCHECKER", "ELB-HealthChecker/2.0")
        self.assertEqual(expected_output, real_output)

        expected_output = "432"
        real_output = process_accessRecord_job.getClientVersion("STACK", "SynapseRepositoryStack/432")
        self.assertEqual(expected_output, real_output)

        real_output = process_accessRecord_job.getClientVersion("UNKNOWN",
                                                                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0)")
        self.assertIsNone(real_output)

        real_output = process_accessRecord_job.getClientVersion(None, "testClient")
        self.assertIsNone(real_output)

        real_output = process_accessRecord_job.getClientVersion("STACK", None)
        self.assertIsNone(real_output)

    def test_getEntityId(self):
        expected_output = "12223809"
        real_output = process_accessRecord_job.getEntityId("/repo/v1/entity/syn12223809")
        self.assertEqual(expected_output, real_output)

        expected_output = "1234"
        real_output = process_accessRecord_job.getEntityId("/repo/v1/entity/1234")
        self.assertEqual(expected_output, real_output)

        real_output = process_accessRecord_job.getEntityId("/repo/v1/version")
        self.assertIsNone(real_output)

        real_output = process_accessRecord_job.getEntityId(None)
        self.assertIsNone(real_output)
