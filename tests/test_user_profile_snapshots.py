import unittest

from src.scripts.glue_jobs import user_profile_snapshots


class TestUserProfileSnapshots(unittest.TestCase):

    def test_get_email_for_none(self):
        expected_output = None
        real_output = user_profile_snapshots.get_email(None)
        self.assertEqual(expected_output, real_output)

    def test_get_email_for_emptyList(self):
        expected_output = None
        emails = []
        real_output = user_profile_snapshots.get_email(emails)
        self.assertEqual(expected_output, real_output)

    def test_get_email_with_singleValue(self):
        expected_output = "abc@gmail.com"
        emails = ["abc@gmail.com"]
        real_output = user_profile_snapshots.get_email(emails)
        self.assertEqual(expected_output, real_output)

    def test_get_email_with_multiValues(self):
        expected_output = "abc@gmail.com"
        emails = ["abc@gmail.com", "def@yahoo.com"]
        real_output = user_profile_snapshots.get_email(emails)
        self.assertEqual(expected_output, real_output)
