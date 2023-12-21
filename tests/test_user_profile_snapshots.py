import unittest
import sys
from pathlib import Path
p = Path(__file__).parents[1]
sys.path.append(str(p) + "/src/scripts/glue_jobs")

from user_profile_snapshots import UserProfileSnapshots

class TestUserProfileSnapshots(unittest.TestCase):

    def test_get_email_for_none(self):
        expected_output = None
        real_output = UserProfileSnapshots.get_email(None)
        self.assertEqual(expected_output, real_output)

    def test_get_email_for_emptyList(self):
        expected_output = None
        emails = []
        real_output = UserProfileSnapshots.get_email(emails)
        self.assertEqual(expected_output, real_output)

    def test_get_email_with_singleValue(self):
        expected_output = "abc@gmail.com"
        emails = ["abc@gmail.com"]
        real_output = UserProfileSnapshots.get_email(emails)
        self.assertEqual(expected_output, real_output)

    def test_get_email_with_multiValues(self):
        expected_output = "abc@gmail.com"
        emails = ["abc@gmail.com", "def@yahoo.com"]
        real_output = UserProfileSnapshots.get_email(emails)
        self.assertEqual(expected_output, real_output)
