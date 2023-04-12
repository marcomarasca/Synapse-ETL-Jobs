import unittest

from src.scripts.glue_jobs import utils

class TestUtils(unittest.TestCase):

    def test_strip_syn_prefix(self):
        input = "syn123"
        self.assertEqual("123", utils.strip_syn_prefix(input))

    def test_strip_syn_prefix_with_no_syn_prefix(self):
        input = "123"
        self.assertEqual("123", utils.strip_syn_prefix(input))

    def test_strip_syn_prefix_with_empty_string(self):
        input = ""
        self.assertEqual("", utils.strip_syn_prefix(input))
    
    def test_strip_syn_prefix_with_none_input(self):
        input = None
        self.assertEqual(None, utils.strip_syn_prefix(input))

    def test_ms_to_partition_date(self):
        input = 1680758367496
        self.assertEqual('2023-04-06', utils.ms_to_partition_date(input))
