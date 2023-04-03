import unittest

from src.scripts.glue_jobs import node_snapshots

class TestNodeSnaphots(unittest.TestCase):

    def test_strip_syn_prefix(self):
        input = "syn123"
        self.assertEqual("123", node_snapshots.strip_syn_prefix(input))

    def test_strip_syn_prefix_with_no_syn_prefix(self):
        input = "123"
        self.assertEqual("123", node_snapshots.strip_syn_prefix(input))

    def test_strip_syn_prefix_with_empty_string(self):
        input = ""
        self.assertEqual("", node_snapshots.strip_syn_prefix(input))
    
    def test_strip_syn_prefix_with_none_input(self):
        input = None
        self.assertEqual(None, node_snapshots.strip_syn_prefix(input))