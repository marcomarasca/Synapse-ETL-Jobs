import unittest

from src.scripts.glue_jobs.utils import Utils

class TestUtils(unittest.TestCase):

    def test_syn_id_string_to_int(self):
        input = "syn123"
        self.assertEqual(123, Utils.syn_id_string_to_int(input))

    def test_syn_id_string_to_int_with_no_syn_prefix(self):
        input = "123"
        self.assertEqual(123, Utils.syn_id_string_to_int(input))
    
    def test_syn_id_string_to_int_with_long_value(self):
        input = "9223372036854775807"
        self.assertEqual(9223372036854775807, Utils.syn_id_string_to_int(input))

    def test_syn_id_string_to_int_with_empty_string(self):
        input = ""
        self.assertEqual(None, Utils.syn_id_string_to_int(input))

    def test_syn_id_string_to_int_with_blank_string(self):
        input = "  "
        self.assertEqual(None, Utils.syn_id_string_to_int(input))
    
    def test_syn_id_string_to_int_with_none_input(self):
        input = None
        self.assertEqual(None, Utils.syn_id_string_to_int(input))

    def test_ms_to_partition_date(self):
        input = 1680758367496
        self.assertEqual('2023-04-06', Utils.ms_to_partition_date(input))

    def test_remove_padded_leading_zeros_with_no_trailing_Zeros(self):
        input_value = "000000236"
        self.assertEqual('236', Utils.remove_padded_leading_zeros(input_value))

    def test_remove_padded_leading_zeros_with_trailing_Zero(self):
        input_value = "0000002360"
        self.assertEqual('2360', Utils.remove_padded_leading_zeros(input_value))

    def test_remove_padded_leading_zeros_with_none_input(self):
        input_value = None
        self.assertEqual(None, Utils.remove_padded_leading_zeros(input_value))
