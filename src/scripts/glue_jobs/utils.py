from datetime import datetime


class Utils(object):
    @staticmethod
    def syn_id_string_to_int(input_string):
        if input_string is None:
            return input_string

        input_string = input_string.strip()

        if input_string.startswith('syn'):
            input_string = input_string[len('syn'):]

        if not input_string:
            return None

        return int(input_string)

    @staticmethod
    def ms_to_partition_date(timestamp_ms):
        return datetime.utcfromtimestamp(timestamp_ms / 1000.0).strftime("%Y-%m-%d")

    @staticmethod
    def remove_padded_leading_zeros(input_string):
        if input_string is None:
            return input_string
        return input_string.lstrip("0")

    @staticmethod
    def format_message(message, *args):
        return message.format(*args)
