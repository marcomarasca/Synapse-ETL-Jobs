from datetime import datetime


def get_date_from_absolute_file_name(file_name):
    last_slash_index = file_name.rfind("/")
    second_last_index = file_name.rfind("/", 0, last_slash_index - 1)
    date_string = file_name[second_last_index + 1: last_slash_index]
    return datetime.strptime(date_string, '%Y-%m-%d')


def ms_to_partition_date(timestamp_ms):
    return datetime.utcfromtimestamp(timestamp_ms / 1000.0)


def remove_padded_leading_zeros(input_string):
    if input_string is None:
        return input_string
    return input_string.lstrip("0")
