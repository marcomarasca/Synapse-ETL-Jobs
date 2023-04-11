from datetime import datetime

def strip_syn_prefix(input_string):
    if input_string is None:
        return input_string
    
    if input_string.startswith('syn'):
        return input_string[len('syn'):]
        
    return input_string

def ms_to_athena_timestamp(timestamp_ms):
    if (timestamp_ms is None):
        return timestamp_ms
    
    # yyyy-MM-dd HH:mm:ss.fff
    return datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat(sep=' ', timespec='milliseconds')

def ms_to_partition_date(timestamp_ms):
    return datetime.utcfromtimestamp(timestamp_ms / 1000.0).strftime("%Y-%m-%d")