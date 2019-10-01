import math
from datetime import datetime


def is_nan(x):
    try:
        return math.isnan(x)
    except TypeError:
        return False


def timestamp_ms_to_datetime_obj(timestamp_ms):
    if is_nan(timestamp_ms):
        return None
    if type(timestamp_ms) is str:
        timestamp_ms = float(timestamp_ms)
    datetime_obj = datetime.fromtimestamp(timestamp_ms / 1000)
    return datetime_obj
