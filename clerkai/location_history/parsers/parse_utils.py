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


def exiftool_date_to_datetime_obj(exiftool_date):
    if is_nan(exiftool_date):
        return None
    datetime_obj = datetime.strptime(exiftool_date, "%Y:%m:%d %H:%M:%S")
    return datetime_obj
