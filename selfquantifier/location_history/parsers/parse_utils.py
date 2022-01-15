from datetime import datetime

import pytz

from clerkai.utils import is_nan


def timestamp_ms_to_utc_datetime_obj(timestamp_ms):
    if is_nan(timestamp_ms):
        return None
    if type(timestamp_ms) is str:
        timestamp_ms = float(timestamp_ms)
    datetime_obj = datetime.fromtimestamp(timestamp_ms / 1000)
    return datetime_obj.astimezone(pytz.utc)


def exiftool_date_to_utc_datetime_obj(exiftool_date):
    if is_nan(exiftool_date):
        return None
    if exiftool_date == "0000:00:00 00:00:00":
        return None
    try:
        naive_parsed_datetime = datetime.strptime(exiftool_date, "%Y:%m:%d %H:%M:%S")
        parsed_datetime = pytz.utc.localize(naive_parsed_datetime)
    except ValueError:
        parsed_datetime = datetime.strptime(
            exiftool_date, "%Y:%m:%d %H:%M:%S%z"
        ).astimezone(pytz.utc)
    return parsed_datetime
