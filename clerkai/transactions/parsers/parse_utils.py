import math
from datetime import datetime
from decimal import Decimal
from typing import Union

TWO_PLACES = Decimal(10) ** -2


def amount_to_rounded_decimal(amount):
    # type: (Union[float, str]) -> Decimal
    return Decimal(amount).quantize(TWO_PLACES)


def convert_european_amount_to_decimal(value):
    # type: (Union[float, str]) -> Decimal
    value = str(value).replace(",", ".")
    return amount_to_rounded_decimal(value)


def is_nan(x):
    try:
        return math.isnan(x)
    except TypeError:
        return False


def ymd_date_to_datetime_obj(datetime_str):
    if is_nan(datetime_str):
        return None
    datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d")
    return datetime_obj
