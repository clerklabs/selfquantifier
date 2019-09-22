import math
from datetime import datetime
from decimal import Decimal
from typing import Union


def amount_to_rounded_decimal(amount, **kwargs):
    if "accuracy" in kwargs:
        accuracy = kwargs["accuracy"]
    else:
        accuracy = 2
    DECIMAL_PLACES = Decimal(10) ** -accuracy
    if is_nan(amount):
        return None
    return Decimal(amount).quantize(DECIMAL_PLACES)


# 195.689,01 -> Decimal(195689.01)
def convert_european_amount_to_decimal(value):
    # type: (Union[float, str]) -> Decimal
    value = str(value).replace(".", "").replace(",", ".")
    return amount_to_rounded_decimal(value)


def is_nan(x):
    try:
        return math.isnan(x)
    except TypeError:
        return False


def strip_whitespace_if_not_is_nan(x):
    if is_nan(x):
        return None
    return x.strip()


def ymd_date_to_datetime_obj(datetime_str):
    if is_nan(datetime_str):
        return None
    datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d")
    return datetime_obj
