from datetime import datetime
from decimal import Decimal
from typing import Union

from clerkai.utils import is_nan


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


def strip_whitespace_if_not_is_nan(x):
    if is_nan(x):
        return None
    return x.strip()
