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
