from decimal import Decimal


def convert_european_amount_to_decimal(value):
    value = str(value).replace(",", ".")
    return Decimal(value)
