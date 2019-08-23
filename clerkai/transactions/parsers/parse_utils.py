from decimal import Decimal

TWO_PLACES = Decimal(10) ** -2


def amount_to_rounded_decimal(amount):
    return Decimal(amount).quantize(TWO_PLACES)


def convert_european_amount_to_decimal(value):
    value = str(value).replace(",", ".")
    return amount_to_rounded_decimal(value)
