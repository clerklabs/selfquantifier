import pandas as pd
from pandas.core.frame import DataFrame

from selfquantifier.transactions.parsers.parse_utils import amount_to_rounded_decimal
from selfquantifier.utils import is_nan


def revolut_date_to_naive_datetime_obj(datetime_str):
    from datetime import datetime

    if is_nan(datetime_str):
        return None
    datetime_obj = datetime.strptime(datetime_str, "%d. %b %Y")
    return datetime_obj


def convert_revolut_amount_to_decimal(value, **kwargs):
    # type: (Union[float, str], dict) -> Decimal
    if value is None:
        return None
    if is_nan(value):
        return None
    value = str(value).replace(" ", "").replace(",", ".")
    return amount_to_rounded_decimal(value, **kwargs)


def split_revolut_exchange_info(value):
    # type: (str) -> Decimal
    if is_nan(value):
        return (None, None)
    split_info = value.strip().split(sep=" ", maxsplit=2)
    return (split_info[0], split_info[1])


def parse_revolut_exchange_rate(value):
    # type: (str) -> Decimal
    if value == " ":
        return None
    if is_nan(value):
        return None
    rate_side = value.split("=")[1].strip()
    return rate_side.split(" ")[0].strip()


def import_revolut_csv_transaction_file(transaction_file):
    # type: (str) -> DataFrame
    return pd.read_csv(transaction_file, delimiter=";")


def revolut_legacy_currency_from_columns(df_columns):
    import re

    for df_column in df_columns:
        m = re.search(r"Paid Out \(([A-Z^\)]+)\)", df_column)
        if m and m.group(1):
            return m.group(1)


def revolut_legacy_csv_transactions_to_general_clerk_format(df):
    # type: (DataFrame) -> DataFrame
    normalized_df = pd.DataFrame()

    currency = revolut_legacy_currency_from_columns(df.columns)

    import numpy as np

    df["amount_info"] = np.where(
        ~df[f"Paid In ({currency})"].isnull(),
        df[f"Paid In ({currency})"],
        df[f"Paid Out ({currency})"],
    )
    df["signed_amount_info"] = np.where(
        ~df[f"Paid Out ({currency})"].isnull(),
        "-" + df["amount_info"],
        df["amount_info"],
    )
    df["exchange_info"] = np.where(
        ~df["Exchange In"].isnull(), df["Exchange In"], df["Exchange Out"]
    )

    normalized_df["Raw Real Date"] = None
    normalized_df["Raw Bank Date"] = df["Completed Date"]
    normalized_df["Raw Payee"] = None
    normalized_df["Raw Bank Message"] = df["Reference"]
    normalized_df["Raw Amount"] = df["signed_amount_info"]
    normalized_df["Raw Currency"] = currency
    normalized_df["Raw Balance"] = df[f" Balance ({currency})"]
    (
        normalized_df["Raw Foreign Currency"],
        normalized_df["Raw Foreign Currency Amount"],
    ) = zip(*df["exchange_info"].apply(split_revolut_exchange_info))
    normalized_df["Raw Foreign Currency Rate"] = df["Exchange Rate"].apply(
        parse_revolut_exchange_rate
    )

    normalized_df["Real Date"] = normalized_df["Raw Real Date"]
    normalized_df["Bank Date"] = normalized_df["Raw Bank Date"].apply(
        revolut_date_to_naive_datetime_obj
    )
    normalized_df["Payee"] = normalized_df["Raw Payee"]
    normalized_df["Bank Message"] = normalized_df["Raw Bank Message"]
    normalized_df["Amount"] = normalized_df["Raw Amount"].apply(
        convert_revolut_amount_to_decimal
    )
    normalized_df["Currency"] = normalized_df["Raw Currency"]
    normalized_df["Balance"] = normalized_df["Raw Balance"].apply(
        convert_revolut_amount_to_decimal
    )
    normalized_df["Foreign Currency"] = normalized_df["Raw Foreign Currency"]
    normalized_df["Foreign Currency Amount"] = normalized_df[
        "Raw Foreign Currency Amount"
    ].apply(convert_revolut_amount_to_decimal)
    normalized_df["Foreign Currency Rate"] = normalized_df[
        "Raw Foreign Currency Rate"
    ].apply(convert_revolut_amount_to_decimal, accuracy=14)
    normalized_df["Original data"] = df[
        [
            "Completed Date",
            "Reference",
            f"Paid Out ({currency})",
            f"Paid In ({currency})",
            "Exchange Out",
            "Exchange In",
            f" Balance ({currency})",
            "Exchange Rate",
            "Category",
        ]
    ].to_dict(orient="records")
    return normalized_df[
        [
            "Real Date",
            "Bank Date",
            "Payee",
            "Bank Message",
            "Amount",
            "Currency",
            "Balance",
            "Foreign Currency",
            "Foreign Currency Amount",
            "Foreign Currency Rate",
            "Original data",
            "Raw Real Date",
            "Raw Bank Date",
            "Raw Payee",
            "Raw Bank Message",
            "Raw Amount",
            "Raw Currency",
            "Raw Balance",
            "Raw Foreign Currency",
            "Raw Foreign Currency Amount",
            "Raw Foreign Currency Rate",
        ]
    ]


def revolut_legacy_csv_transactions_parser(transaction_file):
    # type: (str) -> DataFrame
    df = import_revolut_csv_transaction_file(transaction_file)
    return revolut_legacy_csv_transactions_to_general_clerk_format(df)
