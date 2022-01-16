import pandas as pd
from pandas.core.frame import DataFrame

from selfquantifier.transactions.parsers.parse_utils import amount_to_rounded_decimal
from selfquantifier.utils import is_nan


def revolut_date_to_naive_datetime_obj(datetime_str):
    from datetime import datetime

    if is_nan(datetime_str):
        return None
    datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    return datetime_obj


def import_revolut_csv_transaction_file(transaction_file):
    # type: (str) -> DataFrame
    return pd.read_csv(transaction_file, delimiter=",")


def revolut_csv_transactions_to_general_clerk_format(df):
    # type: (DataFrame) -> DataFrame

    # ignore pending rows
    completed_transactions_mask = ~df["State"].isnull() & (df["State"] == "COMPLETED")
    df = df[completed_transactions_mask]

    normalized_df = pd.DataFrame()

    normalized_df["Raw Real Date"] = df["Started Date"]
    normalized_df["Raw Bank Date"] = df["Completed Date"]
    normalized_df["Raw Payee"] = None
    normalized_df["Raw Bank Message"] = df["Description"]
    normalized_df["Raw Amount"] = df["Amount"]
    normalized_df["Raw Currency"] = df["Currency"]
    normalized_df["Raw Balance"] = df["Balance"]
    normalized_df["Raw Foreign Currency"] = None
    normalized_df["Raw Foreign Currency Amount"] = None
    normalized_df["Raw Foreign Currency Rate"] = None

    normalized_df["Real Date"] = normalized_df["Raw Real Date"].apply(
        revolut_date_to_naive_datetime_obj
    )
    normalized_df["Bank Date"] = normalized_df["Raw Bank Date"].apply(
        revolut_date_to_naive_datetime_obj
    )
    normalized_df["Payee"] = normalized_df["Raw Payee"]
    normalized_df["Bank Message"] = normalized_df["Raw Bank Message"]
    normalized_df["Amount"] = normalized_df["Raw Amount"].apply(
        amount_to_rounded_decimal
    )
    normalized_df["Currency"] = normalized_df["Raw Currency"]
    normalized_df["Balance"] = normalized_df["Raw Balance"].apply(
        amount_to_rounded_decimal
    )
    normalized_df["Foreign Currency"] = None
    normalized_df["Foreign Currency Amount"] = None
    normalized_df["Foreign Currency Rate"] = None
    normalized_df["Original data"] = df[
        [
            "Type",
            "Product",
            "Started Date",
            "Completed Date",
            "Description",
            "Amount",
            "Fee",
            "Currency",
            "State",
            "Balance",
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


def revolut_csv_transactions_parser(transaction_file):
    # type: (str) -> DataFrame
    df = import_revolut_csv_transaction_file(transaction_file)
    return revolut_csv_transactions_to_general_clerk_format(df)
