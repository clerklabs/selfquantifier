import pandas as pd
from pandas.core.frame import DataFrame

from clerkai.transactions.parsers.parse_utils import amount_to_rounded_decimal
from clerkai.utils import ymd_date_to_naive_datetime_obj


def import_n26_csv_transaction_file(transaction_file):
    # type: (str) -> DataFrame
    return pd.read_csv(transaction_file)


def n26_csv_transactions_to_general_clerk_format(df):
    # type: (DataFrame) -> DataFrame
    normalized_df = pd.DataFrame()
    normalized_df["Raw Real Date"] = None
    normalized_df["Raw Bank Date"] = df["Date"]
    normalized_df["Raw Payee"] = df["Payee"]
    normalized_df["Raw Bank Message"] = df["Payment reference"]
    normalized_df["Raw Amount"] = df["Amount (EUR)"]
    normalized_df["Raw Currency"] = None
    normalized_df["Raw Balance"] = None
    normalized_df["Raw Foreign Currency"] = df["Type Foreign Currency"]
    normalized_df["Raw Foreign Currency Amount"] = df["Amount (Foreign Currency)"]
    normalized_df["Raw Foreign Currency Rate"] = df["Exchange Rate"]
    normalized_df["Real Date"] = normalized_df["Raw Real Date"]
    normalized_df["Bank Date"] = normalized_df["Raw Bank Date"].apply(
        ymd_date_to_naive_datetime_obj
    )
    normalized_df["Payee"] = normalized_df["Raw Payee"]
    normalized_df["Bank Message"] = normalized_df["Raw Bank Message"]
    normalized_df["Amount"] = normalized_df["Raw Amount"].apply(
        amount_to_rounded_decimal
    )
    normalized_df["Currency"] = "EUR"
    normalized_df["Balance"] = None
    normalized_df["Foreign Currency"] = normalized_df["Raw Foreign Currency"]
    normalized_df["Foreign Currency Amount"] = normalized_df[
        "Raw Foreign Currency Amount"
    ].apply(amount_to_rounded_decimal)
    normalized_df["Foreign Currency Rate"] = normalized_df[
        "Raw Foreign Currency Rate"
    ].apply(amount_to_rounded_decimal, accuracy=14)
    normalized_df["Original data"] = df[
        [
            "Date",
            "Payee",
            "Account number",
            "Transaction type",
            "Payment reference",
            "Category",
            "Amount (EUR)",
            "Amount (Foreign Currency)",
            "Type Foreign Currency",
            "Exchange Rate",
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


def n26_csv_transactions_parser(transaction_file):
    # type: (str) -> DataFrame
    df = import_n26_csv_transaction_file(transaction_file)
    return n26_csv_transactions_to_general_clerk_format(df)
