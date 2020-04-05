from datetime import datetime

import pandas as pd
from pandas.core.frame import DataFrame

from clerkai.transactions.parsers.parse_utils import amount_to_rounded_decimal
from clerkai.utils import ymd_date_to_naive_datetime_obj


def lhv_ee_description_to_datetime_obj(description_str):
    import re

    p = re.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2})", re.IGNORECASE)
    m = p.search(description_str)
    if m and len(m.groups()) > 0:
        datetime_str = m.groups()[0]
    else:
        return None
    datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
    return datetime_obj


def import_lhv_ee_csv_transaction_file(transaction_file):
    # type: (str) -> DataFrame
    return pd.read_csv(transaction_file)


def lhv_ee_csv_transactions_to_general_clerk_format(df):
    # type: (DataFrame) -> DataFrame
    normalized_df = pd.DataFrame()
    normalized_df["Raw Real Date"] = df["Description"]
    normalized_df["Raw Bank Date"] = df["Date"]
    normalized_df["Raw Payee"] = df["Sender/receiver name"]
    normalized_df["Raw Bank Message"] = df["Description"]
    normalized_df["Raw Amount"] = df["Amount"]
    normalized_df["Raw Balance"] = None
    normalized_df["Real Date"] = normalized_df["Raw Real Date"].apply(
        lhv_ee_description_to_datetime_obj
    )
    normalized_df["Bank Date"] = normalized_df["Raw Bank Date"].apply(
        ymd_date_to_naive_datetime_obj
    )
    normalized_df["Payee"] = normalized_df["Raw Payee"]
    normalized_df["Bank Message"] = normalized_df["Raw Bank Message"]
    normalized_df["Amount"] = normalized_df["Raw Amount"].apply(
        amount_to_rounded_decimal
    )
    normalized_df["Balance"] = None
    normalized_df["Original data"] = df[
        [
            "Customer account no",
            "Document no",
            "Date",
            "Sender/receiver account",
            "Sender/receiver name",
            "Sender bank code",
            "Empty",
            "Debit/Credit (D/C)",
            "Amount",
            "Reference number",
            "Archiving code",
            "Description",
            "Fee",
            "Currency",
            "Personal code or register code",
            "Sender/receiver bank BIC",
            "Ultimate debtor name",
            "Transaction reference",
            "Account servicer reference",
        ]
    ].to_dict(orient="records")
    return normalized_df[
        [
            "Real Date",
            "Bank Date",
            "Payee",
            "Bank Message",
            "Amount",
            "Balance",
            "Original data",
            "Raw Real Date",
            "Raw Bank Date",
            "Raw Payee",
            "Raw Bank Message",
            "Raw Amount",
            "Raw Balance",
        ]
    ]


def lhv_ee_csv_transactions_parser(transaction_file):
    # type: (str) -> DataFrame
    df = import_lhv_ee_csv_transaction_file(transaction_file)
    return lhv_ee_csv_transactions_to_general_clerk_format(df)
