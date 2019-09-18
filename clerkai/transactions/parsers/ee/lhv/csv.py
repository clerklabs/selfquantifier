from datetime import datetime

import pandas as pd
from pandas.core.frame import DataFrame

from clerkai.transactions.parsers.parse_utils import amount_to_rounded_decimal


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


def ymd_date_to_datetime_obj(datetime_str):
    # type: (str) -> datetime
    datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d")
    return datetime_obj


def import_lhv_ee_csv_transaction_file(transaction_file):
    # type: (str) -> DataFrame
    return pd.read_csv(transaction_file)


def lhv_ee_csv_transactions_to_general_clerk_format(df):
    # type: (DataFrame) -> DataFrame
    normalized_df = pd.DataFrame()
    normalized_df["Raw Date Initiated"] = df["Description"]
    normalized_df["Raw Date Settled"] = df["Date"]
    normalized_df["Raw Payee"] = df["Sender/receiver name"]
    normalized_df["Raw Memo"] = df["Description"]
    normalized_df["Raw Amount"] = df["Amount"]
    normalized_df["Raw Balance"] = None
    normalized_df["Date Initiated"] = normalized_df["Raw Date Initiated"].apply(
        lhv_ee_description_to_datetime_obj
    )
    normalized_df["Date Settled"] = normalized_df["Raw Date Settled"].apply(
        ymd_date_to_datetime_obj
    )
    normalized_df["Payee"] = normalized_df["Raw Payee"]
    normalized_df["Memo"] = normalized_df["Raw Memo"]
    normalized_df["Amount"] = normalized_df["Raw Amount"].apply(
        amount_to_rounded_decimal
    )
    normalized_df["Balance"] = normalized_df["Raw Balance"]
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
            "Date Initiated",
            "Date Settled",
            "Payee",
            "Memo",
            "Amount",
            "Balance",
            "Original data",
            "Raw Date Initiated",
            "Raw Date Settled",
            "Raw Payee",
            "Raw Memo",
            "Raw Amount",
            "Raw Balance",
        ]
    ]


def lhv_ee_csv_transactions_parser(transaction_file):
    # type: (str) -> DataFrame
    df = import_lhv_ee_csv_transaction_file(transaction_file)
    return lhv_ee_csv_transactions_to_general_clerk_format(df)
