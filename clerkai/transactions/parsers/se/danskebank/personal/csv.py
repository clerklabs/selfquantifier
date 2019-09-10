from datetime import datetime

import pandas as pd
from pandas.core.frame import DataFrame

from clerkai.transactions.parsers.parse_utils import amount_to_rounded_decimal


def ymd_date_to_datetime_obj(datetime_str):
    # type: (str) -> datetime
    datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d")
    return datetime_obj


def import_danskebank_se_csv_transaction_file(transaction_file):
    # type: (str) -> DataFrame
    return pd.read_csv(
        transaction_file,
        encoding="iso8859_15",
        sep=";",
        decimal=",",
        quotechar='"',
        thousands=".",
    )


def danskebank_se_csv_transactions_to_general_clerk_format(df):
    # type: (DataFrame) -> DataFrame
    normalized_df = pd.DataFrame()

    normalized_df["Date"] = df["Bokföringsdag"].apply(ymd_date_to_datetime_obj)
    normalized_df["Payee"] = None
    normalized_df["Memo"] = df["Specifikation"]
    normalized_df["Amount"] = df["Belopp"].apply(amount_to_rounded_decimal)
    normalized_df["Balance"] = df["Saldo"].apply(amount_to_rounded_decimal)

    normalized_df["Original data"] = df[
        ["Bokföringsdag", "Specifikation", "Belopp", "Saldo", "Status", "Avstämt"]
    ].to_dict(orient="records")
    return normalized_df[
        ["Date", "Payee", "Memo", "Amount", "Balance", "Original data"]
    ]


def danskebank_se_csv_transactions_parser(transaction_file):
    # type: (str) -> DataFrame
    df = import_danskebank_se_csv_transaction_file(transaction_file)
    return danskebank_se_csv_transactions_to_general_clerk_format(df)
