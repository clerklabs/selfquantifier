import pandas as pd
from pandas.core.frame import DataFrame

from clerkai.transactions.parsers.parse_utils import amount_to_rounded_decimal
from clerkai.utils import ymd_date_to_naive_datetime_obj


def import_nordea_se_personal_netbank_csv_transaction_file(transaction_file):
    # type: (str) -> DataFrame
    return pd.read_csv(transaction_file, sep=";", decimal=",",)


def nordea_se_personal_netbank_csv_transactions_to_general_clerk_format(df):
    # type: (DataFrame) -> DataFrame
    normalized_df = pd.DataFrame()

    normalized_df["Raw Real Date"] = None
    normalized_df["Raw Bank Date"] = df["Bokföringsdag"]
    normalized_df["Raw Payer"] = df["Avsändare"]
    normalized_df["Raw Payee"] = df["Mottagare"]
    normalized_df["Raw Bank Message"] = df["Rubrik"]
    normalized_df["Raw Amount"] = df["Belopp"]
    normalized_df["Raw Balance"] = df["Saldo"]
    normalized_df["Raw Currency"] = df["Valuta"]

    normalized_df["Real Date"] = None
    normalized_df["Bank Date"] = normalized_df["Raw Bank Date"].apply(
        ymd_date_to_naive_datetime_obj
    )
    normalized_df["Payer"] = normalized_df["Raw Payer"]
    normalized_df["Payee"] = normalized_df["Raw Payee"]
    normalized_df["Bank Message"] = normalized_df["Raw Bank Message"]
    normalized_df["Amount"] = normalized_df["Raw Amount"].apply(
        amount_to_rounded_decimal
    )
    normalized_df["Balance"] = normalized_df["Raw Balance"].apply(
        amount_to_rounded_decimal
    )
    normalized_df["Currency"] = normalized_df["Raw Currency"]

    normalized_df["Original data"] = df[
        [
            "Bokföringsdag",
            "Belopp",
            "Avsändare",
            "Mottagare",
            "Namn",
            "Rubrik",
            "Saldo",
            "Valuta",
        ]
    ].to_dict(orient="records")
    return normalized_df[
        [
            "Real Date",
            "Bank Date",
            "Payer",
            "Payee",
            "Bank Message",
            "Amount",
            "Balance",
            "Currency",
            "Original data",
            "Raw Real Date",
            "Raw Bank Date",
            "Raw Payer",
            "Raw Payee",
            "Raw Bank Message",
            "Raw Amount",
            "Raw Balance",
            "Raw Currency",
        ]
    ]


def nordea_se_personal_netbank_csv_transactions_parser(transaction_file):
    # type: (str) -> DataFrame
    df = import_nordea_se_personal_netbank_csv_transaction_file(transaction_file)
    return nordea_se_personal_netbank_csv_transactions_to_general_clerk_format(df)
