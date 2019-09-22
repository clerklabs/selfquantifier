import pandas as pd

from clerkai.transactions.parsers.parse_utils import (
    convert_european_amount_to_decimal, ymd_date_to_datetime_obj)


def import_nordea_se_xlsx_transaction_file(transaction_file):
    return pd.read_excel(transaction_file)


def nordea_se_xlsx_transactions_to_general_clerk_format(df):
    normalized_df = pd.DataFrame()
    normalized_df["Raw Real Date"] = None
    normalized_df["Raw Bank Date"] = df["Datum"]
    normalized_df["Raw Payee"] = df["Transaktion"]
    normalized_df["Raw Bank Message"] = df["Kategori"]
    normalized_df["Raw Amount"] = df["Belopp"]
    normalized_df["Raw Balance"] = df["Saldo"]
    # normalized_df["Raw Currency"] = None
    # normalized_df["Raw Doc Status"] = None
    # normalized_df["Raw Payment Status"] = None
    normalized_df["Real Date"] = normalized_df["Raw Real Date"]
    normalized_df["Bank Date"] = normalized_df["Raw Bank Date"].apply(
        ymd_date_to_datetime_obj
    )
    normalized_df["Payee"] = normalized_df["Raw Payee"]
    normalized_df["Bank Message"] = normalized_df["Raw Bank Message"]
    normalized_df["Amount"] = normalized_df["Raw Amount"].apply(
        convert_european_amount_to_decimal
    )
    normalized_df["Balance"] = normalized_df["Raw Balance"].apply(
        convert_european_amount_to_decimal
    )
    # normalized_df["Currency"] = normalized_df["Raw Currency"]
    # normalized_df["Doc Status"] = normalized_df["Raw Doc Status"]
    # normalized_df["Payment Status"] = normalized_df["Raw Payment Status"]
    normalized_df["Original data"] = df[
        ["Datum", "Transaktion", "Kategori", "Belopp", "Saldo"]
    ].to_dict(orient="records")
    return normalized_df[
        [
            "Real Date",
            "Bank Date",
            "Payee",
            "Bank Message",
            "Amount",
            "Balance",
            # "Currency",
            # "Doc Status",
            # "Payment Status",
            "Original data",
            "Raw Real Date",
            "Raw Bank Date",
            "Raw Payee",
            "Raw Bank Message",
            "Raw Amount",
            "Raw Balance",
            # "Raw Currency",
            # "Raw Doc Status",
            # "Raw Payment Status",
        ]
    ]


def nordea_se_xlsx_transactions_parser(transaction_file):
    df = import_nordea_se_xlsx_transaction_file(transaction_file)
    return nordea_se_xlsx_transactions_to_general_clerk_format(df)
