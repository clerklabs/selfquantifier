import pandas as pd
from pandas.core.frame import DataFrame

from clerkai.transactions.parsers.parse_utils import amount_to_rounded_decimal
from clerkai.utils import (fi_dmy_date_to_naive_datetime_obj,
                           ymd_date_to_naive_datetime_obj)


def import_nordea_netbank_csv_transaction_file(transaction_file):
    # type: (str) -> DataFrame
    return pd.read_csv(transaction_file, sep=";", decimal=",",)


def nordea_netbank_csv_transactions_to_general_clerk_format(df):
    # type: (DataFrame) -> DataFrame
    normalized_df = pd.DataFrame()

    normalized_df["Raw Real Date"] = None

    # these files are found in three languages: sv, fi, en
    if "Bokföringsdag" in df.columns:
        normalized_df["Raw Bank Date"] = df["Bokföringsdag"]
        normalized_df["Raw Payer"] = df["Avsändare"]
        normalized_df["Raw Payee"] = df["Mottagare"]
        normalized_df["Raw Bank Message"] = df["Rubrik"]
        normalized_df["Raw Amount"] = df["Belopp"]
        normalized_df["Raw Balance"] = df["Saldo"]
        normalized_df["Raw Currency"] = df["Valuta"]
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
    if "Kirjauspäivä" in df.columns:
        normalized_df["Raw Bank Date"] = df["Kirjauspäivä"]
        normalized_df["Raw Payer"] = df["Maksaja"]
        normalized_df["Raw Payee"] = df["Maksunsaaja"]
        normalized_df["Raw Bank Message"] = df["Otsikko"]
        normalized_df["Raw Amount"] = df["Määrä"]
        normalized_df["Raw Balance"] = df["Saldo"]
        normalized_df["Raw Currency"] = df["Valuutta"]
        normalized_df["Original data"] = df[
            [
                "Kirjauspäivä",
                "Määrä",
                "Maksaja",
                "Maksunsaaja",
                "Nimi",
                "Otsikko",
                "Saldo",
                "Valuutta",
            ]
        ].to_dict(orient="records")
    if "Booking date" in df.columns:
        normalized_df["Raw Bank Date"] = df["Booking date"]
        normalized_df["Raw Payer"] = df["Sender"]
        normalized_df["Raw Payee"] = df["Recipient"]
        normalized_df["Raw Bank Message"] = df["Title"]
        normalized_df["Raw Amount"] = df["Amount"]
        normalized_df["Raw Balance"] = df["Balance"]
        normalized_df["Raw Currency"] = df["Currency"]
        normalized_df["Original data"] = df[
            [
                "Booking date",
                "Amount",
                "Sender",
                "Recipient",
                "Name",
                "Title",
                "Balance",
                "Currency",
            ]
        ].to_dict(orient="records")

    # swedish and finnish netbank files have different date formats
    first_encountered_date = (
        normalized_df["Raw Bank Date"]
        .loc[~normalized_df["Raw Bank Date"].isnull()]
        .iloc[0]
    )
    if "." in first_encountered_date:
        date_parser = fi_dmy_date_to_naive_datetime_obj
    elif "-" in first_encountered_date:
        date_parser = ymd_date_to_naive_datetime_obj
    else:
        raise Exception("Unexpected date format encountered")

    normalized_df["Real Date"] = None
    normalized_df["Bank Date"] = normalized_df["Raw Bank Date"].apply(date_parser)
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


def nordea_netbank_csv_transactions_parser(transaction_file):
    # type: (str) -> DataFrame
    df = import_nordea_netbank_csv_transaction_file(transaction_file)
    return nordea_netbank_csv_transactions_to_general_clerk_format(df)
