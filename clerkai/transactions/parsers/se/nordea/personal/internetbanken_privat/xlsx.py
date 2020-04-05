from datetime import datetime

import pandas as pd

from clerkai.transactions.parsers.parse_utils import \
    convert_european_amount_to_decimal
from clerkai.utils import ymd_date_to_naive_datetime_obj


def import_nordea_se_personal_internetbanken_privat_xlsx_transaction_file(
    transaction_file,
):
    return pd.read_excel(transaction_file)


def nordea_se_transaction_text_to_datetime_obj(description_str):
    import re

    p = re.compile("^Kortköp (\\d{6})", re.IGNORECASE)
    m = p.search(description_str)
    if m and len(m.groups()) > 0:
        datetime_str = m.groups()[0]
    else:
        return None
    datetime_obj = datetime.strptime(datetime_str, "%y%m%d")
    return datetime_obj


def nordea_se_personal_internetbanken_privat_xlsx_transactions_to_general_clerk_format(
    df,
):
    normalized_df = pd.DataFrame()
    # Opting not to set Transaktion here even though it sometimes contains the date
    normalized_df["Raw Real Date"] = None
    normalized_df["Raw Bank Date"] = df["Datum"]
    normalized_df["Raw Payee"] = df["Transaktion"]
    normalized_df["Raw Bank Message"] = df["Kategori"]
    normalized_df["Raw Amount"] = df["Belopp"]
    normalized_df["Raw Balance"] = df["Saldo"]
    normalized_df["Real Date"] = df["Transaktion"].apply(
        nordea_se_transaction_text_to_datetime_obj
    )
    normalized_df["Bank Date"] = normalized_df["Raw Bank Date"].apply(
        ymd_date_to_naive_datetime_obj
    )
    normalized_df["Payee"] = normalized_df["Raw Payee"]
    normalized_df["Bank Message"] = normalized_df["Raw Bank Message"]
    normalized_df["Amount"] = normalized_df["Raw Amount"].apply(
        convert_european_amount_to_decimal
    )
    normalized_df["Balance"] = normalized_df["Raw Balance"].apply(
        convert_european_amount_to_decimal
    )
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
            "Original data",
            "Raw Real Date",
            "Raw Bank Date",
            "Raw Payee",
            "Raw Bank Message",
            "Raw Amount",
            "Raw Balance",
        ]
    ]


def nordea_se_personal_internetbanken_privat_xlsx_transactions_parser(transaction_file):
    df = import_nordea_se_personal_internetbanken_privat_xlsx_transaction_file(
        transaction_file
    )
    return nordea_se_personal_internetbanken_privat_xlsx_transactions_to_general_clerk_format(
        df
    )
