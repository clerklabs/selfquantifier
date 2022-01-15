from datetime import datetime
from io import StringIO

import pandas as pd
from pandas.core.frame import DataFrame

from clerkai.transactions.parsers.parse_utils import \
    convert_european_amount_to_decimal


def fi_date_to_datetime_obj(datetime_str):
    # type: (str) -> datetime
    datetime_obj = datetime.strptime(datetime_str, "%d.%m.%Y")
    return datetime_obj


def nordea_fi_reference_number_to_datetime_obj(reference_number):
    reference_number_str = str(reference_number)
    if not reference_number_str.isdigit():
        return None
    if not len(reference_number_str) == 12:
        return None
    datetime_str = reference_number_str[0:6]
    datetime_obj = datetime.strptime(datetime_str, "%y%m%d")
    return datetime_obj


def import_nordea_fi_lang_se_txt_transaction_file(transaction_file):
    # type: (str) -> DataFrame
    with open(transaction_file, "rb") as f:
        contents = f.read()
    # print(contents)
    decoded_contents = contents.decode("utf8")
    # originally, these files use "\n\r\n" as line separator (go figure)
    # but after editing in a sane editor, these will be "\n\n"
    # by replacing "\n\r\n" with "\n\n" we support both pristine and
    # edited files
    line_ending_normalized_contents = decoded_contents.replace("\n\r\n", "\n\n")
    header_split = line_ending_normalized_contents.split("\n\n", 1)
    # print(header_split)
    nordeafi_adjusted_contents = header_split[1].replace("\n\n", "\n")
    # print(nordeafi_adjusted_contents)
    return pd.read_csv(
        StringIO(nordeafi_adjusted_contents),
        sep="\t",
        thousands=".",
        decimal=",",
        dtype=str,
    )


def nordea_fi_lang_se_txt_transactions_to_general_clerk_format(df):
    # type: (DataFrame) -> DataFrame
    normalized_df = pd.DataFrame()
    normalized_df["Raw Real Date"] = df["Referens"]
    normalized_df["Raw Bank Date"] = df["Betalningsdag"]
    normalized_df["Raw Payee"] = df["Mottagare/Betalare"]
    normalized_df["Raw Bank Message"] = df["Meddelande"]
    normalized_df["Raw Amount"] = df["Belopp"]
    normalized_df["Raw Balance"] = None
    normalized_df["Real Date"] = normalized_df["Raw Real Date"].apply(
        nordea_fi_reference_number_to_datetime_obj
    )
    normalized_df["Bank Date"] = normalized_df["Raw Bank Date"].apply(
        fi_date_to_datetime_obj
    )
    normalized_df["Payee"] = normalized_df["Raw Payee"]
    normalized_df["Bank Message"] = normalized_df["Raw Bank Message"]
    normalized_df["Amount"] = normalized_df["Raw Amount"].apply(
        convert_european_amount_to_decimal
    )
    normalized_df["Balance"] = None
    normalized_df["Original data"] = df[
        [
            "Bokningsdag",
            "Valutadag",
            "Betalningsdag",
            "Belopp",
            "Mottagare/Betalare",
            "Kontonummer",
            "BIC",
            "Kontotransaktion",
            "Referens",
            "Betalarens referens",
            "Meddelande",
            "Kortets nummer",
            "Kvitto",
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


def nordea_fi_lang_se_txt_transactions_parser(transaction_file):
    # type: (str) -> DataFrame
    df = import_nordea_fi_lang_se_txt_transaction_file(transaction_file)
    return nordea_fi_lang_se_txt_transactions_to_general_clerk_format(df)
