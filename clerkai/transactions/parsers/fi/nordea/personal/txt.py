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
    datetime_str = reference_number_str[0:6]
    datetime_obj = datetime.strptime(datetime_str, "%y%m%d")
    return datetime_obj


def import_nordea_fi_lang_se_transaction_file(transaction_file):
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


def nordea_fi_lang_se_transactions_to_general_clerk_format(df):
    # type: (DataFrame) -> DataFrame
    normalized_df = pd.DataFrame()
    normalized_df["Raw Date Initiated"] = df["Referens"]
    normalized_df["Raw Date Settled"] = df["Betalningsdag"]
    normalized_df["Raw Payee"] = df["Mottagare/Betalare"]
    normalized_df["Raw Memo"] = df["Meddelande"]
    normalized_df["Raw Amount"] = df["Belopp"]
    normalized_df["Raw Balance"] = None
    normalized_df["Date Initiated"] = normalized_df["Raw Date Initiated"].apply(
        nordea_fi_reference_number_to_datetime_obj
    )
    normalized_df["Date Settled"] = normalized_df["Raw Date Settled"].apply(
        fi_date_to_datetime_obj
    )
    normalized_df["Payee"] = normalized_df["Raw Payee"]
    normalized_df["Memo"] = normalized_df["Raw Memo"]
    normalized_df["Amount"] = normalized_df["Raw Amount"].apply(
        convert_european_amount_to_decimal
    )
    normalized_df["Balance"] = normalized_df["Raw Balance"]
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


def nordea_fi_lang_se_transactions_parser(transaction_file):
    # type: (str) -> DataFrame
    df = import_nordea_fi_lang_se_transaction_file(transaction_file)
    return nordea_fi_lang_se_transactions_to_general_clerk_format(df)
