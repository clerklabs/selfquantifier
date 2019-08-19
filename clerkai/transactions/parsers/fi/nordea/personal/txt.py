import datetime
from io import StringIO

import pandas as pd

from clerkai.transactions.parsers.parse_utils import \
    convert_european_amount_to_decimal


def fi_date_to_datetime_obj(datetime_str):
    datetime_obj = datetime.datetime.strptime(datetime_str, '%d.%m.%Y')
    return datetime_obj


def import_nordea_fi_lang_se_transaction_file(transaction_file):
    f = open(transaction_file, "rb")
    contents = f.read()
    f.close()
    # print(contents)
    decoded_contents = contents.decode('utf8')
    # originally, these files use "\n\r\n" as line separator (go figure)
    # but after editing in a sane editor, these will be "\n\n"
    # by replacing "\n\r\n" with "\n\n" we support both pristine and
    # edited files
    line_ending_normalized_contents = decoded_contents.replace("\n\r\n", "\n\n")
    header_split = line_ending_normalized_contents.split("\n\n", 1)
    # print(header_split)
    nordeafi_adjusted_contents = header_split[1].replace("\n\n", "\n")
    # print(nordeafi_adjusted_contents)
    return pd.read_csv(StringIO(nordeafi_adjusted_contents),
                       sep="\t",
                       thousands=".",
                       decimal=",",
                       dtype=str)


def nordea_fi_lang_se_transactions_to_general_clerk_format(df):
    normalized_df = pd.DataFrame()
    normalized_df['Date'] = df['Betalningsdag'].apply(fi_date_to_datetime_obj)
    normalized_df['Payee'] = df['Mottagare/Betalare']
    normalized_df['Memo'] = df['Meddelande']
    normalized_df['Amount'] = df['Belopp'].apply(convert_european_amount_to_decimal)
    return normalized_df


def nordea_fi_lang_se_transactions_parser(transaction_file):
    df = import_nordea_fi_lang_se_transaction_file(transaction_file)
    print(df.info())
    return nordea_fi_lang_se_transactions_to_general_clerk_format(df)
