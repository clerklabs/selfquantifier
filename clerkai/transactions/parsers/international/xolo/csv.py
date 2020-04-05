import pandas as pd
from pandas.core.frame import DataFrame

from clerkai.transactions.parsers.parse_utils import amount_to_rounded_decimal
from clerkai.utils import is_nan, ymd_date_to_naive_datetime_obj


def import_xolo_csv_transaction_file(transaction_file):
    # type: (str) -> DataFrame
    return pd.read_csv(transaction_file, dtype=str)


def xolo_expenses_vendor_category_bug_fixer(xolo_csv_record):
    if is_nan(xolo_csv_record["Category"]):
        return xolo_csv_record["Vendor"]
    # todo: make only remove from the end
    return xolo_csv_record["Vendor"].replace(xolo_csv_record["Category"], "")


def xolo_csv_transactions_to_general_clerk_format(df):
    # type: (DataFrame) -> DataFrame
    normalized_df = pd.DataFrame()
    normalized_df["Raw Real Date"] = df[
        "Invoice date"
    ]  # not perfect, but better than nothing
    normalized_df["Raw Bank Date"] = df["Paid date"]
    normalized_df["Raw Payee"] = df["Vendor"]
    normalized_df["Raw Bank Message"] = df["Description"]
    normalized_df["Raw Amount"] = df["Amount"]
    normalized_df["Raw Balance"] = None
    normalized_df["Raw Currency"] = df["Currency"]
    normalized_df["Raw Doc Status"] = df["Status"]
    normalized_df["Raw Payment Status"] = df["Status.1"]
    normalized_df["Real Date"] = normalized_df["Raw Real Date"].apply(
        ymd_date_to_naive_datetime_obj
    )
    normalized_df["Bank Date"] = normalized_df["Raw Bank Date"].apply(
        ymd_date_to_naive_datetime_obj
    )
    normalized_df["Payee"] = df.apply(xolo_expenses_vendor_category_bug_fixer, axis=1)
    normalized_df["Bank Message"] = normalized_df["Raw Bank Message"]
    normalized_df["Amount"] = normalized_df["Raw Amount"].apply(
        amount_to_rounded_decimal
    )
    normalized_df["Balance"] = None
    normalized_df["Currency"] = normalized_df["Raw Currency"]
    normalized_df["Doc Status"] = normalized_df["Raw Doc Status"]
    normalized_df["Payment Status"] = normalized_df["Raw Payment Status"]
    normalized_df["Original data"] = df[
        [
            "Vendor",
            "Category",
            "Description",
            "Status",
            "Invoice date",
            "Paid date",
            "Source",
            "Amount",
            "Currency",
            "Status.1",
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
            "Currency",
            "Doc Status",
            "Payment Status",
            "Original data",
            "Raw Real Date",
            "Raw Bank Date",
            "Raw Payee",
            "Raw Bank Message",
            "Raw Amount",
            "Raw Balance",
            "Raw Currency",
            "Raw Doc Status",
            "Raw Payment Status",
        ]
    ]


def xolo_csv_transactions_parser(transaction_file):
    # type: (str) -> DataFrame
    df = import_xolo_csv_transaction_file(transaction_file)
    # print(df.dtypes, df.columns)
    return xolo_csv_transactions_to_general_clerk_format(df)
