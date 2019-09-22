import pandas as pd


def import_banknorwegian_se_xlsx_transaction_file(transaction_file):
    return pd.read_excel(transaction_file)


def banknorwegian_se_xlsx_transactions_to_general_clerk_format(df):
    normalized_df = pd.DataFrame()
    normalized_df["Raw Real Date"] = None
    normalized_df["Raw Bank Date"] = df["TransactionDate"]
    normalized_df["Raw Payee"] = df["Merchant Category"]
    normalized_df["Raw Bank Message"] = df["Text"]
    normalized_df["Raw Amount"] = df["Amount"]
    normalized_df["Raw Balance"] = None
    normalized_df["Raw Currency"] = df["Currency"]
    normalized_df["Raw Currency Amount"] = df["Currency Amount"]
    normalized_df["Raw Currency Rate"] = df["Currency Rate"]
    normalized_df["Real Date"] = normalized_df["Raw Real Date"]
    normalized_df["Bank Date"] = normalized_df["Raw Bank Date"]
    normalized_df["Payee"] = normalized_df["Raw Payee"]
    normalized_df["Bank Message"] = normalized_df["Raw Bank Message"]
    normalized_df["Amount"] = normalized_df["Raw Amount"]
    normalized_df["Balance"] = normalized_df["Raw Balance"]
    normalized_df["Currency"] = normalized_df["Raw Currency"]
    normalized_df["Currency Amount"] = normalized_df["Raw Currency Amount"]
    normalized_df["Currency Rate"] = normalized_df["Raw Currency Rate"]
    normalized_df["Original data"] = df[
        [
            "TransactionDate",
            "Text",
            "Type",
            "Currency Amount",
            "Currency Rate",
            "Currency",
            "Amount",
            "Merchant Area",
            "Merchant Category",
            "BookDate",
            "ValueDate",
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


def banknorwegian_se_xlsx_transactions_parser(transaction_file):
    df = import_banknorwegian_se_xlsx_transaction_file(transaction_file)
    return banknorwegian_se_xlsx_transactions_to_general_clerk_format(df)
