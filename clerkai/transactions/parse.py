import pandas as pd

from clerkai.transactions.parsers.ee.lhv.csv import \
    lhv_ee_csv_transactions_parser
from clerkai.transactions.parsers.fi.nordea.personal.txt import \
    nordea_fi_lang_se_transactions_parser
from clerkai.transactions.parsers.se.danskebank.personal.csv import \
    danskebank_se_csv_transactions_parser

nordea_fi_lang_fi_transactions_parser = None
nordea_se_transactions_parser = None

parser_by_content_type = {
    "exported-transaction-file/nordea.fi.natbanken-privat.xls": nordea_fi_lang_se_transactions_parser,
    "exported-transaction-file/nordea.fi.verkopankki-henkiloasiakkaat.xls": nordea_fi_lang_fi_transactions_parser,
    "exported-transaction-file/lhv.ee.account-statement.csv": lhv_ee_csv_transactions_parser,
    "exported-transaction-file/nordea.se.internetbanken-privat.xls": nordea_se_transactions_parser,
    "exported-transaction-file/danskebank.se.csv": danskebank_se_csv_transactions_parser,
    "exported-transaction-file/avanza.se.transaktioner.csv": None,
    "exported-transaction-file/norwegianreward.se.via-dataminer.xlsx": None,
    "exported-transaction-file/nordea.se.internetbanken-foretag.xls": None,
    "exported-transaction-file/banknorwegian.se.xlsx": None,
    "exported-transaction-file/clerk.ai.general-transactions-format.xlsx": None,
    "exported-transaction-file/paypal.com.activity-report.csv": None,
    "exported-transaction-file/skatteverket.se.skattekonto": None,
}


def add_naive_transaction_id(transactions):
    import json
    import jellyfish

    def generate_naive_transaction_id(transaction):
        transaction_id_key_dict = {}
        transaction_id_key_dict["date_initiated"] = (
            transaction["Raw Date Initiated"]
            if (transaction["Raw Date Initiated"] is not None)
            else transaction["Date Initiated"]
        )
        transaction_id_key_dict["date_settled"] = (
            transaction["Raw Date Settled"]
            if (transaction["Raw Date Settled"] is not None)
            else transaction["Date Settled"]
        )
        payee = (
            transaction["Raw Payee"]
            if (transaction["Raw Payee"] is not None)
            else transaction["Payee"]
        )
        memo = (
            transaction["Raw Memo"]
            if (transaction["Raw Memo"] is not None)
            else transaction["Memo"]
        )
        transaction_id_key_dict["amount"] = (
            transaction["Raw Amount"]
            if (transaction["Raw Amount"] is not None)
            else transaction["Amount"]
        )
        transaction_id_key_dict["balance"] = (
            transaction["Raw Balance"]
            if (transaction["Raw Balance"] is not None)
            else transaction["Balance"]
        )
        transaction_id_key_dict["payee"] = jellyfish.soundex(payee)
        transaction_id_key_dict["memo"] = jellyfish.soundex(memo)
        return json.dumps(transaction_id_key_dict)

    transactions["naive_transaction_id"] = transactions.apply(
        generate_naive_transaction_id, axis=1
    )
    return transactions


def add_naive_transaction_id_duplicate_num(transactions):
    transactions["naive_transaction_id_duplicate_num"] = (
        transactions.groupby(["naive_transaction_id"]).cumcount() + 1
    )
    return transactions


def add_transaction_id(transactions):
    transactions = add_naive_transaction_id(transactions)
    transactions = add_naive_transaction_id_duplicate_num(transactions)
    return transactions


def parse_transaction_files(transaction_files, clerkai_file_path, failfast=False):
    def parse_transaction_file_row(transaction_file):
        transaction_file_path = clerkai_file_path(transaction_file)
        results = None
        error = None

        def parse():
            content_type = transaction_file["Content type"]
            if not content_type:
                raise ValueError("Transaction file has no content type set")
            if (
                content_type not in parser_by_content_type
                or not parser_by_content_type[content_type]
            ):
                raise ValueError("Content type '%s' has no parser" % content_type)
            parser = parser_by_content_type[content_type]
            # print(parser, transaction_file_path)
            transactions = parser(transaction_file_path)
            transactions["Source transaction file index"] = transaction_file.name
            # add future join/merge index
            return add_transaction_id(transactions)

        # failfast raises errors except expected/benign value errors
        if failfast:
            try:
                results = parse()
            except ValueError as e:
                error = e
        else:
            try:
                results = parse()
            except Exception as e:
                error = e
        return pd.Series([results, error], index=["Parse results", "Error"])

    if len(transaction_files) == 0:
        raise Exception("No transaction files to parse")

    parsed_transaction_file_results = transaction_files.apply(
        parse_transaction_file_row, axis=1
    )

    parsed_transaction_files = transaction_files.join(parsed_transaction_file_results)
    return parsed_transaction_files


def transactions_from_parsed_transaction_files(parsed_transaction_files):
    transactions_df = pd.concat(
        parsed_transaction_files["Parse results"].values, sort=False
    ).reset_index(drop=True)
    transactions_df = pd.merge(
        transactions_df,
        parsed_transaction_files.drop(
            ["Parse results", "History reference"], axis=1
        ).add_prefix("Source transaction file: "),
        left_on="Source transaction file index",
        right_index=True,
    )
    return transactions_df.drop(["Source transaction file index"], axis=1)
