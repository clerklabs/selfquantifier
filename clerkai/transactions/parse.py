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


def parse_transaction_files(transaction_files, clerkai_file_path, failfast=False):
    def parse_transaction_file_row(transaction_file):
        transaction_file_path = clerkai_file_path(transaction_file)
        results = None
        error = None
        try:
            parser = parser_by_content_type[transaction_file["Content type"]]
            # print(parser, transaction_file_path)
            results = parser(transaction_file_path)
            results["Source transaction file index"] = transaction_file.name
        except Exception as e:
            error = e
            if failfast:
                raise e
        return pd.Series([results, error], index=['Parse results', 'Error'])

    if len(transaction_files) == 0:
        raise Exception("No transaction files to parse")

    parsed_transaction_file_results = transaction_files.apply(parse_transaction_file_row, axis=1)

    parsed_transaction_files = transaction_files.join(parsed_transaction_file_results)
    return parsed_transaction_files


def transactions_from_parsed_transaction_files(parsed_transaction_files):
    transactions_df = pd.concat(parsed_transaction_files["Parse results"].values, sort=False).reset_index(drop=True)
    transactions_df = pd.merge(transactions_df,
                     parsed_transaction_files.drop(["Parse results", "History reference"], axis=1).add_prefix("Source transaction file: "),
                     left_on="Source transaction file index", right_index=True)
    return transactions_df.drop(["Source transaction file index"], axis=1)
