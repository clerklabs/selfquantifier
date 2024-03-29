import json
import traceback

import pandas as pd

from selfquantifier.transactions.parsers.ee.lhv.csv import (
    lhv_ee_csv_transactions_parser,
)
from selfquantifier.transactions.parsers.fi.nordea.personal.txt import (
    nordea_fi_lang_se_txt_transactions_parser,
)
from selfquantifier.transactions.parsers.international.n26.csv import (
    n26_csv_transactions_parser,
)
from selfquantifier.transactions.parsers.international.nordea.netbank.csv import (
    nordea_netbank_csv_transactions_parser,
)
from selfquantifier.transactions.parsers.international.revolut.csv import (
    revolut_csv_transactions_parser,
)
from selfquantifier.transactions.parsers.international.revolut.legacy.csv import (
    revolut_legacy_csv_transactions_parser,
)
from selfquantifier.transactions.parsers.international.xolo.csv import (
    xolo_csv_transactions_parser,
)
from selfquantifier.transactions.parsers.se.banknorwegian.xlsx import (
    banknorwegian_se_xlsx_transactions_parser,
)
from selfquantifier.transactions.parsers.se.danskebank.personal.csv import (
    danskebank_se_csv_transactions_parser,
)
from selfquantifier.transactions.parsers.se.nordea.personal.internetbanken_privat.xlsx import (
    nordea_se_personal_internetbanken_privat_xlsx_transactions_parser,
)
from selfquantifier.utils import (
    is_nan,
    raw_if_available,
    selfquantifier_input_file_path,
)

parser_by_content_type = {
    "exported-transaction-file/nordea.fi.natbanken-privat.xls": nordea_fi_lang_se_txt_transactions_parser,
    "exported-transaction-file/lhv.ee.account-statement.csv": lhv_ee_csv_transactions_parser,
    "exported-transaction-file/nordea.se.internetbanken-privat.xls": (
        nordea_se_personal_internetbanken_privat_xlsx_transactions_parser
    ),
    "exported-transaction-file/nordea.netbank.csv": nordea_netbank_csv_transactions_parser,
    "exported-transaction-file/danskebank.se.csv": danskebank_se_csv_transactions_parser,
    "exported-transaction-file/xolo.io.expenses.csv": xolo_csv_transactions_parser,
    "exported-transaction-file/revolut.com.csv": revolut_csv_transactions_parser,
    "exported-transaction-file/revolut.com.legacy.csv": revolut_legacy_csv_transactions_parser,
    "exported-transaction-file/n26.com.csv": n26_csv_transactions_parser,
    "exported-transaction-file/avanza.se.transaktioner.csv": None,
    "exported-transaction-file/norwegianreward.se.via-dataminer.xlsx": None,
    "exported-transaction-file/nordea.se.internetbanken-foretag.xls": None,
    "exported-transaction-file/banknorwegian.se.xlsx": banknorwegian_se_xlsx_transactions_parser,
    "exported-transaction-file/banknorwegian.fi.xlsx": banknorwegian_se_xlsx_transactions_parser,
    "exported-transaction-file/clerk.ai.general-transactions-format.xlsx": None,
    "exported-transaction-file/paypal.com.activity-report.csv": None,
    "exported-transaction-file/skatteverket.se.skattekonto": None,
}


def naive_transaction_ids(transactions):
    import jellyfish

    def generate_naive_transaction_id(transaction):
        def none_if_nan(x):
            if is_nan(x):
                return None
            else:
                return x

        id_key_dict = {}
        id_key_dict["real_date"] = none_if_nan(
            raw_if_available("Real Date", transaction)
        )
        id_key_dict["bank_date"] = none_if_nan(
            raw_if_available("Bank Date", transaction)
        )
        payee = none_if_nan(raw_if_available("Payee", transaction))
        bank_message = none_if_nan(raw_if_available("Bank Message", transaction))
        id_key_dict["amount"] = none_if_nan(raw_if_available("Amount", transaction))
        id_key_dict["balance"] = none_if_nan(raw_if_available("Balance", transaction))
        id_key_dict["payee"] = jellyfish.soundex(payee) if type(payee) is str else payee
        id_key_dict["bank_message"] = (
            jellyfish.soundex(bank_message)
            if type(bank_message) is str
            else bank_message
        )
        return json.dumps(id_key_dict, sort_keys=True, default=str, allow_nan=False)

    return transactions.apply(generate_naive_transaction_id, axis=1)


def naive_transaction_id_duplicate_nums(transactions):
    return transactions.groupby(["naive_transaction_id"]).cumcount() + 1


def transaction_ids(transactions):
    copy = transactions.copy()
    copy["naive_transaction_id"] = naive_transaction_ids(copy)
    copy["naive_transaction_id_duplicate_num"] = naive_transaction_id_duplicate_nums(
        copy
    )

    def generate_transaction_id(transaction):
        id_key_dict = {
            "ref": json.loads(transaction["naive_transaction_id"]),
            "ord": transaction["naive_transaction_id_duplicate_num"],
        }
        return json.dumps(id_key_dict)

    return copy.apply(generate_transaction_id, axis=1)


def parse_transaction_files(
    transaction_files, selfquantifier_input_folder_path, keepraw=False, failfast=False
):
    class ContentTypeNotSetError(Exception):
        pass

    class ParserNotAvailableError(Exception):
        pass

    def parse_transaction_file_row(transaction_file):
        transaction_file_path = selfquantifier_input_file_path(
            selfquantifier_input_folder_path, transaction_file
        )
        results = None
        error = None
        error_string = None

        if failfast:
            print(
                "Context (showing since failfast=True) - transaction_file:",
                "%s/%s"
                % (transaction_file["File path"], transaction_file["File name"]),
            )

        def parse():
            content_type = transaction_file["Content type"]
            if not content_type:
                raise ContentTypeNotSetError("Transaction file has no content type set")
            if (
                content_type not in parser_by_content_type
                or not parser_by_content_type[content_type]
            ):
                raise ParserNotAvailableError(
                    "Content type '%s' has no parser" % content_type
                )
            parser = parser_by_content_type[content_type]
            # print(parser, transaction_file_path)
            transactions = parser(transaction_file_path)
            transactions["Source transaction file index"] = transaction_file.name
            # add future join/merge index
            transactions["ID"] = transaction_ids(transactions)
            # drop raw columns
            if not keepraw:
                transactions = transactions.drop(
                    [
                        "Raw Real Date",
                        "Raw Bank Date",
                        "Raw Payee",
                        "Raw Bank Message",
                        "Raw Amount",
                        "Raw Currency",
                        "Raw Balance",
                        "Raw Foreign Currency",
                        "Raw Foreign Currency Amount",
                        "Raw Foreign Currency Rate",
                        "Raw Doc Status",
                        "Raw Payment Status",
                    ],
                    axis=1,
                    errors="ignore",
                )
            return transactions

        # failfast raises errors except expected/benign value errors
        if failfast:
            try:
                results = parse()
            except (ContentTypeNotSetError, ParserNotAvailableError) as e:
                error = e
        else:
            try:
                results = parse()
            except Exception as e:
                error = e
                error_string = f"Error occurred:\n\n{e}\n\n{traceback.format_exc()}\n"
        return pd.Series([results, error_string], index=["Parse results", "Error"])

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
