from os.path import dirname, join, realpath

from selfquantifier.transactions.parsers.international.revolut.legacy.csv import (
    revolut_legacy_csv_transactions_parser,
    revolut_legacy_currency_from_columns,
)

test_data_dir_path = join(dirname(realpath(__file__)), "test_data")


def test_revolut_legacy_currency_from_columns_sek():
    expected = "SEK"
    actual = revolut_legacy_currency_from_columns(["Paid Out (SEK)"])
    assert actual == expected


def test_revolut_legacy_currency_from_columns_eur():
    expected = "EUR"
    actual = revolut_legacy_currency_from_columns(["Paid Out (EUR)"])
    assert actual == expected


def test_revolut_legacy_currency_from_columns_none():
    expected = None
    actual = revolut_legacy_currency_from_columns(["Foo"])
    assert actual == expected


def test_revolut_legacy_csv_transactions_parser_sek():
    # type: () -> None
    transaction_file_path = join(test_data_dir_path, "revolut-csv-transactions.sek.csv")
    transactions_df = revolut_legacy_csv_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "{}{}".format(transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "{}{}".format(transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_revolut_legacy_csv_transactions_parser_eur():
    # type: () -> None
    transaction_file_path = join(test_data_dir_path, "revolut-csv-transactions.eur.csv")
    transactions_df = revolut_legacy_csv_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "{}{}".format(transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "{}{}".format(transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected
