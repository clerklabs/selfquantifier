from os.path import dirname, join, realpath

from selfquantifier.transactions.parsers.international.revolut.csv import (
    revolut_csv_transactions_parser,
)

test_data_dir_path = join(dirname(realpath(__file__)), "test_data")


def test_revolut_csv_transactions_parser_sek():
    # type: () -> None
    transaction_file_path = join(
        test_data_dir_path, "account-statement_2021-04-01_2021-04-06_en_123456.sek.csv"
    )
    transactions_df = revolut_csv_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "{}{}".format(transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "{}{}".format(transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_revolut_csv_transactions_parser_eur():
    # type: () -> None
    transaction_file_path = join(
        test_data_dir_path, "account-statement_2021-04-01_2021-04-06_en_234567.eur.csv"
    )
    transactions_df = revolut_csv_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "{}{}".format(transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "{}{}".format(transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_revolut_csv_transactions_parser_eur_with_pending():
    # type: () -> None
    transaction_file_path = join(
        test_data_dir_path, "account-statement_2022-01-05_2022-01-05_en_234567.eur.csv"
    )
    transactions_df = revolut_csv_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "{}{}".format(transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "{}{}".format(transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_revolut_csv_transactions_parser_eur_only_pending():
    # type: () -> None
    transaction_file_path = join(
        test_data_dir_path,
        "account-statement_2022-01-05_2022-01-05_en_234567.only-pending.eur.csv",
    )
    transactions_df = revolut_csv_transactions_parser(transaction_file_path)
    assert transactions_df.empty
