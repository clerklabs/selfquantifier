from os.path import dirname, join, realpath

from selfquantifier.transactions.parsers.se.banknorwegian.xlsx import (
    banknorwegian_se_xlsx_transactions_parser,
)

test_data_dir_path = join(dirname(realpath(__file__)), "test_data")


def test_banknorwegian_se_xlsx_transactions_parser_se():
    # type: () -> None
    transaction_file_path = join(test_data_dir_path, "Statement aug norwegian.xlsx")
    transactions_df = banknorwegian_se_xlsx_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "{}{}".format(transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "{}{}".format(transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_banknorwegian_se_xlsx_transactions_parser_fi():
    # type: () -> None
    transaction_file_path = join(
        test_data_dir_path, "Statement 2020-04-24 banknorwegian.fi.xlsx"
    )
    transactions_df = banknorwegian_se_xlsx_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "{}{}".format(transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "{}{}".format(transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected
