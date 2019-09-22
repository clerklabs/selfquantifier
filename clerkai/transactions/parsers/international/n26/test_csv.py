from os.path import dirname, join, realpath

from clerkai.transactions.parsers.international.n26.csv import \
    n26_csv_transactions_parser

test_data_dir_path = join(dirname(realpath(__file__)), "test_data")


def test_n26_csv_transactions_parser():
    # type: () -> None
    transaction_file_path = join(test_data_dir_path, "n26-csv-transactions.csv")
    transactions_df = n26_csv_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "%s%s" % (transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected
