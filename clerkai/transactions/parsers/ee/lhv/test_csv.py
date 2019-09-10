from os.path import dirname, join, realpath

from clerkai.transactions.parsers.ee.lhv.csv import \
    lhv_ee_csv_transactions_parser

test_data_dir_path = join(dirname(realpath(__file__)), "test_data")


def test_lhv_ee_csv_transactions_parser():
    # type: () -> None
    transaction_file_path = join(
        test_data_dir_path,
        "EE123456789012345678_Account_Statement_2019-08-17.edited.csv",
    )
    transactions_df = lhv_ee_csv_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
