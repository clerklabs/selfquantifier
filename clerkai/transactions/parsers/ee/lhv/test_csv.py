from os.path import dirname, join, realpath

from clerkai.transactions.parsers.ee.lhv.csv import (
    lhv_ee_csv_transactions_parser, lhv_ee_description_to_datetime_obj)

test_data_dir_path = join(dirname(realpath(__file__)), "test_data")


def test_lhv_ee_description_to_datetime_obj():
    assert (
        lhv_ee_description_to_datetime_obj(
            "(..1234) 2019-04-06 19:15 ALEPA LENTOASEMA \\LENTOASEMA T2 2B SAAP \\VANTAA \\01530 FINFIN"
        ).strftime("%Y-%m-%d %H:%M")
        == "2019-04-06 19:15"
    )


def test_lhv_ee_csv_transactions_parser():
    # type: () -> None
    transaction_file_path = join(
        test_data_dir_path,
        "EE123456789012345678_Account_Statement_2019-08-17.edited.csv",
    )
    transactions_df = lhv_ee_csv_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "%s%s" % (transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected
