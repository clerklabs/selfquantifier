from os.path import dirname, join, realpath

from clerkai.transactions.parsers.fi.nordea.personal.txt import \
    nordea_fi_lang_se_txt_transactions_parser

test_data_dir_path = join(dirname(realpath(__file__)), "test_data")


def test_nordea_fi_lang_se_txt_transactions_parser():
    # type: () -> None
    transaction_file_path = join(
        test_data_dir_path,
        "Transaktioner_FI1234567890123456_20190406_20190816.edited.txt",
    )
    transactions_df = nordea_fi_lang_se_txt_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "%s%s" % (transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_nordea_fi_lang_se_txt_transactions_parser_2():
    # type: () -> None
    transaction_file_path = join(
        test_data_dir_path,
        "Transaktioner_FI1234567890123456_20191028_20200125.edited.2.txt",
    )
    transactions_df = nordea_fi_lang_se_txt_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "%s%s" % (transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected
