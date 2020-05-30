from os.path import dirname, join, realpath

from clerkai.transactions.parsers.international.nordea.netbank.csv import \
    nordea_netbank_csv_transactions_parser

test_data_dir_path = join(dirname(realpath(__file__)), "test_data")


def test_nordea_netbank_csv_transactions_parser_se_lang_sv():
    # type: () -> None
    transaction_file_path = join(
        test_data_dir_path, "PERSONKONTO 1234 56 78901 - 2020.01.25 16.47.edited.csv"
    )
    transactions_df = nordea_netbank_csv_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "%s%s" % (transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_nordea_netbank_csv_transactions_parser_fi_lang_sv():
    # type: () -> None
    transaction_file_path = join(
        test_data_dir_path,
        "Personkonto FI12 3456 7890 1234 56 - 2020.05.30 17.51 - sv.edited.csv",
    )
    transactions_df = nordea_netbank_csv_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "%s%s" % (transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_nordea_netbank_csv_transactions_parser_fi_lang_fi():
    # type: () -> None
    transaction_file_path = join(
        test_data_dir_path,
        "Personkonto FI12 3456 7890 1234 56 - 2020.05.30 17.47 - fi.edited.csv",
    )
    transactions_df = nordea_netbank_csv_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "%s%s" % (transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_nordea_netbank_csv_transactions_parser_fi_lang_en():
    # type: () -> None
    transaction_file_path = join(
        test_data_dir_path,
        "Personkonto FI12 3456 7890 1234 56 - 2020.05.30 17.49 - en.edited.csv",
    )
    transactions_df = nordea_netbank_csv_transactions_parser(transaction_file_path)
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "%s%s" % (transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected
