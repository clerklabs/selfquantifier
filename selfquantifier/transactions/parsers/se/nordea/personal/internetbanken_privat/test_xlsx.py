from os.path import dirname, join, realpath

from clerkai.transactions.parsers.se.nordea.personal.internetbanken_privat.xlsx import (
    nordea_se_personal_internetbanken_privat_xlsx_transactions_parser,
    nordea_se_transaction_text_to_datetime_obj)

test_data_dir_path = join(dirname(realpath(__file__)), "test_data")


def test_nordea_se_transaction_text_to_datetime_obj():
    assert (
        nordea_se_transaction_text_to_datetime_obj(
            "Kortköp 181008 Conrad Electronic"
        ).strftime("%Y-%m-%d")
        == "2018-10-08"
    )


def test_nordea_se_personal_internetbanken_privat_xlsx_transactions_parser():
    # type: () -> None
    transaction_file_path = join(test_data_dir_path, "export.personkonto.edited.xls")
    transactions_df = nordea_se_personal_internetbanken_privat_xlsx_transactions_parser(
        transaction_file_path
    )
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "%s%s" % (transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_nordea_se_personal_internetbanken_privat_xlsx_transactions_parser_2():
    # type: () -> None
    transaction_file_path = join(test_data_dir_path, "export.personkonto.edited.2.xls")
    transactions_df = nordea_se_personal_internetbanken_privat_xlsx_transactions_parser(
        transaction_file_path
    )
    assert not transactions_df.empty
    actual = transactions_df.to_csv(index=False)
    actual_file_path = "%s%s" % (transaction_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (transaction_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected
