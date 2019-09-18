from datetime import datetime

import pandas as pd

from clerkai.transactions.parse import (naive_transaction_id_duplicate_nums,
                                        naive_transaction_ids, transaction_ids)

transactions_df = pd.DataFrame(
    {
        "Raw Date Initiated": [None],
        "Raw Date Settled": ["2019/05/02"],
        "Raw Payee": ["Acme-Industries 123 Inc"],
        "Raw Memo": ["Foo"],
        "Raw Amount": ["3.000,12"],
        "Raw Balance": [None],
        "Date Initiated": [None],
        "Date Settled": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
        "Payee": ["Acme-Industries 123 Inc"],
        "Memo": ["Foo"],
        "Amount": [3000.12],
        "Balance": [None],
    }
)


def test_naive_transaction_ids():
    df = transactions_df.copy()
    df["naive_transaction_id"] = naive_transaction_ids(df)
    expected = pd.DataFrame(
        {
            "Raw Date Initiated": [None],
            "Raw Date Settled": ["2019/05/02"],
            "Raw Payee": ["Acme-Industries 123 Inc"],
            "Raw Memo": ["Foo"],
            "Raw Amount": ["3.000,12"],
            "Raw Balance": [None],
            "Date Initiated": [None],
            "Date Settled": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
            "Payee": ["Acme-Industries 123 Inc"],
            "Memo": ["Foo"],
            "Amount": [3000.12],
            "Balance": [None],
            "naive_transaction_id": [
                (
                    '{"date_initiated": null, "date_settled": '
                    '"2019/05/02", "amount": "3.000,12", "balance": '
                    'null, "payee": "A255", "memo": "F000"}'
                )
            ],
        }
    )
    assert df.to_dict(orient="records") == expected.to_dict(orient="records")


def test_add_duplicate_num():
    import pandas as pd

    df = pd.DataFrame(
        {
            "foo": [0, 0, 0, 3, 3, 5, 5],
            "f_key": [1, 2, 1, 2, 3, 1, 1],
            "values": ["red", "blue", "green", "yellow", "orange", "violet", "cyan"],
        }
    )

    df["duplicate_num"] = df.groupby(["foo", "f_key"]).cumcount() + 1

    expected = pd.DataFrame(
        {
            "foo": [0, 0, 0, 3, 3, 5, 5],
            "f_key": [1, 2, 1, 2, 3, 1, 1],
            "values": ["red", "blue", "green", "yellow", "orange", "violet", "cyan"],
            "duplicate_num": [1, 1, 2, 1, 1, 1, 2],
        }
    )

    assert df.to_dict(orient="records") == expected.to_dict(orient="records")


def test_naive_transaction_id_duplicate_nums():
    df = transactions_df.copy()
    df["naive_transaction_id"] = naive_transaction_ids(df)
    df2 = df.copy()
    df2["naive_transaction_id_duplicate_num"] = naive_transaction_id_duplicate_nums(df2)
    expected = pd.DataFrame(
        {
            "Raw Date Initiated": [None],
            "Raw Date Settled": ["2019/05/02"],
            "Raw Payee": ["Acme-Industries 123 Inc"],
            "Raw Memo": ["Foo"],
            "Raw Amount": ["3.000,12"],
            "Raw Balance": [None],
            "Date Initiated": [None],
            "Date Settled": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
            "Payee": ["Acme-Industries 123 Inc"],
            "Memo": ["Foo"],
            "Amount": [3000.12],
            "Balance": [None],
            "naive_transaction_id": [
                (
                    '{"date_initiated": null, "date_settled": '
                    '"2019/05/02", "amount": "3.000,12", "balance": '
                    'null, "payee": "A255", "memo": "F000"}'
                )
            ],
            "naive_transaction_id_duplicate_num": [1],
        }
    )
    assert df2.to_dict(orient="records") == expected.to_dict(orient="records")


def test_transaction_ids():
    df = transactions_df.copy()
    df["ID"] = transaction_ids(df)
    expected = pd.DataFrame(
        {
            "Raw Date Initiated": [None],
            "Raw Date Settled": ["2019/05/02"],
            "Raw Payee": ["Acme-Industries 123 Inc"],
            "Raw Memo": ["Foo"],
            "Raw Amount": ["3.000,12"],
            "Raw Balance": [None],
            "Date Initiated": [None],
            "Date Settled": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
            "Payee": ["Acme-Industries 123 Inc"],
            "Memo": ["Foo"],
            "Amount": [3000.12],
            "Balance": [None],
            "ID": [
                (
                    '{"ref": {"date_initiated": null, "date_settled": "2019/05/02", "amount": "3.000,12",'
                    ' "balance": null, "payee": "A255", "memo": "F000"}, "ord": 1}'
                )
            ],
        }
    )
    assert df.to_dict(orient="records") == expected.to_dict(orient="records")
