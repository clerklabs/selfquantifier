from datetime import datetime

import numpy as np
import pandas as pd

from clerkai.transactions.parse import (naive_transaction_id_duplicate_nums,
                                        naive_transaction_ids, transaction_ids)

transactions_df = pd.DataFrame(
    {
        "Raw Real Date": [None],
        "Raw Bank Date": ["2019/05/02"],
        "Raw Payee": ["Acme-Industries 123 Inc"],
        "Raw Bank Message": ["Foo"],
        "Raw Amount": ["3.000,12"],
        "Raw Balance": [None],
        "Real Date": [None],
        "Bank Date": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
        "Payee": ["Acme-Industries 123 Inc"],
        "Bank Message": ["Foo"],
        "Amount": [3000.12],
        "Balance": [None],
    }
)

transactions_with_nan_df = pd.DataFrame(
    {
        "Raw Real Date": [float("NaN")],
        "Raw Bank Date": ["2019/05/02"],
        "Raw Payee": ["Acme-Industries 123 Inc"],
        "Raw Bank Message": ["Foo"],
        "Raw Amount": ["3.000,12"],
        "Raw Balance": [None],
        "Real Date": [None],
        "Bank Date": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
        "Payee": ["Acme-Industries 123 Inc"],
        "Bank Message": ["Foo"],
        "Amount": [3000.12],
        "Balance": [None],
    }
)

transactions_without_raw_columns_df = pd.DataFrame(
    {
        "Real Date": [None],
        "Bank Date": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
        "Payee": ["Acme-Industries 123 Inc"],
        "Bank Message": ["Foo"],
        "Amount": [3000.12],
        "Balance": [None],
    }
)

transactions_without_raw_columns_and_some_core_columns_df = pd.DataFrame(
    {
        "Bank Date": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
        "Payee": ["Acme-Industries 123 Inc"],
        "Amount": [3000.12],
        "Balance": [None],
    }
)


def test_naive_transaction_ids():
    df = transactions_df.copy()
    df["naive_transaction_id"] = naive_transaction_ids(df)
    expected = pd.DataFrame(
        {
            "Raw Real Date": [None],
            "Raw Bank Date": ["2019/05/02"],
            "Raw Payee": ["Acme-Industries 123 Inc"],
            "Raw Bank Message": ["Foo"],
            "Raw Amount": ["3.000,12"],
            "Raw Balance": [None],
            "Real Date": [None],
            "Bank Date": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
            "Payee": ["Acme-Industries 123 Inc"],
            "Bank Message": ["Foo"],
            "Amount": [3000.12],
            "Balance": [None],
            "naive_transaction_id": [
                (
                    '{"amount": "3.000,12", "balance": null, "bank_date": "2019/05/02", '
                    '"bank_message": "F000", "payee": "A255", "real_date": null}'
                )
            ],
        }
    )
    assert df.to_dict(orient="records") == expected.to_dict(orient="records")


def test_naive_transaction_ids_with_nan():
    df = transactions_with_nan_df.copy()
    df["naive_transaction_id"] = naive_transaction_ids(df)
    expected = pd.DataFrame(
        {
            "Raw Real Date": [float("NaN")],
            "Raw Bank Date": ["2019/05/02"],
            "Raw Payee": ["Acme-Industries 123 Inc"],
            "Raw Bank Message": ["Foo"],
            "Raw Amount": ["3.000,12"],
            "Raw Balance": [None],
            "Real Date": [None],
            "Bank Date": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
            "Payee": ["Acme-Industries 123 Inc"],
            "Bank Message": ["Foo"],
            "Amount": [3000.12],
            "Balance": [None],
            "naive_transaction_id": [
                (
                    '{"amount": "3.000,12", "balance": null, "bank_date": "2019/05/02", '
                    '"bank_message": "F000", "payee": "A255", "real_date": null}'
                )
            ],
        }
    )
    assert df.replace({np.nan: None}).to_dict(orient="records") == expected.replace(
        {np.nan: None}
    ).to_dict(orient="records")


def test_naive_transaction_ids_without_raw_columns():
    df = transactions_without_raw_columns_df.copy()
    df["naive_transaction_id"] = naive_transaction_ids(df)
    expected = pd.DataFrame(
        {
            "Real Date": [None],
            "Bank Date": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
            "Payee": ["Acme-Industries 123 Inc"],
            "Bank Message": ["Foo"],
            "Amount": [3000.12],
            "Balance": [None],
            "naive_transaction_id": [
                (
                    '{"amount": 3000.12, "balance": null, "bank_date": "2019-05-02 00:00:00", '
                    '"bank_message": "F000", "payee": "A255", "real_date": null}'
                )
            ],
        }
    )
    assert df.to_dict(orient="records") == expected.to_dict(orient="records")


def test_naive_transaction_ids_without_raw_columns_and_some_core_columns():
    df = transactions_without_raw_columns_and_some_core_columns_df.copy()
    df["naive_transaction_id"] = naive_transaction_ids(df)
    expected = pd.DataFrame(
        {
            "Bank Date": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
            "Payee": ["Acme-Industries 123 Inc"],
            "Amount": [3000.12],
            "Balance": [None],
            "naive_transaction_id": [
                (
                    '{"amount": 3000.12, "balance": null, "bank_date": "2019-05-02 00:00:00",'
                    ' "bank_message": null, "payee": "A255", "real_date": null}'
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
            "Raw Real Date": [None],
            "Raw Bank Date": ["2019/05/02"],
            "Raw Payee": ["Acme-Industries 123 Inc"],
            "Raw Bank Message": ["Foo"],
            "Raw Amount": ["3.000,12"],
            "Raw Balance": [None],
            "Real Date": [None],
            "Bank Date": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
            "Payee": ["Acme-Industries 123 Inc"],
            "Bank Message": ["Foo"],
            "Amount": [3000.12],
            "Balance": [None],
            "naive_transaction_id": [
                (
                    '{"amount": "3.000,12", "balance": null, "bank_date": "2019/05/02", '
                    '"bank_message": "F000", "payee": "A255", "real_date": null}'
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
            "Raw Real Date": [None],
            "Raw Bank Date": ["2019/05/02"],
            "Raw Payee": ["Acme-Industries 123 Inc"],
            "Raw Bank Message": ["Foo"],
            "Raw Amount": ["3.000,12"],
            "Raw Balance": [None],
            "Real Date": [None],
            "Bank Date": [datetime.strptime("2019/05/02", "%Y/%m/%d")],
            "Payee": ["Acme-Industries 123 Inc"],
            "Bank Message": ["Foo"],
            "Amount": [3000.12],
            "Balance": [None],
            "ID": [
                (
                    '{"ref": {"amount": "3.000,12", "balance": null, "bank_date": "2019/05/02",'
                    ' "bank_message": "F000", "payee": "A255", "real_date": null}, "ord": 1}'
                )
            ],
        }
    )
    assert df.to_dict(orient="records") == expected.to_dict(orient="records")
