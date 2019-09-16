import pandas as pd
from pandas.util.testing import assert_frame_equal

from clerkai.transactions.parse import (add_naive_transaction_id,
                                        add_naive_transaction_id_duplicate_num)

transactions_df = pd.DataFrame(
    {
        "Raw Date Initiated": [""],
        "Raw Date Settled": ["2019/05/02"],
        "Raw Payee": ["Acme-Industries 123 Inc"],
        "Raw Memo": ["Foo"],
        "Raw Amount": ["3.000,12"],
        "Raw Balance": [None],
    }
)


def test_add_naive_transaction_id():
    df2 = add_naive_transaction_id(transactions_df)
    assert not assert_frame_equal(transactions_df, df2)
    expected = pd.DataFrame(
        {
            "Raw Date Initiated": [""],
            "Raw Date Settled": ["2019/05/02"],
            "Raw Payee": ["Acme-Industries 123 Inc"],
            "Raw Memo": ["Foo"],
            "Raw Amount": ["3.000,12"],
            "Raw Balance": [None],
            "naive_transaction_id": ["foo"],
        }
    )
    assert df2.to_dict(orient="records") == expected.to_dict(orient="records")


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


def test_add_duplicate_naive_transaction_id_num():
    df2 = add_naive_transaction_id(transactions_df)
    assert not assert_frame_equal(transactions_df, df2)
    df3 = add_naive_transaction_id_duplicate_num(df2)
    assert not assert_frame_equal(df2, df3)
    expected = pd.DataFrame(
        {
            "Raw Date Initiated": [""],
            "Raw Date Settled": ["2019/05/02"],
            "Raw Payee": ["Acme-Industries 123 Inc"],
            "Raw Memo": ["Foo"],
            "Raw Amount": ["3.000,12"],
            "Raw Balance": [None],
            "naive_transaction_id": ["foo"],
            "naive_transaction_id_duplicate_num": [1],
        }
    )
    assert df3.to_dict(orient="records") == expected.to_dict(orient="records")
