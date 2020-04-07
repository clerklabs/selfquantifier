import numpy as np
import pandas as pd

from clerkai.time_tracking.parse import (
    naive_time_tracking_entry_id_duplicate_nums, naive_time_tracking_entry_ids,
    time_tracking_entry_ids)

time_tracking_entries_df = pd.DataFrame(
    {
        "Raw UTC Timestamp": ["2020-04-05 09:37"],
        "Raw Source Lines Summary": ["foo | bar | 123 | zoo"],
        "Raw Session": ["start 2020-04-05 (+0300) 09:00"],
        "UTC Timestamp": ["2020-04-05 09:37"],
        "Source Lines Summary": ["foo | bar | 123 | zoo"],
        "Session": ["start 2020-04-05 (+0300) 09:00"],
    }
)

time_tracking_entries_with_none_df = pd.DataFrame(
    {
        "Raw UTC Timestamp": [None],
        "Raw Source Lines Summary": ["foo | bar | 123 | zoo"],
        "Raw Session": ["start 2020-04-05 (+0300) 09:00"],
        "UTC Timestamp": [None],
        "Source Lines Summary": ["foo | bar | 123 | zoo"],
        "Session": ["start 2020-04-05 (+0300) 09:00"],
    }
)

time_tracking_entries_with_nan_df = pd.DataFrame(
    {
        "Raw UTC Timestamp": [float("NaN")],
        "Raw Source Lines Summary": ["foo | bar | 123 | zoo"],
        "Raw Session": ["start 2020-04-05 (+0300) 09:00"],
        "UTC Timestamp": [float("NaN")],
        "Source Lines Summary": ["foo | bar | 123 | zoo"],
        "Session": ["start 2020-04-05 (+0300) 09:00"],
    }
)

time_tracking_entries_without_raw_columns_df = pd.DataFrame(
    {
        "UTC Timestamp": [None],
        "Source Lines Summary": ["foo | bar | 123 | zoo"],
        "Session": ["start 2020-04-05 (+0300) 09:00"],
    }
)

time_tracking_entries_without_raw_columns_and_some_core_columns_df = pd.DataFrame(
    {
        "Source Lines Summary": ["foo | bar | 123 | zoo"],
        "Session": ["start 2020-04-05 (+0300) 09:00"],
    }
)


def test_naive_time_tracking_entry_ids():
    df = time_tracking_entries_df.copy()
    df["naive_time_tracking_entry_id"] = naive_time_tracking_entry_ids(df)
    expected = pd.DataFrame(
        {
            "Raw UTC Timestamp": ["2020-04-05 09:37"],
            "Raw Source Lines Summary": ["foo | bar | 123 | zoo"],
            "Raw Session": ["start 2020-04-05 (+0300) 09:00"],
            "UTC Timestamp": ["2020-04-05 09:37"],
            "Source Lines Summary": ["foo | bar | 123 | zoo"],
            "Session": ["start 2020-04-05 (+0300) 09:00"],
            "naive_time_tracking_entry_id": [
                '{"session": "start 2020-04-05 (+0300) '
                '09:00", "source_lines_summary": "F162", '
                '"utc_timestamp": "2020-04-05 09:37"}',
            ],
        }
    )
    assert df.to_dict(orient="records") == expected.to_dict(orient="records")


def test_naive_time_tracking_entry_ids_with_none():
    df = time_tracking_entries_with_none_df.copy()
    df["naive_time_tracking_entry_id"] = naive_time_tracking_entry_ids(df)
    expected = pd.DataFrame(
        {
            "Raw UTC Timestamp": [None],
            "Raw Source Lines Summary": ["foo | bar | 123 | zoo"],
            "Raw Session": ["start 2020-04-05 (+0300) 09:00"],
            "UTC Timestamp": [None],
            "Source Lines Summary": ["foo | bar | 123 | zoo"],
            "Session": ["start 2020-04-05 (+0300) 09:00"],
            "naive_time_tracking_entry_id": [
                '{"session": "start 2020-04-05 (+0300) 09:00", "source_lines_summary": "F162"'
                ', "utc_timestamp": null}'
            ],
        }
    )
    assert df.replace({np.nan: None}).to_dict(orient="records") == expected.replace(
        {np.nan: None}
    ).to_dict(orient="records")


def test_naive_time_tracking_entry_ids_with_nan():
    df = time_tracking_entries_with_nan_df.copy()
    df["naive_time_tracking_entry_id"] = naive_time_tracking_entry_ids(df)
    expected = pd.DataFrame(
        {
            "Raw UTC Timestamp": [float("NaN")],
            "Raw Source Lines Summary": ["foo | bar | 123 | zoo"],
            "Raw Session": ["start 2020-04-05 (+0300) 09:00"],
            "UTC Timestamp": [float("NaN")],
            "Source Lines Summary": ["foo | bar | 123 | zoo"],
            "Session": ["start 2020-04-05 (+0300) 09:00"],
            "naive_time_tracking_entry_id": [
                '{"session": "start 2020-04-05 (+0300) 09:00", "source_lines_summary": "F162"'
                ', "utc_timestamp": null}'
            ],
        }
    )
    assert df.replace({np.nan: None}).to_dict(orient="records") == expected.replace(
        {np.nan: None}
    ).to_dict(orient="records")


def test_naive_time_tracking_entry_ids_without_raw_columns():
    df = time_tracking_entries_without_raw_columns_df.copy()
    df["naive_time_tracking_entry_id"] = naive_time_tracking_entry_ids(df)
    expected = pd.DataFrame(
        {
            "UTC Timestamp": [None],
            "Source Lines Summary": ["foo | bar | 123 | zoo"],
            "Session": ["start 2020-04-05 (+0300) 09:00"],
            "naive_time_tracking_entry_id": [
                '{"session": "start 2020-04-05 (+0300) 09:00", "source_lines_summary": "F162"'
                ', "utc_timestamp": null}'
            ],
        }
    )
    assert df.to_dict(orient="records") == expected.to_dict(orient="records")


def test_naive_time_tracking_entry_ids_without_raw_columns_and_some_core_columns():
    df = time_tracking_entries_without_raw_columns_and_some_core_columns_df.copy()
    df["naive_time_tracking_entry_id"] = naive_time_tracking_entry_ids(df)
    expected = pd.DataFrame(
        {
            "Source Lines Summary": ["foo | bar | 123 | zoo"],
            "Session": ["start 2020-04-05 (+0300) 09:00"],
            "naive_time_tracking_entry_id": [
                '{"session": "start 2020-04-05 (+0300) 09:00", "source_lines_summary": "F162"'
                ', "utc_timestamp": null}'
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


def test_naive_time_tracking_entry_id_duplicate_nums():
    df = time_tracking_entries_df.copy()
    df["naive_time_tracking_entry_id"] = naive_time_tracking_entry_ids(df)
    df2 = df.copy()
    df2[
        "naive_time_tracking_entry_id_duplicate_num"
    ] = naive_time_tracking_entry_id_duplicate_nums(df2)
    expected = pd.DataFrame(
        {
            "Raw UTC Timestamp": ["2020-04-05 09:37"],
            "Raw Source Lines Summary": ["foo | bar | 123 | zoo"],
            "Raw Session": ["start 2020-04-05 (+0300) 09:00"],
            "UTC Timestamp": ["2020-04-05 09:37"],
            "Source Lines Summary": ["foo | bar | 123 | zoo"],
            "Session": ["start 2020-04-05 (+0300) 09:00"],
            "naive_time_tracking_entry_id": [
                '{"session": "start 2020-04-05 (+0300) '
                '09:00", "source_lines_summary": "F162", '
                '"utc_timestamp": "2020-04-05 09:37"}',
            ],
            "naive_time_tracking_entry_id_duplicate_num": [1],
        }
    )
    assert df2.to_dict(orient="records") == expected.to_dict(orient="records")


def test_time_tracking_entry_ids():
    df = time_tracking_entries_df.copy()
    df["ID"] = time_tracking_entry_ids(df)
    expected = pd.DataFrame(
        {
            "Raw UTC Timestamp": ["2020-04-05 09:37"],
            "Raw Source Lines Summary": ["foo | bar | 123 | zoo"],
            "Raw Session": ["start 2020-04-05 (+0300) 09:00"],
            "UTC Timestamp": ["2020-04-05 09:37"],
            "Source Lines Summary": ["foo | bar | 123 | zoo"],
            "Session": ["start 2020-04-05 (+0300) 09:00"],
            "ID": [
                '{"ref": {"session": "start 2020-04-05 (+0300) 09:00", "source_lines_summary": "F162"'
                ', "utc_timestamp": "2020-04-05 09:37"}, "ord": 1}'
            ],
        }
    )
    assert df.to_dict(orient="records") == expected.to_dict(orient="records")
