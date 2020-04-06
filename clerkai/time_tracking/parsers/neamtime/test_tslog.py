from os.path import dirname, join, realpath

from clerkai.time_tracking.parsers.neamtime.tslog import (
    neamtime_datetime_to_naive_datetime_obj,
    neamtime_tslog_time_tracking_entries_parser)

test_data_dir_path = join(dirname(realpath(__file__)), "test_data")


def test_neamtime_datetime_to_naive_datetime_obj():
    assert (
        neamtime_datetime_to_naive_datetime_obj("2017-08-26 22:09").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        == "2017-08-26 22:09:00"
    )
    assert neamtime_datetime_to_naive_datetime_obj(float("NaN")) is None
    assert neamtime_datetime_to_naive_datetime_obj(None) is None


def test_neamtime_tslog_time_tracking_entries_parser():
    # type: () -> None
    time_tracking_file_path = join(test_data_dir_path, "an-hour-of-something.tslog")
    (
        time_tracking_entries_df,
        parsing_metadata,
        processing_errors,
    ) = neamtime_tslog_time_tracking_entries_parser(time_tracking_file_path)
    assert not time_tracking_entries_df.empty
    actual = time_tracking_entries_df.to_csv(index=False)
    actual_file_path = "%s%s" % (time_tracking_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (time_tracking_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_neamtime_tslog_time_tracking_entries_parser_2():
    # type: () -> None
    time_tracking_file_path = join(
        test_data_dir_path,
        "example-1-from-neamtime-reporting-2010-docs.with-paus-typo.tslog",
    )
    (
        time_tracking_entries_df,
        parsing_metadata,
        processing_errors,
    ) = neamtime_tslog_time_tracking_entries_parser(time_tracking_file_path)
    assert not time_tracking_entries_df.empty
    actual = time_tracking_entries_df.to_csv(index=False)
    actual_file_path = "%s%s" % (time_tracking_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (time_tracking_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_neamtime_tslog_time_tracking_entries_parser_3():
    # type: () -> None
    time_tracking_file_path = join(test_data_dir_path, "newly-cycled.tslog")
    (
        time_tracking_entries_df,
        parsing_metadata,
        processing_errors,
    ) = neamtime_tslog_time_tracking_entries_parser(time_tracking_file_path)
    assert time_tracking_entries_df.empty
    actual = time_tracking_entries_df.to_csv(index=False)
    actual_file_path = "%s%s" % (time_tracking_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (time_tracking_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_neamtime_tslog_time_tracking_entries_parser_4():
    # type: () -> None
    time_tracking_file_path = join(test_data_dir_path, "pause-handling.tslog")
    (
        time_tracking_entries_df,
        parsing_metadata,
        processing_errors,
    ) = neamtime_tslog_time_tracking_entries_parser(time_tracking_file_path)
    assert not time_tracking_entries_df.empty
    actual = time_tracking_entries_df.to_csv(index=False)
    actual_file_path = "%s%s" % (time_tracking_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (time_tracking_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_neamtime_tslog_time_tracking_entries_parser_5():
    # type: () -> None
    time_tracking_file_path = join(
        test_data_dir_path, "multiple-entries-with-the-same-timestamp.tslog"
    )
    (
        time_tracking_entries_df,
        parsing_metadata,
        processing_errors,
    ) = neamtime_tslog_time_tracking_entries_parser(time_tracking_file_path)
    assert not time_tracking_entries_df.empty
    actual = time_tracking_entries_df.to_csv(index=False)
    actual_file_path = "%s%s" % (time_tracking_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (time_tracking_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected


def test_neamtime_tslog_time_tracking_entries_parser_6():
    # type: () -> None
    time_tracking_file_path = join(
        test_data_dir_path, "numbers-solo-should-not-be-interpreted-as-timestamps.tslog"
    )
    (
        time_tracking_entries_df,
        parsing_metadata,
        processing_errors,
    ) = neamtime_tslog_time_tracking_entries_parser(time_tracking_file_path)
    assert not time_tracking_entries_df.empty
    actual = time_tracking_entries_df.to_csv(index=False)
    actual_file_path = "%s%s" % (time_tracking_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (time_tracking_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected
