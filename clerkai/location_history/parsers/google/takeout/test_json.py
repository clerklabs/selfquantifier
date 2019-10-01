from os.path import dirname, join, realpath

from clerkai.location_history.parsers.google.takeout.json import \
    google_takeout_location_history_json_location_history_parser

test_data_dir_path = join(dirname(realpath(__file__)), "test_data")


def test_google_takeout_location_history_json_location_history_parser():
    # type: () -> None
    location_history_file_path = join(
        test_data_dir_path, "Location History.edited.json"
    )
    location_history_df = google_takeout_location_history_json_location_history_parser(
        location_history_file_path
    )
    assert not location_history_df.empty
    actual = location_history_df.to_csv(index=False)
    actual_file_path = "%s%s" % (location_history_file_path, ".actual.csv")
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (location_history_file_path, ".expected.csv")
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected
