from os.path import dirname, join, realpath

from clerkai.location_history.defaults import (
    location_history_by_date_editable_columns,
    location_history_files_editable_columns)
from clerkai.location_history.flow import location_history_flow

test_data_dir_path = join(dirname(realpath(__file__)), "test_data")


def test_location_history_flow():
    def acknowledge_changes_in_clerkai_input_folder():
        pass

    def current_history_reference():
        return "foo"

    def possibly_edited_df(
        current_commit_df,
        record_type,
        editable_columns,
        keep_unmerged_previous_edits=False,
    ):
        if record_type == "location_history_files":
            current_commit_df.loc[
                current_commit_df["File name"] == "Location History.edited.json",
                "Content type",
            ] = "exported-location-history-file/google-takeout.location-history.json"
            current_commit_df.loc[
                current_commit_df["File name"] == "dropbox-camera-uploads.edited.csv",
                "Content type",
            ] = "exported-location-history-file/exiftool-output.csv"
        return current_commit_df

    clerkai_input_folder_path = join(test_data_dir_path, "Input")
    location_history_folder_path = join(clerkai_input_folder_path, "Location History")

    (
        location_history_files_df,
        possibly_edited_location_history_files_df,
        unsuccessfully_parsed_location_history_files,
        successfully_parsed_location_history_files,
        all_parsed_location_history_df,
        location_history_df,
        location_history_with_geonames_df,
        possibly_edited_location_history_by_date_df,
    ) = location_history_flow(
        location_history_files_editable_columns=location_history_files_editable_columns,
        location_history_by_date_editable_columns=location_history_by_date_editable_columns,
        clerkai_input_folder_path=clerkai_input_folder_path,
        possibly_edited_df=possibly_edited_df,
        location_history_folder_path=location_history_folder_path,
        acknowledge_changes_in_clerkai_input_folder=acknowledge_changes_in_clerkai_input_folder,
        current_history_reference=current_history_reference,
        keep_unmerged_previous_edits=False,
        failfast=True,
    )

    assert len(unsuccessfully_parsed_location_history_files) == 0
    assert len(possibly_edited_location_history_by_date_df) > 0
    actual = possibly_edited_location_history_by_date_df.to_csv(index=False)
    possibly_edited_location_history_by_date_df_csv_path = join(
        test_data_dir_path, "possibly_edited_location_history_by_date_df"
    )
    actual_file_path = "%s%s" % (
        possibly_edited_location_history_by_date_df_csv_path,
        ".actual.csv",
    )
    with open(actual_file_path, "w") as f:
        f.write(actual)
    expected_file_path = "%s%s" % (
        possibly_edited_location_history_by_date_df_csv_path,
        ".expected.csv",
    )
    with open(expected_file_path, "r") as f:
        expected = f.read()
    assert actual == expected
