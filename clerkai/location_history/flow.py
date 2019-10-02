import os

import pandas as pd


def location_history_flow(
    location_history_files_editable_columns,
    location_history_editable_columns,
    list_location_history_files_in_location_history_folder,
    possibly_edited_df,
    location_history_folder_path,
    acknowledge_changes_in_clerkai_input_folder,
    clerkai_input_file_path,
    current_history_reference,
    keep_unmerged_previous_edits=False,
    failfast=False,
):
    location_history_files_df = list_location_history_files_in_location_history_folder()
    record_type = "location_history_files"
    location_history_files_first_columns = [
        "File name",
        "File path",
        *location_history_files_editable_columns,
    ]
    location_history_files_export_columns = [
        *location_history_files_first_columns,
        *location_history_files_df.columns.difference(
            location_history_files_first_columns
        ),
    ]
    # print("location_history_files_export_columns", location_history_files_export_columns)
    location_history_files_export_df = location_history_files_df.reindex(
        location_history_files_export_columns, axis=1
    )
    possibly_edited_location_history_files_df = possibly_edited_df(
        location_history_files_export_df,
        record_type,
        location_history_files_editable_columns,
        keep_unmerged_previous_edits,
    )

    included_location_history_files = possibly_edited_location_history_files_df[
        possibly_edited_location_history_files_df["Ignore"] != 1
    ]

    # make sure that the edited column values yields new commits
    # so that edit-files are dependent on the editable values
    location_history_files_editable_data_df = included_location_history_files[
        location_history_files_editable_columns + ["File metadata"]
    ]
    save_location_history_files_editable_data_in_location_history_folder(
        location_history_folder_path, location_history_files_editable_data_df
    )
    acknowledge_changes_in_clerkai_input_folder()

    from clerkai.location_history.parse import parse_location_history_files

    parsed_location_history_files = parse_location_history_files(
        included_location_history_files, clerkai_input_file_path, failfast
    )

    unsuccessfully_parsed_location_history_files = parsed_location_history_files[
        ~parsed_location_history_files["Error"].isnull()
    ].drop(["File metadata", "Parse results"], axis=1)

    successfully_parsed_location_history_files = parsed_location_history_files[
        parsed_location_history_files["Error"].isnull()
    ].drop(["Error"], axis=1)

    if len(successfully_parsed_location_history_files) > 0:
        # concat all location_history
        all_parsed_location_history_df = pd.concat(
            successfully_parsed_location_history_files["Parse results"].values,
            sort=False,
        ).reset_index(drop=True)
        all_parsed_location_history_df[
            "History reference"
        ] = current_history_reference()
        # include location_history_files data
        all_parsed_location_history_df = pd.merge(
            all_parsed_location_history_df,
            included_location_history_files.drop(["Ignore"], axis=1).add_prefix(
                "Source location history file: "
            ),
            left_on="Source location history file index",
            right_index=True,
            suffixes=(False, False),
        )
        # print("all_parsed_location_history_df.columns", all_parsed_location_history_df.columns)

        location_history_df = (
            all_parsed_location_history_df
        )  # .drop_duplicates(subset=["ID"])

        # export all location_history to xlsx
        record_type = "location_history"

        location_history_first_columns = [
            "Timestamp",
            *location_history_editable_columns,
        ]
        location_history_export_columns = [
            *location_history_first_columns,
            *location_history_df.columns.difference(
                location_history_first_columns, sort=False
            ),
        ]
        # print("location_history_export_columns", location_history_export_columns)
        location_history_export_df = location_history_df.reindex(
            location_history_export_columns, axis=1
        )

        possibly_edited_location_history_df = possibly_edited_df(
            location_history_export_df,
            record_type,
            location_history_editable_columns,
            keep_unmerged_previous_edits=False,
        )

    else:
        all_parsed_location_history_df = []
        location_history_df = []
        possibly_edited_location_history_df = []

    return (
        location_history_files_df,
        possibly_edited_location_history_files_df,
        unsuccessfully_parsed_location_history_files,
        successfully_parsed_location_history_files,
        all_parsed_location_history_df,
        location_history_df,
        possibly_edited_location_history_df,
    )


def save_location_history_files_editable_data_in_location_history_folder(
    location_history_folder_path, location_history_files_editable_data_df
):
    csv = location_history_files_editable_data_df.to_csv(index=False)
    file_path = os.path.join(
        location_history_folder_path, "location_history_files_editable_data.csv"
    )
    with open(file_path, "w") as f:
        f.write(csv)
