import os

import pandas as pd

from selfquantifier.utils import (add_date_columns_for_pivoting,
                           list_files_in_clerk_input_subfolder)


def time_tracking_flow(
    time_tracking_files_editable_columns,
    time_tracking_entries_editable_columns,
    selfquantifier_input_folder_path,
    possibly_edited_df,
    time_tracking_folder_path,
    acknowledge_changes_in_selfquantifier_input_folder,
    current_history_reference,
    keep_unmerged_previous_edits=False,
    failfast=False,
):
    time_tracking_files_calculated_columns = [
        "Parse status",
        # "Include in reports?",
        "Link",
        "Hours",
        "Sessions",
        "Processed lines",
        "Last modified",
        "Oldest timestamp",
        "Most recent timestamp",
        "Timelog comment",
    ]

    def list_time_tracking_files_in_time_tracking_folder():
        _ = list_files_in_clerk_input_subfolder(
            time_tracking_folder_path,
            selfquantifier_input_folder_path=selfquantifier_input_folder_path,
        )
        if len(_) == 0:
            return _
        for column in time_tracking_files_editable_columns:
            _[column] = None
        for column in time_tracking_files_calculated_columns:
            _[column] = None
        _["History reference"] = current_history_reference()
        return _[
            [
                "File name",
                "File path",
                *time_tracking_files_editable_columns,
                *time_tracking_files_calculated_columns,
                "File metadata",
                "History reference",
            ]
        ]

    time_tracking_files_df = list_time_tracking_files_in_time_tracking_folder()
    record_type = "time_tracking_files"
    time_tracking_files_first_columns = [
        "File name",
        "File path",
        *time_tracking_files_editable_columns,
        *time_tracking_files_calculated_columns,
    ]
    time_tracking_files_export_columns = [
        *time_tracking_files_first_columns,
        *time_tracking_files_df.columns.difference(time_tracking_files_first_columns),
    ]
    # print("time_tracking_files_export_columns", time_tracking_files_export_columns)
    time_tracking_files_export_df = time_tracking_files_df.reindex(
        time_tracking_files_export_columns, axis=1
    )

    def set_guessed_content_types(df):
        mask1 = df["Content type"].isnull()
        mask2 = df["File path"].str.contains("/ts")
        mask3 = df["File name"].str.contains(".md")
        df.loc[
            mask1 & mask2 & mask3, "Content type",
        ] = "exported-time-tracking-file/neamtime-tslog"
        return df

    time_tracking_files_export_df = set_guessed_content_types(
        time_tracking_files_export_df
    )

    possibly_edited_time_tracking_files_df = possibly_edited_df(
        time_tracking_files_export_df,
        record_type,
        time_tracking_files_editable_columns,
        keep_unmerged_previous_edits,
    )

    included_time_tracking_files = possibly_edited_time_tracking_files_df[
        (
            possibly_edited_time_tracking_files_df["Ignore"].isnull()
            | (possibly_edited_time_tracking_files_df["Ignore"] == 0)
            | (possibly_edited_time_tracking_files_df["Ignore"] == "0")
        )
    ]

    # make sure that the edited column values yields new commits
    # so that edit-files are dependent on the editable columns of file metadata
    time_tracking_files_editable_data_df = included_time_tracking_files[
        time_tracking_files_editable_columns + ["File metadata"]
    ]
    save_time_tracking_files_editable_data_in_time_tracking_folder(
        time_tracking_folder_path, time_tracking_files_editable_data_df
    )
    acknowledge_changes_in_selfquantifier_input_folder()

    from selfquantifier.time_tracking.parse import parse_time_tracking_files

    parsed_time_tracking_files = parse_time_tracking_files(
        time_tracking_files=included_time_tracking_files,
        selfquantifier_input_folder_path=selfquantifier_input_folder_path,
        failfast=failfast,
    )

    # concat all processing errors into a single dataframe
    if len(parsed_time_tracking_files) > 0:
        all_time_tracking_processing_errors_df = pd.concat(
            parsed_time_tracking_files["Processing errors"].values, sort=False,
        ).reset_index(drop=True)
        all_time_tracking_processing_errors_df[
            "History reference"
        ] = current_history_reference()
        # include time_tracking_files data
        all_time_tracking_processing_errors_df = pd.merge(
            all_time_tracking_processing_errors_df,
            included_time_tracking_files.drop(["Ignore"], axis=1).add_prefix(
                "Source time tracking file: "
            ),
            left_on="Source time tracking file index",
            right_index=True,
            suffixes=(False, False),
        )

        time_tracking_processing_errors_first_columns = [
            "Source time tracking file: File name",
            "Source time tracking file: File path",
        ]
        time_tracking_processing_errors_export_columns = [
            *time_tracking_processing_errors_first_columns,
            *all_time_tracking_processing_errors_df.columns.difference(
                time_tracking_processing_errors_first_columns, sort=False
            ),
            "Row number at export",
        ]
        all_time_tracking_processing_errors_df = all_time_tracking_processing_errors_df.reindex(
            time_tracking_processing_errors_export_columns, axis=1
        )
    else:
        all_time_tracking_processing_errors_df = []

    unsuccessfully_parsed_time_tracking_files = parsed_time_tracking_files[
        ~parsed_time_tracking_files["Error"].isnull()
    ].drop(
        [
            "File metadata",
            "Parsed time tracking entries",
            "Parsing metadata",
            "Processing errors",
        ],
        axis=1,
    )

    successfully_parsed_time_tracking_files = parsed_time_tracking_files[
        parsed_time_tracking_files["Error"].isnull()
    ].drop(["Error"], axis=1)

    if len(successfully_parsed_time_tracking_files) > 0:
        # concat all time_tracking_entries
        all_parsed_time_tracking_entries_df = pd.concat(
            successfully_parsed_time_tracking_files[
                "Parsed time tracking entries"
            ].values,
            sort=False,
        ).reset_index(drop=True)
        all_parsed_time_tracking_entries_df[
            "History reference"
        ] = current_history_reference()
        # include time_tracking_files data
        all_parsed_time_tracking_entries_df = pd.merge(
            all_parsed_time_tracking_entries_df,
            included_time_tracking_files.drop(["Ignore"], axis=1).add_prefix(
                "Source time tracking file: "
            ),
            left_on="Source time tracking file index",
            right_index=True,
            suffixes=(False, False),
        )

        # print("all_parsed_time_tracking_entries_df.columns", all_parsed_time_tracking_entries_df.columns)

        time_tracking_entry_duplicates = all_parsed_time_tracking_entries_df.groupby(
            "ID"
        ).filter(lambda x: len(x) > 1)
        print(
            "time_tracking_entry_duplicates: ",
            time_tracking_entry_duplicates[
                [
                    "Source time tracking file: File name",
                    "Source time tracking file: File path",
                    "Session",
                    "Source Lines Summary",
                    "UTC Timestamp",
                    "Hours",
                    "ID",
                ]
            ],
        )

        time_tracking_entries_df = all_parsed_time_tracking_entries_df.drop_duplicates(
            subset=["ID"]
        )

        # ensure that empty tag values is filled with source file default tag if available
        """
        time_tracking_entries_df_where_currency_column_is_null_mask = time_tracking_entries_df[
            "Tags"
        ].isnull()
        time_tracking_entries_df_where_source_transaction_file_account_currency_column_is_null_mask = \
            time_tracking_entries_df[
            "Source transaction file: Default tags"
        ].isnull()
        time_tracking_entries_df.loc[
            time_tracking_entries_df_where_currency_column_is_null_mask, "Tags",
        ] = time_tracking_entries_df.loc[
            ~time_tracking_entries_df_where_source_transaction_file_account_currency_column_is_null_mask,
            "Source transaction file: Default tags",
        ]
        """

        # add columns that are useful for aggregation / pivoting
        add_date_columns_for_pivoting(time_tracking_entries_df, "Work Date")

        # export all time_tracking_entries to xlsx
        record_type = "time_tracking_entries"

        time_tracking_entries_first_columns = [
            "Source time tracking file: File name",
            "Source time tracking file: File path",
            *time_tracking_entries_editable_columns,
        ]
        time_tracking_entries_export_columns = [
            *time_tracking_entries_first_columns,
            *time_tracking_entries_df.columns.difference(
                time_tracking_entries_first_columns, sort=False
            ),
            "Row number at export",
        ]
        # print("time_tracking_entries_export_columns", time_tracking_entries_export_columns)
        time_tracking_entries_export_df = time_tracking_entries_df.reindex(
            time_tracking_entries_export_columns, axis=1
        )

        # convert Decimal columns to float prior to export or excel will treat them as strings
        # todo: less hacky conversion of Decimal-columns
        from selfquantifier.utils import is_nan

        def float_if_not_nan(number):
            if is_nan(number) or number is None:
                return None
            return float(number)

        # time_tracking_entries_export_df[
        #     "Foo"
        # ] = time_tracking_entries_export_df["Foo"].apply(float_if_not_nan)

        possibly_edited_time_tracking_entries_df = possibly_edited_df(
            time_tracking_entries_export_df,
            record_type,
            time_tracking_entries_editable_columns,
            keep_unmerged_previous_edits=keep_unmerged_previous_edits,
        )

    else:
        all_parsed_time_tracking_entries_df = []
        all_time_tracking_processing_errors_df = []
        time_tracking_entries_df = []
        possibly_edited_time_tracking_entries_df = []

    return (
        time_tracking_files_df,
        possibly_edited_time_tracking_files_df,
        unsuccessfully_parsed_time_tracking_files,
        all_time_tracking_processing_errors_df,
        successfully_parsed_time_tracking_files,
        all_parsed_time_tracking_entries_df,
        time_tracking_entries_df,
        possibly_edited_time_tracking_entries_df,
        time_tracking_files_editable_columns,
        time_tracking_entries_editable_columns,
    )


def save_time_tracking_files_editable_data_in_time_tracking_folder(
    time_tracking_folder_path, time_tracking_files_editable_data_df
):
    csv = time_tracking_files_editable_data_df.to_csv(index=False)
    file_path = os.path.join(
        time_tracking_folder_path, "time_tracking_files_editable_data.csv"
    )
    with open(file_path, "w") as f:
        f.write(csv)
