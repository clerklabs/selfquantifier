import os

import geopy.distance
import pandas as pd
import reverse_geocoder as rg

from clerkai.utils import list_files_in_clerk_input_subfolder


def location_history_flow(
    location_history_files_editable_columns,
    location_history_by_date_editable_columns,
    clerkai_input_folder_path,
    possibly_edited_df,
    location_history_folder_path,
    acknowledge_changes_in_clerkai_input_folder,
    current_history_reference,
    keep_unmerged_previous_edits=False,
    failfast=False,
):
    def list_location_history_files_in_location_history_folder():
        _ = list_files_in_clerk_input_subfolder(
            location_history_folder_path,
            clerkai_input_folder_path=clerkai_input_folder_path,
        )
        if len(_) == 0:
            return _
        for column in location_history_files_editable_columns:
            _[column] = None
        _["History reference"] = current_history_reference()
        return _[
            [
                "File name",
                "File path",
                *location_history_files_editable_columns,
                "File metadata",
                "History reference",
            ]
        ]

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

    # Todo
    """
    def guess_content_type_based_on_filename():
        pass
    """
    # location_history_files_export_df[""]

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
    # so that edit-files are dependent on the editable columns of file metadata
    location_history_files_editable_data_df = included_location_history_files[
        location_history_files_editable_columns + ["File metadata"]
    ]
    save_location_history_files_editable_data_in_location_history_folder(
        location_history_folder_path, location_history_files_editable_data_df
    )
    acknowledge_changes_in_clerkai_input_folder()

    from clerkai.location_history.parse import parse_location_history_files

    parsed_location_history_files = parse_location_history_files(
        included_location_history_files, clerkai_input_folder_path, failfast
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
        have_timestamp_and_coordinates_mask = (
            ~all_parsed_location_history_df["Timestamp"].isnull()
            & ~all_parsed_location_history_df["Latitude"].isnull()
            & ~all_parsed_location_history_df["Longitude"].isnull()
        )
        location_history_df = all_parsed_location_history_df[
            have_timestamp_and_coordinates_mask
        ].sort_values(
            ["Timestamp"]
        )  # .drop_duplicates(subset=["ID"])

        # TODO: only in-memory location history, then editable day-by-day summary

        # reverse geo-coding
        location_history_df["coordinates"] = location_history_df.apply(
            lambda df: (df["Latitude"], df["Longitude"]), axis=1
        )
        unique_coordinates = list(set(list(location_history_df["coordinates"])))

        results = rg.search(unique_coordinates)  # default mode = 2
        reverse_geocoded_coordinates_df = pd.DataFrame(results)
        reverse_geocoded_coordinates_df["coordinates"] = unique_coordinates

        location_history_with_geonames_df = location_history_df.merge(
            reverse_geocoded_coordinates_df, on="coordinates"
        )

        location_history_with_geonames_df["date"] = location_history_with_geonames_df[
            "Timestamp"
        ].apply(lambda ts: ts.strftime("%Y-%m-%d"))
        location_history_with_geonames_df[
            "datehour"
        ] = location_history_with_geonames_df["Timestamp"].apply(
            lambda ts: ts.strftime("%Y-%m-%d %H")
        )
        groupby_date = location_history_with_geonames_df.groupby(["date"])
        groupby_datehour = location_history_with_geonames_df.groupby(["datehour"])

        def drop_duplicates_retain_order(a_list):
            return pd.Series(a_list).drop_duplicates().tolist()

        def geo_group_attributes(group):
            df = group.size().reset_index(name="counts")
            df["different_geo_names"] = (
                group["name"]
                .apply(drop_duplicates_retain_order)
                .reset_index(name="different_geo_names")["different_geo_names"]
            )
            df["different_geo_admin1"] = (
                group["admin1"]
                .apply(drop_duplicates_retain_order)
                .reset_index(name="different_geo_admin1")["different_geo_admin1"]
            )
            df["different_geo_admin2"] = (
                group["admin2"]
                .apply(drop_duplicates_retain_order)
                .reset_index(name="different_geo_admin2")["different_geo_admin2"]
            )
            df["different_countries"] = (
                group["cc"]
                .apply(drop_duplicates_retain_order)
                .reset_index(name="different_countries")["different_countries"]
            )
            df["coordinates"] = (
                group["coordinates"]
                .apply(drop_duplicates_retain_order)
                .reset_index(name="coordinates")["coordinates"]
            )
            df["last_known_coordinates"] = df["coordinates"].shift(1)
            df["last_known_different_geo_names"] = df["different_geo_names"].shift(1)
            df["last_known_different_geo_admin1"] = df["different_geo_admin1"].shift(1)
            df["last_known_different_geo_admin2"] = df["different_geo_admin2"].shift(1)
            df["last_known_different_countries"] = df["different_countries"].shift(1)
            return df

        location_history_by_date_df = geo_group_attributes(groupby_date)
        location_history_by_date_df["last_known_date"] = location_history_by_date_df[
            "date"
        ].shift(1)
        location_history_by_datehour_df = geo_group_attributes(groupby_datehour)
        location_history_by_datehour_df[
            "last_known_datehour"
        ] = location_history_by_datehour_df["datehour"].shift(1)

        def km_distance_since_last(row):
            # print(row["coordinates"], row["last_known_coordinates"])
            if row["last_known_coordinates"] != row["last_known_coordinates"]:
                return None
            return geopy.distance.distance(
                row["coordinates"][-1], row["last_known_coordinates"][-1]
            ).km

        location_history_by_date_df[
            "km_distance_since_last"
        ] = location_history_by_date_df.apply(km_distance_since_last, axis=1)
        location_history_by_datehour_df[
            "km_distance_since_last"
        ] = location_history_by_datehour_df.apply(km_distance_since_last, axis=1)

        # export location_history_by_date to xlsx
        record_type = "location_history_by_date"

        location_history_by_date_first_columns = [
            "last_known_date",
            "date",
            "km_distance_since_last",
            *location_history_by_date_editable_columns,
            "last_known_different_geo_names",
            "last_known_different_geo_admin1",
            "last_known_different_geo_admin2",
            "last_known_different_countries",
            "different_geo_names",
            "different_geo_admin1",
            "different_geo_admin2",
            "different_countries",
        ]
        location_history_by_date_export_columns = [
            *location_history_by_date_first_columns,
            *location_history_by_date_df.columns.difference(
                location_history_by_date_first_columns, sort=False
            ),
        ]
        # print("location_history_by_date_export_columns", location_history_by_date_export_columns)
        location_history_by_date_export_df = location_history_by_date_df.reindex(
            location_history_by_date_export_columns, axis=1
        )

        # index by date
        dates = pd.to_datetime(location_history_by_date_export_df["date"])
        location_history_by_date_export_df = location_history_by_date_export_df.set_index(
            dates
        )

        # include all dates, so that the edit file can be used to fill in locations for missing dates as well
        all_days = pd.date_range(
            location_history_by_date_export_df.index.min(),
            location_history_by_date_export_df.index.max(),
            freq="D",
        )
        location_history_by_date_export_df = location_history_by_date_export_df.reindex(
            all_days
        )
        location_history_by_date_export_df["date"] = all_days

        possibly_edited_location_history_by_date_df = possibly_edited_df(
            location_history_by_date_export_df,
            record_type,
            location_history_by_date_editable_columns,
            keep_unmerged_previous_edits=False,
        )

        # index by date again after reading from edit file
        dates = pd.to_datetime(possibly_edited_location_history_by_date_df["date"])
        possibly_edited_location_history_by_date_df = possibly_edited_location_history_by_date_df.set_index(
            dates
        )

    else:
        all_parsed_location_history_df = []
        location_history_df = []
        location_history_with_geonames_df = []
        possibly_edited_location_history_by_date_df = []

    return (
        location_history_files_df,
        possibly_edited_location_history_files_df,
        unsuccessfully_parsed_location_history_files,
        successfully_parsed_location_history_files,
        all_parsed_location_history_df,
        location_history_df,
        location_history_with_geonames_df,
        possibly_edited_location_history_by_date_df,
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
