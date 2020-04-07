import json

import pandas as pd
from pandas import json_normalize
from pandas.core.frame import DataFrame

from clerkai.location_history.parsers.parse_utils import \
    timestamp_ms_to_utc_datetime_obj


def import_google_takeout_location_history_json_location_history_file(
    location_history_file,
):
    with open(location_history_file, "r") as f:
        data = json.load(f)
    locations_df = json_normalize(data["locations"])
    [
        elem.update({"activity": []})
        for elem in data["locations"]
        if "activity" not in elem.keys()
    ]
    [
        elem.update({"heading": None})
        for elem in data["locations"]
        if "heading" not in elem.keys()
    ]
    [
        elem.update({"velocity": None})
        for elem in data["locations"]
        if "velocity" not in elem.keys()
    ]
    activity_df = json_normalize(
        data=data["locations"],
        record_path=["activity", "activity"],
        meta=["timestampMs"],
    )
    return (locations_df, activity_df)


def google_takeout_location_history_json_location_history_to_general_clerk_format(
    locations_df,
):
    # type: (DataFrame) -> DataFrame
    normalized_df = pd.DataFrame()
    normalized_df["Raw Timestamp"] = locations_df["timestampMs"]
    normalized_df["Raw Latitude"] = locations_df["latitudeE7"]
    normalized_df["Raw Longitude"] = locations_df["longitudeE7"]
    normalized_df["Raw Accuracy"] = locations_df["accuracy"]
    normalized_df["Raw Velocity"] = locations_df["velocity"]
    normalized_df["Raw Heading"] = locations_df["heading"]
    normalized_df["Raw Altitude"] = locations_df["altitude"]
    normalized_df["Raw Vertical Accuracy"] = locations_df["verticalAccuracy"]
    normalized_df["Timestamp"] = normalized_df["Raw Timestamp"].apply(
        timestamp_ms_to_utc_datetime_obj
    )
    normalized_df["Latitude"] = normalized_df["Raw Latitude"] / 1e7
    normalized_df["Longitude"] = normalized_df["Raw Longitude"] / 1e7
    normalized_df["Accuracy"] = normalized_df["Raw Accuracy"]
    normalized_df["Velocity"] = normalized_df["Raw Velocity"]
    normalized_df["Heading"] = normalized_df["Raw Heading"]
    normalized_df["Altitude"] = normalized_df["Raw Altitude"]
    normalized_df["Vertical Accuracy"] = normalized_df["Raw Vertical Accuracy"]
    normalized_df["Original data"] = locations_df[
        [
            "timestampMs",
            "latitudeE7",
            "longitudeE7",
            "accuracy",
            "activity",
            "velocity",
            "heading",
            "altitude",
            "verticalAccuracy",
        ]
    ].to_dict(orient="records")
    return normalized_df[
        [
            "Timestamp",
            "Latitude",
            "Longitude",
            "Accuracy",
            "Velocity",
            "Heading",
            "Altitude",
            "Vertical Accuracy",
            "Original data",
            "Raw Timestamp",
            "Raw Latitude",
            "Raw Longitude",
            "Raw Accuracy",
            "Raw Velocity",
            "Raw Heading",
            "Raw Altitude",
            "Raw Vertical Accuracy",
        ]
    ]


def google_takeout_location_history_json_location_history_parser(location_history_file):
    # type: (str) -> DataFrame
    (
        locations_df,
        activity_df,
    ) = import_google_takeout_location_history_json_location_history_file(
        location_history_file
    )
    return google_takeout_location_history_json_location_history_to_general_clerk_format(
        locations_df
    )
