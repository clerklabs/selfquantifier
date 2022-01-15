import pandas as pd
from pandas.core.frame import DataFrame

from clerkai.location_history.parsers.parse_utils import \
    exiftool_date_to_utc_datetime_obj
from clerkai.utils import is_nan


def either_date_time_original_or_create_date(raw_dates):
    exiftool_date = None
    if raw_dates["DateTimeOriginal"] and not is_nan(raw_dates["DateTimeOriginal"]):
        exiftool_date = raw_dates["DateTimeOriginal"]
    elif raw_dates["CreateDate"] and not is_nan(raw_dates["CreateDate"]):
        exiftool_date = raw_dates["CreateDate"]
    if not exiftool_date:
        return None
    return exiftool_date_to_utc_datetime_obj(exiftool_date)


def import_exiftool_csv_location_history_file(location_history_file):
    # type: (str) -> DataFrame
    return pd.read_csv(location_history_file, dtype=str)


def exiftool_csv_location_history_to_general_clerk_format(df):
    # type: (DataFrame) -> DataFrame
    normalized_df = pd.DataFrame()
    normalized_df["Raw Timestamp"] = df[["DateTimeOriginal", "CreateDate"]].to_dict(
        orient="records"
    )
    normalized_df["Raw Latitude"] = df["GPSLatitude"]
    normalized_df["Raw Longitude"] = df["GPSLongitude"]
    normalized_df["Raw Accuracy"] = None
    normalized_df["Raw Velocity"] = None
    normalized_df["Raw Heading"] = None
    normalized_df["Raw Altitude"] = df["GPSAltitude"]
    normalized_df["Raw Vertical Accuracy"] = None
    normalized_df["Timestamp"] = normalized_df["Raw Timestamp"].apply(
        either_date_time_original_or_create_date
    )
    normalized_df["Latitude"] = normalized_df["Raw Latitude"].apply(float)
    normalized_df["Longitude"] = normalized_df["Raw Longitude"].apply(float)
    normalized_df["Accuracy"] = None
    normalized_df["Velocity"] = None
    normalized_df["Heading"] = None
    normalized_df["Altitude"] = normalized_df["Raw Altitude"]
    normalized_df["Vertical Accuracy"] = None
    if "GPSDOP" not in df.columns:
        df["GPSDOP"] = None
    if "GPSStatus" not in df.columns:
        df["GPSStatus"] = None
    normalized_df["Original data"] = df[
        [
            "SourceFile",
            "FileSize",
            "DateTimeOriginal",
            "CreateDate",
            "GPSAltitude",
            "GPSAltitudeRef",
            "GPSDOP",
            "GPSSpeed",
            "GPSSpeedRef",
            "GPSStatus",
            "GPSLatitude",
            "GPSLatitudeRef",
            "GPSLongitude",
            "GPSLongitudeRef",
            "ImageSize",
            "FileTypeExtension",
            "MIMEType",
            "Make",
            "Model",
            "ExifVersion",
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


def exiftool_csv_location_history_parser(location_history_file):
    # type: (str) -> DataFrame
    df = import_exiftool_csv_location_history_file(location_history_file)
    return exiftool_csv_location_history_to_general_clerk_format(df)
