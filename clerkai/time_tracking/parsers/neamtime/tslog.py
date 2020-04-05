import json
import subprocess
from datetime import datetime
from typing import List

import pandas as pd
import pytz
from pandas.core.frame import DataFrame
from pandas.io.json import json_normalize

from clerkai.utils import is_nan, ymd_date_to_naive_datetime_obj

return_columns: List[str] = [
    "Raw Ignore",
    "Raw Source Line Numbers",
    "Raw Source Lines Summary",
    "Raw Session",
    "Raw UTC Timestamp",
    "Raw Work Date",
    "Raw Date Before Parsing",
    "Raw Timezone",
    "Raw Time-annotated Source Lines Summary",
    "Raw Hours",
    # "Raw Hours Rounded",
    "Raw Annotation",
    "Raw Year",
    "Raw Year-half",
    "Raw Month",
    "Raw Week",
    "Raw Client",
    "Raw Invoice",
    "Raw Invoice row ordinal",
    "Raw Invoice row item",
    "Raw Invoice row item category",
    "Raw Invoice row item reference",
    "Raw Invoice hours",
    "Ignore",
    "Source Line Numbers",
    "Source Lines Summary",
    "Session",
    "UTC Timestamp",
    "Work Date",
    "Timestamp Before Parsing",
    "Timezone",
    "Time-annotated Source Lines Summary",
    "Hours",
    # "Hours Rounded",
    "Annotation",
    "Year",
    "Year-half",
    "Month",
    "Week",
    "Client",
    "Invoice",
    "Invoice row ordinal",
    "Invoice row item",
    "Invoice row item category",
    "Invoice row item reference",
    "Invoice hours",
    "Original data",
]


def neamtime_datetime_to_utc_datetime_obj(neamtime_datetime):
    naive_parsed_datetime = neamtime_datetime_to_naive_datetime_obj(neamtime_datetime)
    parsed_datetime = pytz.utc.localize(naive_parsed_datetime)
    return parsed_datetime


def neamtime_datetime_to_naive_datetime_obj(neamtime_datetime):
    if neamtime_datetime is None:
        return None
    if is_nan(neamtime_datetime):
        return None
    naive_parsed_datetime = datetime.strptime(neamtime_datetime, "%Y-%m-%d %H:%M")
    return naive_parsed_datetime


def parse_neamtime_tslog_time_tracking_file(time_tracking_file_path):
    # type: (str) -> DataFrame
    # print("time_tracking_file_path", time_tracking_file_path)

    result = subprocess.run(
        ["neamtime-log-parser", "--filePath", time_tracking_file_path, ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if result.returncode == 1:
        print("Neamtime log parsing failed: %s" % result.stderr.decode("utf-8"))
        raise Exception("Neamtime log parsing failed")

    # print("result", result)

    json_str = result.stdout.decode("utf-8")
    # print("json_str", json_str)

    parse_results = json.loads(json_str)
    # print("parse_results", parse_results)

    parsing_metadata = json_normalize(parse_results)
    parsed_time_log_entries = json_normalize(
        parse_results["timeLogEntriesWithMetadata"]
    )

    # print("tslog.py - parsing_metadata", parsing_metadata)
    # print("tslog.py - parsing_metadata.columns", parsing_metadata.columns)
    # print("tslog.py - time_log_entries", parsed_time_log_entries)
    # print("tslog.py - time_log_entries.columns", parsed_time_log_entries.columns)

    if len(parsed_time_log_entries) == 0:
        return pd.DataFrame(columns=return_columns)

    return parsed_time_log_entries


def neamtime_tslog_time_tracking_entries_to_general_clerk_format(df):
    # type: (DataFrame) -> DataFrame
    normalized_df = pd.DataFrame(columns=return_columns)

    if len(df) == 0:
        return normalized_df

    """
    normalized_df["Raw "] = df["ts"]
    normalized_df["Raw "] = df["sessionMeta.tzFirst"]
    """

    normalized_df["Raw Ignore"] = None
    normalized_df["Raw Source Line Numbers"] = None
    normalized_df["Raw Source Lines Summary"] = df["text"]
    normalized_df["Raw Session"] = df["sessionMeta.session_ref"]
    normalized_df["Raw UTC Timestamp"] = df["gmtTimestamp"]
    normalized_df["Raw Work Date"] = df["date"]
    # normalized_df["Raw Timestamp Before Parsing"] = df["dateRaw"] # No, dateRaw is not what it seems
    normalized_df["Raw Timezone"] = df["tz"]
    normalized_df["Raw Time-annotated Source Lines Summary"] = df["lineWithoutDate"]
    normalized_df["Raw Hours"] = df["hours"]
    # normalized_df["Raw Hours Rounded"] = df["hoursRounded"]
    normalized_df["Raw Annotation"] = df["category"]
    normalized_df["Raw Year"] = None
    normalized_df["Raw Year-half"] = None
    normalized_df["Raw Month"] = None
    normalized_df["Raw Week"] = None
    normalized_df["Raw Client"] = None
    normalized_df["Raw Invoice"] = None
    normalized_df["Raw Invoice row ordinal"] = None
    normalized_df["Raw Invoice row item"] = None
    normalized_df["Raw Invoice row item category"] = None
    normalized_df["Raw Invoice row item reference"] = None
    normalized_df["Raw Invoice hours"] = None

    normalized_df["Ignore"] = normalized_df["Raw Ignore"]
    normalized_df["Source Line Numbers"] = normalized_df["Raw Source Line Numbers"]
    normalized_df["Source Lines Summary"] = normalized_df[
        "Raw Source Lines Summary"
    ].str.strip()
    normalized_df["Session"] = normalized_df["Raw Session"]
    normalized_df["UTC Timestamp"] = normalized_df["Raw UTC Timestamp"].apply(
        neamtime_datetime_to_naive_datetime_obj
    )
    normalized_df["Work Date"] = normalized_df["Raw Work Date"].apply(
        ymd_date_to_naive_datetime_obj
    )
    # normalized_df["Timestamp Before Parsing"] = normalized_df["Raw Timestamp Before Parsing"]
    normalized_df["Timezone"] = normalized_df["Raw Timezone"]
    normalized_df["Time-annotated Source Lines Summary"] = normalized_df[
        "Raw Time-annotated Source Lines Summary"
    ].str.strip()
    normalized_df["Hours"] = normalized_df["Raw Hours"]
    # normalized_df["Hours Rounded"] = normalized_df["Raw Hours Rounded"].apply(
    #     amount_to_rounded_decimal
    # )
    normalized_df["Annotation"] = normalized_df["Raw Annotation"]
    normalized_df["Year"] = normalized_df["Raw Year"]
    normalized_df["Year-half"] = normalized_df["Raw Year-half"]
    normalized_df["Month"] = normalized_df["Raw Month"]
    normalized_df["Week"] = normalized_df["Raw Week"]
    normalized_df["Client"] = normalized_df["Raw Client"]
    normalized_df["Invoice"] = normalized_df["Raw Invoice"]
    normalized_df["Invoice row ordinal"] = normalized_df["Raw Invoice row ordinal"]
    normalized_df["Invoice row item"] = normalized_df["Raw Invoice row item"]
    normalized_df["Invoice row item category"] = normalized_df[
        "Raw Invoice row item category"
    ]
    normalized_df["Invoice row item reference"] = normalized_df[
        "Raw Invoice row item reference"
    ]
    normalized_df["Invoice hours"] = normalized_df["Raw Invoice hours"]

    normalized_df["Original data"] = df[
        [
            "gmtTimestamp",
            "category",
            "date",
            "dateRaw",
            "hours",
            "hoursRounded",
            "lineWithoutDate",
            "text",
            "ts",
            "tz",
            "sessionMeta.session_ref",
            "sessionMeta.tzFirst",
        ]
    ].to_dict(orient="records")

    # print("normalized_df.columns", normalized_df.columns)

    return normalized_df[return_columns]


def neamtime_tslog_time_tracking_entries_parser(time_tracking_file_path):
    # type: (str) -> DataFrame
    df = parse_neamtime_tslog_time_tracking_file(time_tracking_file_path)
    return neamtime_tslog_time_tracking_entries_to_general_clerk_format(df)
