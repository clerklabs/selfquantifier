import json

import pandas as pd

from clerkai.time_tracking.parsers.neamtime.tslog import \
    neamtime_tslog_time_tracking_entries_parser
from clerkai.utils import clerkai_input_file_path, is_nan

parser_by_content_type = {
    "exported-time-tracking-file/neamtime-tslog": neamtime_tslog_time_tracking_entries_parser,
}


def naive_time_tracking_entry_ids(time_tracking_entries):
    import jellyfish

    def generate_naive_time_tracking_entry_id(time_tracking_entry):
        def none_if_nan(x):
            if is_nan(x):
                return None
            else:
                return x

        def raw_if_available(field_name, time_tracking_entry):
            raw_field_name = "Raw %s" % field_name
            if (
                raw_field_name in time_tracking_entry
                and time_tracking_entry[raw_field_name] is not None
            ):
                return time_tracking_entry[raw_field_name]
            if field_name in time_tracking_entry:
                return time_tracking_entry[field_name]
            else:
                return None

        id_key_dict = {}
        id_key_dict["foo"] = "bar"
        return json.dumps(id_key_dict, sort_keys=True, default=str, allow_nan=False)

    return time_tracking_entries.apply(generate_naive_time_tracking_entry_id, axis=1)


def naive_time_tracking_entry_id_duplicate_nums(time_tracking_entries):
    return (
        time_tracking_entries.groupby(["naive_time_tracking_entry_id"]).cumcount() + 1
    )


def time_tracking_entry_ids(time_tracking_entries):
    copy = time_tracking_entries.copy()
    copy["naive_time_tracking_entry_id"] = naive_time_tracking_entry_ids(copy)
    copy[
        "naive_time_tracking_entry_id_duplicate_num"
    ] = naive_time_tracking_entry_id_duplicate_nums(copy)

    def generate_time_tracking_entry_id(time_tracking_entry):
        id_key_dict = {
            "ref": json.loads(time_tracking_entry["naive_time_tracking_entry_id"]),
            "ord": time_tracking_entry["naive_time_tracking_entry_id_duplicate_num"],
        }
        return json.dumps(id_key_dict)

    return copy.apply(generate_time_tracking_entry_id, axis=1)


def parse_time_tracking_files(
    time_tracking_files, clerkai_input_folder_path, keepraw=False, failfast=False
):
    class ContentTypeNotSetError(Exception):
        pass

    class ParserNotAvailableError(Exception):
        pass

    def parse_time_tracking_file_row(time_tracking_file):
        time_tracking_file_path = clerkai_input_file_path(
            clerkai_input_folder_path, time_tracking_file
        )
        results = None
        error = None

        if failfast:
            print(
                "time_tracking_file",
                "%s/%s"
                % (time_tracking_file["File path"], time_tracking_file["File name"]),
            )

        def parse():
            content_type = time_tracking_file["Content type"]
            if not content_type:
                raise ContentTypeNotSetError(
                    "Time tracking file has no content type set"
                )
            if (
                content_type not in parser_by_content_type
                or not parser_by_content_type[content_type]
            ):
                raise ParserNotAvailableError(
                    "Content type '%s' has no parser" % content_type
                )
            parser = parser_by_content_type[content_type]
            # print(parser, time_tracking_file_path)
            time_tracking_entries = parser(time_tracking_file_path)
            time_tracking_entries[
                "Source time tracking file index"
            ] = time_tracking_file.name
            # add future join/merge index
            time_tracking_entries["ID"] = time_tracking_entry_ids(time_tracking_entries)
            # drop raw columns
            if not keepraw:
                time_tracking_entries = time_tracking_entries.drop(
                    [
                        "Raw Foo",
                    ],
                    axis=1,
                    errors="ignore",
                )
            return time_tracking_entries

        # failfast raises errors except expected/benign value errors
        if failfast:
            try:
                results = parse()
            except (ContentTypeNotSetError, ParserNotAvailableError) as e:
                error = e
        else:
            try:
                results = parse()
            except Exception as e:
                error = e
        return pd.Series([results, error], index=["Parse results", "Error"])

    if len(time_tracking_files) == 0:
        raise Exception("No time tracking files to parse")

    parsed_time_tracking_file_results = time_tracking_files.apply(
        parse_time_tracking_file_row, axis=1
    )

    parsed_time_tracking_files = time_tracking_files.join(
        parsed_time_tracking_file_results
    )
    return parsed_time_tracking_files


def time_tracking_entries_from_parsed_time_tracking_files(parsed_time_tracking_files):
    time_tracking_entries_df = pd.concat(
        parsed_time_tracking_files["Parse results"].values, sort=False
    ).reset_index(drop=True)
    time_tracking_entries_df = pd.merge(
        time_tracking_entries_df,
        parsed_time_tracking_files.drop(
            ["Parse results", "History reference"], axis=1
        ).add_prefix("Source time tracking file: "),
        left_on="Source time tracking file index",
        right_index=True,
    )
    return time_tracking_entries_df.drop(["Source time tracking file index"], axis=1)
