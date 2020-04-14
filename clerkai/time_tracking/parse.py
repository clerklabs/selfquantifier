import json

import pandas as pd
from joblib import Memory

from clerkai.time_tracking.parsers.neamtime.tslog import \
    neamtime_tslog_time_tracking_entries_parser
from clerkai.utils import clerkai_input_file_path, is_nan, raw_if_available

memory = Memory(location="/tmp", verbose=0)

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

        id_key_dict = {}
        id_key_dict["utc_timestamp"] = none_if_nan(
            raw_if_available("UTC Timestamp", time_tracking_entry)
        )
        source_lines_summary = none_if_nan(
            raw_if_available("Source Lines Summary", time_tracking_entry)
        )
        id_key_dict["source_lines_summary"] = (
            jellyfish.soundex(source_lines_summary)
            if type(source_lines_summary) is str
            else source_lines_summary
        )
        id_key_dict["session"] = none_if_nan(
            raw_if_available("Session", time_tracking_entry)
        )
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

    class ProcessingErrorsEncountered(Exception):
        pass

    def parse_time_tracking_file_row(time_tracking_file):
        time_tracking_file_path = clerkai_input_file_path(
            clerkai_input_folder_path, time_tracking_file
        )
        results = {
            "time_tracking_entries": None,
            "parsing_metadata": None,
            "processing_errors": None,
        }
        error = None

        if failfast:
            print(
                '* Processing time tracking file with path: "%s/%s"'
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

            @memory.cache
            def parse_file(_file_metadata, _content_type):
                return parser(time_tracking_file_path)

            time_tracking_entries, parsing_metadata, processing_errors = parse_file(
                time_tracking_file["File metadata"], content_type
            )

            # mark which source file was parsed, for later merging of the data
            time_tracking_entries[
                "Source time tracking file index"
            ] = time_tracking_file.name
            parsing_metadata[
                "Source time tracking file index"
            ] = time_tracking_file.name
            processing_errors[
                "Source time tracking file index"
            ] = time_tracking_file.name

            # time tracking entries ids
            if len(time_tracking_entries) > 0:
                # add future join/merge index
                time_tracking_entries["ID"] = time_tracking_entry_ids(
                    time_tracking_entries
                )
            else:
                time_tracking_entries["ID"] = None

            # drop raw columns
            if not keepraw:
                time_tracking_entries = time_tracking_entries.drop(
                    [
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
                        "Raw Hours Rounded",
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
                    ],
                    axis=1,
                    errors="ignore",
                )
            return {
                "time_tracking_entries": time_tracking_entries,
                "parsing_metadata": parsing_metadata,
                "processing_errors": processing_errors,
            }

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

        # check for processing errors stemming from invalid/non-understood
        # time tracking files (rather than errors in the parser)
        if (
            results["processing_errors"] is not None
            and len(results["processing_errors"]) > 0
        ):
            error = ProcessingErrorsEncountered("Processing errors encountered")

        return pd.Series(
            [
                results["time_tracking_entries"],
                results["parsing_metadata"],
                results["processing_errors"],
                error,
            ],
            index=[
                "Parsed time tracking entries",
                "Parsing metadata",
                "Processing errors",
                "Error",
            ],
        )

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
