import pandas as pd

from clerkai.location_history.parsers.exiftool.csv import \
    exiftool_csv_location_history_parser
from clerkai.location_history.parsers.google.takeout.json import \
    google_takeout_location_history_json_location_history_parser
from clerkai.utils import clerkai_input_file_path

parser_by_content_type = {
    "exported-location-history-file/exiftool-output.csv": exiftool_csv_location_history_parser,
    (
        "exported-location-history-file/google-takeout.location-history.json"
    ): google_takeout_location_history_json_location_history_parser,
}


def parse_location_history_files(
    location_history_files, clerkai_input_folder_path, keepraw=False, failfast=False
):
    class ContentTypeNotSetError(Exception):
        pass

    class ParserNotAvailableError(Exception):
        pass

    def parse_location_history_file_row(location_history_file):
        location_history_file_path = clerkai_input_file_path(
            clerkai_input_folder_path, location_history_file
        )
        results = None
        error = None

        if failfast:
            print(
                "location_history_file",
                "%s/%s"
                % (
                    location_history_file["File path"],
                    location_history_file["File name"],
                ),
            )

        def parse():
            content_type = location_history_file["Content type"]
            if not content_type:
                raise ContentTypeNotSetError("Transaction file has no content type set")
            if (
                content_type not in parser_by_content_type
                or not parser_by_content_type[content_type]
            ):
                raise ParserNotAvailableError(
                    "Content type '%s' has no parser" % content_type
                )
            parser = parser_by_content_type[content_type]
            # print(parser, location_history_file_path)
            location_history = parser(location_history_file_path)
            location_history[
                "Source location history file index"
            ] = location_history_file.name
            # drop raw columns
            if not keepraw:
                location_history = location_history.drop(
                    [
                        "Raw Real Date",
                        "Raw Bank Date",
                        "Raw Payee",
                        "Raw Bank Message",
                        "Raw Amount",
                        "Raw Currency",
                        "Raw Balance",
                        "Raw Foreign Currency",
                        "Raw Foreign Currency Amount",
                        "Raw Foreign Currency Rate",
                        "Raw Doc Status",
                        "Raw Payment Status",
                    ],
                    axis=1,
                    errors="ignore",
                )
            return location_history

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

    if len(location_history_files) == 0:
        raise Exception("No transaction files to parse")

    parsed_location_history_file_results = location_history_files.apply(
        parse_location_history_file_row, axis=1
    )

    parsed_location_history_files = location_history_files.join(
        parsed_location_history_file_results
    )
    return parsed_location_history_files


def location_history_from_parsed_location_history_files(parsed_location_history_files):
    location_history_df = pd.concat(
        parsed_location_history_files["Parse results"].values, sort=False
    ).reset_index(drop=True)
    location_history_df = pd.merge(
        location_history_df,
        parsed_location_history_files.drop(
            ["Parse results", "History reference"], axis=1
        ).add_prefix("Source transaction file: "),
        left_on="Source transaction file index",
        right_index=True,
    )
    return location_history_df.drop(["Source transaction file index"], axis=1)
