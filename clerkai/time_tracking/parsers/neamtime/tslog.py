import json
import subprocess

from pandas.io.json import json_normalize


def neamtime_tslog_time_tracking_entries_parser(time_tracking_file_path):
    print("time_tracking_file_path", time_tracking_file_path)

    result = subprocess.run(
        ["neamtime-log-parser", "--filePath", time_tracking_file_path, ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if result.returncode == 1:
        print("Neamtime log parsing failed: %s" % result.stderr.decode("utf-8"))
        raise Exception("Neamtime log parsing failed")

    print("result", result)

    json_str = result.stdout.decode("utf-8")
    print("json_str", json_str)

    parse_results = json.loads(json_str)
    print("parse_results", parse_results)

    parsing_metadata = json_normalize(parse_results)
    time_log_entries = json_normalize(parse_results["timeLogEntriesWithMetadata"])

    print("parsing_metadata", parsing_metadata)
    print("time_log_entries", time_log_entries)

    return time_log_entries
