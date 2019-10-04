from clerkai.location_history.parsers.parse_utils import (
    exiftool_date_to_datetime_obj, timestamp_ms_to_datetime_obj)


def test_timestamp_ms_to_datetime_obj():
    # type: () -> None
    assert (
        timestamp_ms_to_datetime_obj(1384393776624).strftime("%Y-%m-%d %H:%M")
        == "2013-11-14 03:49"
    )
    assert (
        timestamp_ms_to_datetime_obj("1384393776624").strftime("%Y-%m-%d %H:%M")
        == "2013-11-14 03:49"
    )


def test_exiftool_date_to_datetime_obj():
    # type: () -> None
    assert (
        exiftool_date_to_datetime_obj("2017:08:26 22:09:57").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        == "2017-08-26 22:09:57"
    )
    assert (
        exiftool_date_to_datetime_obj("2017:08:26 22:09:57+02:00").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        == "2017-08-26 20:09:57"
    )
    assert (
        exiftool_date_to_datetime_obj("2017:08:26 22:09:57-08:00").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        == "2017-08-27 06:09:57"
    )
    assert exiftool_date_to_datetime_obj("0000:00:00 00:00:00") is None
