from clerkai.location_history.parsers.parse_utils import (
    exiftool_date_to_utc_datetime_obj, timestamp_ms_to_utc_datetime_obj)


def test_timestamp_ms_to_utc_datetime_obj():
    # type: () -> None
    # TODO: check if this is correct - the results should be the same regardless of the timezone of the os
    assert (
        timestamp_ms_to_utc_datetime_obj(0).strftime("%Y-%m-%d %H:%M%z")
        == "1970-01-01 00:00+0000"
    )
    assert (
        timestamp_ms_to_utc_datetime_obj(1384393776624).strftime("%Y-%m-%d %H:%M%z")
        == "2013-11-14 01:49+0000"
    )
    assert (
        timestamp_ms_to_utc_datetime_obj("1384393776624").strftime("%Y-%m-%d %H:%M%z")
        == "2013-11-14 01:49+0000"
    )
    assert (
        timestamp_ms_to_utc_datetime_obj("1574246049000").strftime("%Y-%m-%d %H:%M%z")
        == "2019-11-20 10:34+0000"
    )


def test_exiftool_date_to_utc_datetime_obj():
    # type: () -> None
    assert (
        exiftool_date_to_utc_datetime_obj("2017:08:26 22:09:57").strftime(
            "%Y-%m-%d %H:%M:%S%z"
        )
        == "2017-08-26 22:09:57+0000"
    )
    assert (
        exiftool_date_to_utc_datetime_obj("2017:08:26 22:09:57").strftime(
            "%Y-%m-%d %H:%M:%S%z"
        )
        == "2017-08-26 22:09:57+0000"
    )
    assert (
        exiftool_date_to_utc_datetime_obj("2017:08:26 22:09:57+02:00").strftime(
            "%Y-%m-%d %H:%M:%S%z"
        )
        == "2017-08-26 20:09:57+0000"
    )
    assert (
        exiftool_date_to_utc_datetime_obj("2017:08:26 22:09:57-08:00").strftime(
            "%Y-%m-%d %H:%M:%S%z"
        )
        == "2017-08-27 06:09:57+0000"
    )
    assert exiftool_date_to_utc_datetime_obj("0000:00:00 00:00:00") is None
