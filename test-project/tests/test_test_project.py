from test_project import __version__


def test_version():
    from pandas import DataFrame

    import clerkai

    clerkai_df = DataFrame()
    clerkai.export_for_github(clerkai_df)
    assert __version__ == "0.1.0"
