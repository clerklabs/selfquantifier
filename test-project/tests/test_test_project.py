from test_project import __version__


def test_version():
    from pandas import DataFrame

    import clerkai

    assert __version__ == "0.1.0"
