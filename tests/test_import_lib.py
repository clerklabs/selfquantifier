def test_import_lib() -> None:
    import clerkai  # noqa: F401

    assert "Empty assert to ensure pytest passes as long as the above import succeeds."
