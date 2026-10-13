"""Tests for vepyr.download_cache and vepyr.download_plugin_cache."""

from __future__ import annotations

import pytest
from huggingface_hub import errors
from huggingface_hub import utils as hub_utils

from vepyr import _download


def hub_error(cls, message="fake"):
    """An instance of a huggingface_hub HTTP error without an HTTP response."""
    error = cls.__new__(cls)
    OSError.__init__(error, message)
    return error


# A transport error as httpx (huggingface_hub 1.x) or httpx2 (2.x) raises it.
TransportError = type(
    "ConnectError", (Exception,), {"__module__": "httpx2._exceptions"}
)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (hub_error(errors.RepositoryNotFoundError), ValueError),
        (hub_error(errors.RevisionNotFoundError), ValueError),
        (hub_error(errors.HfHubHTTPError), RuntimeError),
        (errors.LocalEntryNotFoundError("offline"), RuntimeError),
        (ConnectionError("refused"), RuntimeError),
        (TransportError("refused"), RuntimeError),
    ],
)
def test_hub_errors_are_translated_and_name_the_dataset(error, expected):
    with pytest.raises(expected) as excinfo:
        with _download._hub_errors("biodatageeks/x", "abc123"):
            raise error
    assert "biodatageeks/x" in str(excinfo.value)
    assert "abc123" in str(excinfo.value)


def test_other_errors_pass_through():
    with pytest.raises(KeyError):
        with _download._hub_errors("biodatageeks/x", "abc123"):
            raise KeyError("bug")


def test_progress_false_disables_and_restores_even_on_error():
    hub_utils.enable_progress_bars()
    with pytest.raises(RuntimeError):
        with _download._progress(False):
            assert hub_utils.are_progress_bars_disabled()
            raise RuntimeError("boom")
    assert not hub_utils.are_progress_bars_disabled()


def test_progress_false_keeps_bars_disabled_if_they_already_were():
    hub_utils.disable_progress_bars()
    try:
        with _download._progress(False):
            pass
        assert hub_utils.are_progress_bars_disabled()
    finally:
        hub_utils.enable_progress_bars()


def test_progress_true_leaves_the_state_alone():
    hub_utils.enable_progress_bars()
    with _download._progress(True):
        assert not hub_utils.are_progress_bars_disabled()
