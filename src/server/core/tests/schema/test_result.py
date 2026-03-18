from acontext_core.schema.result import Result, Code
from acontext_core.telemetry.log import (
    get_wide_event,
    set_wide_event,
    clear_wide_event,
)


def test_result_class():
    test_data = {"message": "pong"}
    suc = Result.resolve(test_data)
    d, eil = suc.unpack()
    assert d == test_data
    assert eil is None

    err = Result.reject("test", Code.BAD_REQUEST)
    d, eil = err.unpack()
    assert d is None
    assert eil.status == Code.BAD_REQUEST


# ---------------------------------------------------------------------------
# get_wide_event: always returns a dict
# ---------------------------------------------------------------------------


def test_get_wide_event_returns_dict_when_no_contextvar():
    clear_wide_event()
    wide = get_wide_event()
    assert isinstance(wide, dict)
    wide["key"] = "value"


def test_get_wide_event_returns_contextvar_dict_when_set():
    event = {"handler": "test"}
    set_wide_event(event)
    try:
        wide = get_wide_event()
        assert wide is event
        wide["extra"] = 1
        assert event["extra"] == 1
    finally:
        clear_wide_event()


def test_get_wide_event_throwaway_dict_does_not_pollute_contextvar():
    """Writing to the throwaway dict must not set the contextvar."""
    clear_wide_event()
    throwaway = get_wide_event()
    throwaway["garbage"] = True

    next_call = get_wide_event()
    assert "garbage" not in next_call


# ---------------------------------------------------------------------------
# Result.resolve records deduplicated caller in success_stack
# ---------------------------------------------------------------------------


def test_resolve_records_caller_in_success_stack():
    event: dict = {}
    set_wide_event(event)
    try:
        Result.resolve("a")
        Result.resolve("b")
        Result.resolve("c")
        assert event["success_stack"] == ["test_resolve_records_caller_in_success_stack"]
    finally:
        clear_wide_event()


def _helper_resolve():
    return Result.resolve("from helper")


def test_resolve_deduplicates_same_caller():
    event: dict = {}
    set_wide_event(event)
    try:
        Result.resolve("first")
        _helper_resolve()
        Result.resolve("second")
        _helper_resolve()
        assert event["success_stack"] == [
            "test_resolve_deduplicates_same_caller",
            "_helper_resolve",
        ]
    finally:
        clear_wide_event()


# ---------------------------------------------------------------------------
# Result.reject appends to error_stack
# ---------------------------------------------------------------------------


def test_reject_appends_error_to_wide_event():
    event: dict = {}
    set_wide_event(event)
    try:
        Result.reject("something broke", Code.INTERNAL_ERROR)
        assert len(event["error_stack"]) == 1
        assert event["error_stack"][0]["errmsg"] == "something broke"
        assert event["error_stack"][0]["status"] == str(Code.INTERNAL_ERROR)
        assert event["error_stack"][0]["caller"] == "test_reject_appends_error_to_wide_event"
    finally:
        clear_wide_event()


def test_reject_accumulates_multiple_errors():
    event: dict = {}
    set_wide_event(event)
    try:
        Result.reject("first", Code.BAD_REQUEST)
        Result.reject("second", Code.INTERNAL_ERROR)
        assert len(event["error_stack"]) == 2
        assert event["error_stack"][0]["errmsg"] == "first"
        assert event["error_stack"][1]["errmsg"] == "second"
    finally:
        clear_wide_event()


def test_reject_without_wide_event_does_not_raise():
    """reject() outside MQ context must not crash."""
    clear_wide_event()
    err = Result.reject("no context", Code.BAD_REQUEST)
    d, eil = err.unpack()
    assert d is None
    assert eil.status == Code.BAD_REQUEST
