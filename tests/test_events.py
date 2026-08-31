import pytest

from wiki_pulse.utils import (
    is_wanted,
    to_record,
    to_insert_params,
    validate,
    threshold_at_percentile,
)
from datetime import datetime, timedelta, timezone

@pytest.fixture
def edit_event():
    """A well-formed English Wikipedia main-namespace edit."""
    return {
        "meta": {
            "id": "d7f8ec26-316d-4003-b407-7dd7165ad6a3",
            "dt": "2026-07-25T23:22:14.453Z",
            "domain": "en.wikipedia.org",
        },
        "type": "edit",
        "namespace": 0,
        "title": "Anna Ware Poole",
        "title_url": "https://en.wikipedia.org/wiki/Anna_Ware_Poole",
        "comment": "added [[Category:1838 births]] using [[WP:HC|HotCat]]",
        "user": "T. E. Meeks",
        "bot": False,
        "minor": False,
        "length": {"old": 4821, "new": 4846},
        "revision": {"old": 1366043498, "new": 1366043519},
        "server_name": "en.wikipedia.org",
    }


class TestIsWanted:
    def test_accepts_english_wikipedia_main_namespace_edit(self, edit_event):
        assert is_wanted(edit_event) is True

    def test_accepts_new_page(self, edit_event):
        assert is_wanted({**edit_event, "type": "new"}) is True

    def test_rejects_wikidata(self, edit_event):
        event = {**edit_event, "server_name": "www.wikidata.org"}
        assert is_wanted(event) is False

    @pytest.mark.parametrize("namespace", [1, 2, 4, 14])
    def test_rejects_non_main_namespace(self, edit_event, namespace):
        assert is_wanted({**edit_event, "namespace": namespace}) is False

    def test_rejects_bot_edit(self, edit_event):
        assert is_wanted({**edit_event, "bot": True}) is False

    @pytest.mark.parametrize("event_type", ["categorize", "log"])
    def test_rejects_non_content_change(self, edit_event, event_type):
        assert is_wanted({**edit_event, "type": event_type}) is False

    def test_rejects_empty_dict(self):
        assert is_wanted({}) is False


class TestToRecord:
    def test_projects_expected_fields(self, edit_event):
        record = to_record(edit_event)
        assert record == {
            "event_id": "d7f8ec26-316d-4003-b407-7dd7165ad6a3",
            "title": "Anna Ware Poole",
            "user": "T. E. Meeks",
            "type": "edit",
            "timestamp": "2026-07-25T23:22:14.453Z",
            "bytes_delta": 25,
            "minor": False,
            "title_url": "https://en.wikipedia.org/wiki/Anna_Ware_Poole",
            "comment": "added [[Category:1838 births]] using [[WP:HC|HotCat]]",
        }

    def test_new_page_without_length_old(self, edit_event):
        """A created page has no previous revision; delta is the full size."""
        event = {**edit_event, "type": "new", "length": {"new": 1204}}
        record = to_record(event)
        assert record["bytes_delta"] == 1204
        assert record["type"] == "new"

    def test_missing_length_block(self, edit_event):
        event = {k: v for k, v in edit_event.items() if k != "length"}
        assert to_record(event)["bytes_delta"] == 0

    def test_missing_optional_fields_default(self, edit_event):
        event = {k: v for k, v in edit_event.items() if k not in ("comment", "minor")}
        record = to_record(event)
        assert record["comment"] == ""
        assert record["minor"] is False

    def test_negative_delta_on_removal(self, edit_event):
        event = {**edit_event, "length": {"old": 5000, "new": 4000}}
        assert to_record(event)["bytes_delta"] == -1000
        
@pytest.fixture
def edit_record(edit_event):
    """The projected record as published to wiki-edits."""
    return to_record(edit_event)


class TestToInsertParams:
    def test_maps_record_to_column_order(self, edit_record):
        assert to_insert_params(edit_record) == (
            "d7f8ec26-316d-4003-b407-7dd7165ad6a3",
            "Anna Ware Poole",
            "T. E. Meeks",
            "edit",
            "2026-07-25T23:22:14.453Z",
            25,
            False,
            "https://en.wikipedia.org/wiki/Anna_Ware_Poole",
            "added [[Category:1838 births]] using [[WP:HC|HotCat]]",
        )

    def test_nullable_columns_accept_missing_keys(self, edit_record):
        record = {k: v for k, v in edit_record.items() if k not in ("title_url", "comment")}
        params = to_insert_params(record)
        assert params[7] is None
        assert params[8] is None

    def test_missing_minor_defaults_to_false(self, edit_record):
        record = {k: v for k, v in edit_record.items() if k != "minor"}
        assert to_insert_params(record)[6] is False

    @pytest.mark.parametrize(
        "field", ["event_id", "title", "user", "type", "timestamp", "bytes_delta"]
    )
    def test_missing_not_null_column_raises(self, edit_record, field):
        """NOT NULL columns fail in Python rather than as a constraint violation."""
        record = {k: v for k, v in edit_record.items() if k != field}
        with pytest.raises(KeyError):
            to_insert_params(record)       

class TestValidate:
    def test_accepts_valid_record(self, edit_record):
        assert validate(edit_record) is None

    @pytest.mark.parametrize(
        "field", ["event_id", "title", "user", "type", "timestamp", "bytes_delta"]
    )
    def test_rejects_missing_required_field(self, edit_record, field):
        record = {k: v for k, v in edit_record.items() if k != field}
        assert validate(record) == f"missing required field: {field}"

    @pytest.mark.parametrize(
        "field", ["event_id", "title", "user", "type", "timestamp", "bytes_delta"]
    )
    def test_rejects_null_required_field(self, edit_record, field):
        assert validate({**edit_record, field: None}) is not None

    @pytest.mark.parametrize("title", ["", "   ", "\t\n"])
    def test_rejects_empty_title(self, edit_record, title):
        assert validate({**edit_record, "title": title}) == "title is empty"

    @pytest.mark.parametrize("delta", ["25", 25.5, True])
    def test_rejects_non_integer_bytes_delta(self, edit_record, delta):
        reason = validate({**edit_record, "bytes_delta": delta})
        assert reason is not None
        assert "bytes_delta" in reason

    @pytest.mark.parametrize("ts", ["not-a-date", "", 1785017619])
    def test_rejects_unparseable_timestamp(self, edit_record, ts):
        reason = validate({**edit_record, "timestamp": ts})
        assert reason is not None
        assert "timestamp" in reason

    def test_rejects_future_timestamp(self, edit_record):
        future = datetime.now(timezone.utc) + timedelta(days=1)
        reason = validate({**edit_record, "timestamp": future.isoformat()})
        assert reason is not None
        assert "future" in reason

    def test_rejects_prehistoric_timestamp(self, edit_record):
        reason = validate({**edit_record, "timestamp": "1999-01-01T00:00:00Z"})
        assert reason is not None
        assert "predates" in reason

    def test_accepts_small_clock_skew(self, edit_record):
        """Clock skew between Wikimedia and the consumer shouldn't reject valid records."""
        skewed = datetime.now(timezone.utc) + timedelta(seconds=60)
        assert validate({**edit_record, "timestamp": skewed.isoformat()}) is None

@pytest.fixture
def quiet_window():
    """Realistic distribution: almost every bucket is a single edit, one clear spike."""
    return [1] * 99 + [50]


class TestThresholdAtPercentile:
    def test_empty_window_returns_none(self):
        """An empty lookback window is a no-op for the DAG, not an error."""
        assert threshold_at_percentile([], 99.5) is None

    def test_single_value(self):
        assert threshold_at_percentile([5], 99.5) == 5

    def test_uniform_counts_return_that_value(self):
        assert threshold_at_percentile([1] * 10, 99) == 1.0

    def test_sorts_input(self):
        """Callers pass rows straight from SQL, which come back unordered."""
        assert threshold_at_percentile([5, 1, 3, 2, 4], 50) == 3

    @pytest.mark.parametrize(("percentile", "expected"), [(0, 1), (50, 3), (100, 5)])
    def test_percentile_bounds(self, percentile, expected):
        assert threshold_at_percentile([1, 2, 3, 4, 5], percentile) == expected

    def test_interpolates_between_neighbours(self):
        assert threshold_at_percentile([10, 20], 50) == 15.0

    def test_spike_exceeds_threshold(self, quiet_window):
        """The one busy bucket is above the cutoff; the quiet ones are not."""
        threshold = threshold_at_percentile(quiet_window, 99)
        assert 50 > threshold
        assert not 1 > threshold