import pytest

from wiki_pulse.utils import is_wanted, to_record, to_insert_params


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
