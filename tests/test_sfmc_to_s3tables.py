from datetime import datetime, timedelta, timezone

import pytest

from sfmc_to_s3tables import ENTITIES, materialize, merge_sql, parse_list, to_snake, window


@pytest.mark.parametrize(
    "name, expected",
    [
        ("SubscriberKey", "subscriber_key"),
        ("SendID", "send_id"),
        ("URL", "url"),
        ("EventDate", "event_date"),
        ("ID", "id"),
        ("TriggeredSendDefinitionObjectID", "triggered_send_definition_object_id"),
        ("Client.ID", "client_id"),
        ("HTMLBody", "html_body"),
        ("already_snake", "already_snake"),
    ],
)
def test_to_snake(name, expected):
    assert to_snake(name) == expected


def test_window_spans_lookback_days_through_next_utc_midnight():
    now = datetime(2026, 10, 14, 15, 30, tzinfo=timezone.utc)
    assert window(now, 2) == ("2026-10-12T00:00:00.000Z", "2026-10-15T00:00:00.000Z")


def test_window_uses_utc_day_for_non_utc_input():
    # 23:00 at UTC-5 is 04:00 the next day in UTC
    now = datetime(2026, 10, 14, 23, 0, tzinfo=timezone(timedelta(hours=-5)))
    assert window(now, 2) == ("2026-10-13T00:00:00.000Z", "2026-10-16T00:00:00.000Z")


def test_window_zero_lookback_is_today_only():
    now = datetime(2026, 10, 14, 0, 0, tzinfo=timezone.utc)
    assert window(now, 0) == ("2026-10-14T00:00:00.000Z", "2026-10-15T00:00:00.000Z")


def test_merge_sql_events_insert_only():
    sql = merge_sql("open_event", "src", ("send_id", "event_date"), ["send_id", "event_date", "x"], update=False)
    assert sql == (
        "MERGE INTO s3tables.sfmc.open_event t USING src s"
        " ON t.send_id <=> s.send_id AND t.event_date <=> s.event_date"
        " WHEN NOT MATCHED THEN INSERT (send_id, event_date, x) VALUES (s.send_id, s.event_date, s.x)"
    )


def test_merge_sql_dimensions_update_non_key_columns():
    sql = merge_sql("send", "src", ("id",), ["id", "name", "_ingested_at"], update=True)
    assert sql == (
        "MERGE INTO s3tables.sfmc.send t USING src s ON t.id <=> s.id"
        " WHEN MATCHED THEN UPDATE SET t.name = s.name, t._ingested_at = s._ingested_at"
        " WHEN NOT MATCHED THEN INSERT (id, name, _ingested_at) VALUES (s.id, s.name, s._ingested_at)"
    )


def test_merge_sql_events_bound_target_to_window():
    sql = merge_sql(
        "open_event", "src", ("send_id", "event_date"), ["send_id", "event_date"], update=False,
        target_since="2026-10-12T00:00:00.000Z",
    )
    assert (
        " ON t.send_id <=> s.send_id AND t.event_date <=> s.event_date"
        " AND t.event_date >= TIMESTAMP '2026-10-12T00:00:00.000Z' WHEN NOT MATCHED"
    ) in sql


class FakeFrame:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def persist(self):
        self.calls.append("persist")
        return self

    def count(self):
        self.calls.append("count")
        return self.rows


def test_materialize_caches_before_counting():
    df = FakeFrame(3)
    cached, rows = materialize(df)
    assert (cached, rows) == (df, 3)
    assert df.calls == ["persist", "count"]


def test_parse_list_trims_and_drops_empties():
    assert parse_list(" sent_event, ,open_event ,") == ["sent_event", "open_event"]
    assert parse_list("") == []


def test_entities_cover_planned_tables():
    assert set(ENTITIES) == {
        "sent_event", "open_event", "click_event", "bounce_event", "unsub_event",
        "not_sent_event", "send", "subscriber", "list", "email",
    }


def test_event_entities_are_incremental_and_dimensions_are_not():
    for table, entity in ENTITIES.items():
        assert entity.table == table
        if table.endswith("_event"):
            assert entity.event_date == "EventDate"
            assert {"send_id", "subscriber_key", "event_date", "event_type"} <= set(entity.keys)
        else:
            assert entity.event_date is None
            assert entity.keys == ("id",)


def test_click_event_key_includes_url():
    assert "url" in ENTITIES["click_event"].keys
    assert "url" not in ENTITIES["open_event"].keys


def test_entity_keys_are_already_snake_case():
    for entity in ENTITIES.values():
        assert all(to_snake(k) == k for k in entity.keys)
