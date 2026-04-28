"""Cursor-advancement tests — locks in the fix for Sahil/Copilot finding #2.

Bug: connector fetched a single page (skip=0) then advanced
state["last_receive_date"] to the latest receive_date in that page. When
total > max_events, every event past max_events was permanently skipped
because (a) the next sync's range filter started at the just-advanced
cursor and (b) the connector had no client-side dedupe.

Fix (mirrors PR #567 / drug-label):
  1. Paginate skip/limit through all pages up to max_events.
  2. Compound cursor (receivedate, safety_report_id) — both keys persist.
  3. Skip records whose tuple is <= the cursor on each subsequent sync.

# see databricks-fm-fda-drug-label-intelligence/tests/integration/
# test_cursor_advancement.py for the canonical version of this pattern.
"""

import pytest

import connector


def _make_event(report_id, receive_date, serious=True):
    """Minimal raw FAERS event — receive_date drives the cursor."""
    return {
        "safetyreportid": report_id,
        "receivedate": receive_date,
        "serious": "1" if serious else "2",
        "patient": {
            "drug": [{"medicinalproduct": "Aspirin"}],
            "reaction": [{"reactionmeddrapt": "Headache"}],
        },
    }


@pytest.fixture
def four_events_one_window():
    """Four events all within the same range filter window."""
    return [
        _make_event("RPT-A", "20260415"),
        _make_event("RPT-B", "20260416"),
        _make_event("RPT-C", "20260417"),
        _make_event("RPT-D", "20260418"),
    ]


def _make_paginated_fetcher(events_to_return):
    """Build a fake fetch_adverse_events that simulates OpenFDA: parses
    the receivedate range from the search_query, filters events to that
    range, and returns a sorted page with skip+limit semantics. The sort
    key matches the cursor tuple (receivedate, id)."""
    import re

    sorted_all = sorted(events_to_return, key=lambda e: (e["receivedate"], e["safetyreportid"]))

    def fake_fetch(session, search_query, limit, skip):
        # Parse `receivedate:[X+TO+Y]` from the query. OpenFDA range is
        # inclusive on both bounds.
        m = re.search(r"receivedate:\[(\d{8})\+TO\+(\d{8})\]", search_query)
        if m:
            lo, hi = m.group(1), m.group(2)
            filtered = [e for e in sorted_all if lo <= e["receivedate"] <= hi]
        else:
            filtered = sorted_all
        return filtered[skip : skip + limit], len(filtered)

    return fake_fetch


class TestCapHitAdvancesCursorToLastSynced:
    """When max_events caps the sync, the cursor advances to the highest
    (date, id) tuple actually synced — so the next sync's skip-already-
    synced filter knows exactly where to resume."""

    def test_cursor_advances_to_last_synced_tuple(
        self,
        base_config,
        four_events_one_window,
        captured_upserts,
        monkeypatch,
    ):
        config = dict(base_config)
        config["max_events"] = "2"
        config["enable_enrichment"] = "false"

        monkeypatch.setattr(
            connector,
            "fetch_adverse_events",
            _make_paginated_fetcher(four_events_one_window),
        )

        connector.update(config, {})

        event_upserts = [u for u in captured_upserts["upserts"] if u["table"] == "adverse_events"]
        assert len(event_upserts) == 2
        synced_ids = [u["data"]["safety_report_id"] for u in event_upserts]
        # Sorted (receivedate, id) ASC — first 2 are RPT-A (20260415) and
        # RPT-B (20260416).
        assert synced_ids == ["RPT-A", "RPT-B"]

        last_state = captured_upserts["checkpoints"][-1]
        assert last_state.get("last_receive_date") == "20260416"
        assert last_state.get("last_safety_report_id") == "RPT-B"


class TestRepeatedSyncsCoverAllEventsNoDuplicates:
    """The hard regression test: repeated syncs with a small max_events
    cap against a larger window must end up with all events synced
    exactly once. Loop until either everything is synced or we've made
    enough attempts that the bug pattern (no progress) would loop
    forever."""

    def test_drained_in_finite_runs_no_duplicates(
        self,
        base_config,
        four_events_one_window,
        captured_upserts,
        monkeypatch,
    ):
        config = dict(base_config)
        config["max_events"] = "2"
        config["enable_enrichment"] = "false"

        monkeypatch.setattr(
            connector,
            "fetch_adverse_events",
            _make_paginated_fetcher(four_events_one_window),
        )

        state = {}
        max_attempts = 10  # small cap; the pre-fix bug would loop forever
        for _ in range(max_attempts):
            connector.update(config, state)
            state = dict(captured_upserts["checkpoints"][-1])
            synced_ids = {
                u["data"]["safety_report_id"]
                for u in captured_upserts["upserts"]
                if u["table"] == "adverse_events"
            }
            if synced_ids == {"RPT-A", "RPT-B", "RPT-C", "RPT-D"}:
                break

        all_event_upserts = [
            u for u in captured_upserts["upserts"] if u["table"] == "adverse_events"
        ]
        synced_ids_list = [u["data"]["safety_report_id"] for u in all_event_upserts]

        assert sorted(set(synced_ids_list)) == ["RPT-A", "RPT-B", "RPT-C", "RPT-D"], (
            f"Repeated syncs should cover all 4 events; " f"got {sorted(set(synced_ids_list))}"
        )
        # No duplicates: each report id should appear exactly once across
        # all syncs combined.
        assert len(synced_ids_list) == len(
            set(synced_ids_list)
        ), f"Duplicate upserts detected: {synced_ids_list}"


class TestPaginationFetchesBeyondFirstPage:
    """fetch_all_events must page through skip+limit until either
    max_events is reached or the API runs out. The pre-fix bug used a
    single skip=0 call regardless of max_events."""

    def test_pagination_calls_with_increasing_skip(self, monkeypatch):
        # Force a small page size so we can test pagination with a
        # max_events well below the configuration ceiling.
        monkeypatch.setattr(connector, "__FDA_PAGE_SIZE", 10)

        events = [_make_event(f"RPT-{i:04d}", "20260415") for i in range(25)]

        skips_seen = []

        def fake_fetch(session, search_query, limit, skip):
            skips_seen.append(skip)
            sorted_events = sorted(events, key=lambda e: (e["receivedate"], e["safetyreportid"]))
            return sorted_events[skip : skip + limit], len(sorted_events)

        monkeypatch.setattr(connector, "fetch_adverse_events", fake_fetch)

        # Call fetch_all_events directly to avoid validate_configuration
        # ceilings and exercise the pagination loop in isolation.
        results, total, cap_hit = connector.fetch_all_events(
            session=None, search_query="dummy", max_events=25
        )

        assert skips_seen[0] == 0, "First page must request skip=0"
        assert any(
            s > 0 for s in skips_seen
        ), f"Pagination never advanced past skip=0: {skips_seen}"
        assert len(results) == 25
        assert total == 25
        assert cap_hit is False
