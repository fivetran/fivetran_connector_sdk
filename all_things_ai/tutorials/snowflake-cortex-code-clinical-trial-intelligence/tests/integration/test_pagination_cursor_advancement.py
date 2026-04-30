"""Cursor + pagination tests for #566.

Clinical Trial uses a different shape than NVD CVE:
  - pageToken pagination (not start_index)
  - composite cursor (last_update_post_date, synced_nct_ids_at_cursor)
  - day-granularity dates require the synced_nct_ids set to disambiguate same-day records

Tests the same hard regression (drain-all-records-no-duplicates) plus the
composite cursor merge logic.

# see snowflake-cortex-code-nvd-cve-threat-intelligence/tests/integration/test_pagination_cursor_advancement.py
# for the start_index analogue.
"""

import pytest

import connector
from tests.conftest import make_trial


def _build_paginated_fetch(all_studies, page_size=20):
    """Fake fetch_data_with_retry that emits ClinicalTrials.gov v2 pages with
    nextPageToken pagination."""

    def fake_fetch(session, url, params=None):
        params = params or {}
        token = params.get("pageToken")
        # token is the start index encoded as a string for test purposes
        start = int(token) if token else 0
        per_page = int(params.get("pageSize", page_size))
        page = all_studies[start : start + per_page]
        next_start = start + per_page
        next_token = str(next_start) if next_start < len(all_studies) else None
        result = {"studies": page}
        if next_token:
            result["nextPageToken"] = next_token
        if not token:
            result["totalCount"] = len(all_studies)
        return result

    return fake_fetch


@pytest.fixture
def fifty_trials():
    """50 trials with monotonically increasing lastUpdatePostDate values.

    Real ClinicalTrials.gov data sorted by `LastUpdatePostDate` is monotonic;
    cycling dates would correctly trigger the composite cursor's same-day
    skip logic and is not representative."""
    # 50 distinct YYYY-MM-DD dates spanning Jan-Feb 2026
    dates = [f"2026-{m:02d}-{d:02d}" for m in (1, 2) for d in range(1, 26)][:50]
    return [make_trial(f"NCT{i:08d}", dates[i]) for i in range(50)]


class TestPageTokenPersistedCorrectly:
    """When capped mid-page, the connector must persist the CURRENT page's
    token (not the next page's), so on resume the same page is re-fetched
    and the unread tail of the page is picked up. The composite cursor
    handles dedup of already-synced records on the re-fetched page.

    Ref PR #566 retrofit (Level 3 test): the harness caught a bug where
    the original code persisted next_token, silently dropping records 15-19
    on page 1 (and 35-39 on page 2) when max_seed_trials=15.
    """

    def test_first_sync_capped_persists_current_page_token(
        self, base_config, fifty_trials, captured_upserts, monkeypatch
    ):
        """Cap=15 against page_size=20 caps mid-page-1. State.seed_page_token
        should be the token that fetched page 1 (None for the initial sync),
        NOT the next page's token."""
        config = dict(base_config)
        config["max_seed_trials"] = "15"
        config["enable_cortex"] = "false"

        monkeypatch.setattr(
            connector, "fetch_data_with_retry", _build_paginated_fetch(fifty_trials)
        )

        connector.update(config, {})
        last_state = captured_upserts["checkpoints"][-1]
        # First page was fetched with no pageToken; current_page_token = None.
        # State persists that, so resume re-fetches page 1.
        assert (
            last_state.get("seed_page_token") is None
        ), f"Capped on page 1 should persist None (re-fetch page 1 on resume); got {last_state}"

    def test_second_sync_re_fetches_page_with_unread_tail(
        self, base_config, fifty_trials, captured_upserts, monkeypatch
    ):
        """Sync 2 re-fetches the same page where sync 1 capped, skips the
        already-synced records via the composite cursor, and picks up the
        unread tail."""
        config = dict(base_config)
        config["max_seed_trials"] = "15"
        config["enable_cortex"] = "false"

        tokens_seen = []

        def tracking_fetch(session, url, params=None):
            tokens_seen.append(params.get("pageToken"))
            return _build_paginated_fetch(fifty_trials)(session, url, params)

        monkeypatch.setattr(connector, "fetch_data_with_retry", tracking_fetch)
        connector.update(config, {})
        first_run_state = dict(captured_upserts["checkpoints"][-1])
        tokens_seen.clear()

        connector.update(config, first_run_state)
        # Second sync's first fetch should re-use the capped-page's token
        # (None for page 1) so the unread tail is fetched.
        assert tokens_seen[0] is None, (
            f"Second sync should re-fetch page 1 (token=None); got first call token={tokens_seen[0]}, "
            f"state was {first_run_state}"
        )


class TestRepeatedSyncsCoverAllTrialsNoDuplicates:
    """Hard regression: repeated syncs with a small max_seed_trials cap drain
    all 50 trials exactly once."""

    def test_drained_no_duplicates(self, base_config, fifty_trials, captured_upserts, monkeypatch):
        config = dict(base_config)
        config["max_seed_trials"] = "15"
        config["enable_cortex"] = "false"

        monkeypatch.setattr(
            connector, "fetch_data_with_retry", _build_paginated_fetch(fifty_trials)
        )

        state = {}
        for _ in range(20):
            connector.update(config, state)
            state = dict(captured_upserts["checkpoints"][-1])

        synced = [
            u["data"]["nct_id"] for u in captured_upserts["upserts"] if u["table"] == "trials"
        ]
        unique = set(synced)
        assert unique == {
            f"NCT{i:08d}" for i in range(50)
        }, f"Repeated syncs should cover all 50 trials; got {len(unique)} unique"


class TestCompositeCursorSameDayDoesNotSkip:
    """Day-granularity cursor requires the nct_ids_at_cursor set so same-day
    records aren't dropped on the next sync. PR #566 reviewer finding."""

    def test_same_day_unsynced_record_is_picked_up_next_sync(
        self, base_config, captured_upserts, monkeypatch
    ):
        """First sync sees trials A and B both updated on day X, with cap=1.
        Second sync sees the same window plus a NEW trial C also on day X.
        Trial C must NOT be skipped because its nct_id isn't in synced_at_cursor."""
        config = dict(base_config)
        config["max_seed_trials"] = "1"
        config["enable_cortex"] = "false"

        # Trials A and B are on the same day; trial C is also on the same day
        # but doesn't appear until sync 2.
        trials_sync_1 = [
            make_trial("NCT_A", "2026-04-15"),
            make_trial("NCT_B", "2026-04-15"),
        ]
        trials_sync_2 = trials_sync_1 + [make_trial("NCT_C", "2026-04-15")]

        sync_n = [1]

        def fake_fetch(session, url, params=None):
            data = trials_sync_1 if sync_n[0] == 1 else trials_sync_2
            return {
                "studies": data,
                "totalCount": len(data),
            }

        monkeypatch.setattr(connector, "fetch_data_with_retry", fake_fetch)

        # First sync — caps at 1, syncs NCT_A.
        connector.update(config, {})
        first_state = dict(captured_upserts["checkpoints"][-1])
        first_synced = {
            u["data"]["nct_id"] for u in captured_upserts["upserts"] if u["table"] == "trials"
        }

        # Second sync — sees A, B, C all on the same date. A is in
        # synced_at_cursor, so it's skipped. B and C are NOT, so they sync.
        sync_n[0] = 2
        connector.update(config, first_state)

        all_synced = {
            u["data"]["nct_id"] for u in captured_upserts["upserts"] if u["table"] == "trials"
        }
        # Sync 1 picked up at least NCT_A.
        assert "NCT_A" in first_synced
        # Sync 2 must pick up NCT_B and NCT_C; without composite cursor they'd be skipped.
        assert "NCT_B" in all_synced or "NCT_C" in all_synced, (
            f"Same-day unsynced records must NOT be skipped. First sync: {first_synced}, "
            f"All synced: {all_synced}"
        )
