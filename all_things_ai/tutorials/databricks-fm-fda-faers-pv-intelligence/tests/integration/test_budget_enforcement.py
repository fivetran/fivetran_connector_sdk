"""Lock the README contract for max_enrichments.

README statement:
  "max_enrichments (optional): Maximum events to run through debate
   (3 ai_query calls each). Default: 5. Maximum: 50."

Each "enrichment" is one event going through the 3-call debate (Safety +
Clinical + Consensus). The cap is on the number of *events* enriched, not
the raw ai_query() count. This test enforces both: events_enriched <=
max_enrichments AND total_ai_query_calls <= 3 * max_enrichments.

# see databricks-fm-noaa-weather-risk-intelligence/tests/integration/
# test_budget_enforcement.py for the analogous test on a different counting
# convention (NOAA caps total ai_query calls; FAERS caps enriched events).
"""

import pytest

import connector


def _run_update_counting_ai_calls(
    config,
    events_to_return,
    monkeypatch,
    captured_upserts,
    response_cycle,
):
    """Helper: run update() with mocked HTTP and return the list of
    ai_query call markers captured."""
    ai_calls = []
    response_iter = iter(response_cycle * 200)

    def fake_call_ai_query(session, configuration, prompt):
        ai_calls.append(prompt[:60])
        return next(response_iter)

    def fake_fetch_adverse_events(session, search_query, limit, skip):
        if skip > 0:
            return [], len(events_to_return)
        return events_to_return[:limit], len(events_to_return)

    monkeypatch.setattr(connector, "call_ai_query", fake_call_ai_query)
    monkeypatch.setattr(connector, "fetch_adverse_events", fake_fetch_adverse_events)

    connector.update(config, {})
    return ai_calls


@pytest.mark.parametrize("max_enrichments", [1, 2, 3, 5])
def test_max_enrichments_caps_debated_events(
    max_enrichments,
    base_config,
    faers_events_batch,
    captured_upserts,
    sample_safety_response,
    sample_clinical_response,
    sample_consensus_response,
    monkeypatch,
):
    """README contract: max_enrichments caps the number of events that get
    debated. Each event triggers up to 3 ai_query() calls."""
    config = dict(base_config)
    config["max_enrichments"] = str(max_enrichments)
    config["max_events"] = "10"
    config["enable_enrichment"] = "true"

    responses = [sample_safety_response, sample_clinical_response, sample_consensus_response]

    ai_calls = _run_update_counting_ai_calls(
        config,
        faers_events_batch,
        monkeypatch,
        captured_upserts,
        responses,
    )

    # Each event = up to 3 ai_query() calls (safety, clinical, consensus).
    assert len(ai_calls) <= 3 * max_enrichments, (
        f"Budget violated: max_enrichments={max_enrichments} permits at most "
        f"{3 * max_enrichments} ai_query calls but {len(ai_calls)} were made."
    )

    # Verify no more than max_enrichments distinct safety/clinical/consensus
    # records were upserted.
    safety_count = sum(
        1 for u in captured_upserts["upserts"] if u["table"] == "safety_assessments"
    )
    assert safety_count <= max_enrichments, (
        f"safety_assessments table got {safety_count} upserts, "
        f"exceeds max_enrichments={max_enrichments}"
    )


def test_enrichment_disabled_makes_zero_ai_query_calls(
    base_config,
    faers_events_batch,
    captured_upserts,
    sample_safety_response,
    monkeypatch,
):
    """When enable_enrichment=false, no ai_query() calls regardless of
    max_enrichments."""
    config = dict(base_config)
    config["enable_enrichment"] = "false"
    config["max_enrichments"] = "20"
    config["max_events"] = "10"

    ai_calls = _run_update_counting_ai_calls(
        config,
        faers_events_batch,
        monkeypatch,
        captured_upserts,
        [sample_safety_response],
    )

    assert len(ai_calls) == 0, (
        f"Expected zero ai_query() calls with enrichment disabled, " f"got {len(ai_calls)}"
    )

    # adverse_events should still be upserted in data-only mode.
    event_upserts = [u for u in captured_upserts["upserts"] if u["table"] == "adverse_events"]
    assert len(event_upserts) > 0, "Data-only mode should still upsert adverse_events"
