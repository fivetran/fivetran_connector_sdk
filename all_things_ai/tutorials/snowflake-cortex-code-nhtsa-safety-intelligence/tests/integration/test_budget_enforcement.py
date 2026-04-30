"""Budget enforcement tests — `max_enrichments` cap.

Hazard #2 (api-profiling.md) — partial: the per-sync cap on Cortex calls.

The NHTSA connector makes UP TO 2 Cortex Agent calls per sync:
  1. Discovery (Phase 2): analyze seed data, recommend vehicles
  2. Synthesis (Phase 3): cross-vehicle safety analysis

`max_enrichments` is meant to cap the total Cortex call count. But in the
current implementation, `enrichment_count` is local to `run_discovery_phase()`
and is never threaded into `run_synthesis_phase()`. Synthesis fires
unconditionally (gated only on `discovery_depth >= 2 and vehicles > 1`),
silently bypassing the budget.

These tests are EXPECTED TO FAIL RED against the current connector. The fix
threads `max_enrichments` into the discovery return value so the caller
can pre-check before invoking synthesis.
"""

import pytest

import connector


@pytest.fixture
def cortex_call_counter(monkeypatch, sample_discovery_response, sample_synthesis_response):
    """Replace call_cortex_agent with a counter that returns the right shape
    for whichever prompt it sees (discovery vs synthesis)."""
    calls = []

    def fake_call(configuration, prompt, cortex_session=None):
        calls.append(prompt)
        if "investigated multiple vehicles" in prompt or "AGGREGATE DATA" in prompt:
            return sample_synthesis_response
        return sample_discovery_response

    monkeypatch.setattr(connector, "call_cortex_agent", fake_call)
    return calls


@pytest.fixture
def stubbed_fetches(monkeypatch, recall_batch, complaint_batch, spec_batch):
    """Stub all NHTSA fetch helpers to return canned data."""
    monkeypatch.setattr(connector, "fetch_recalls", lambda *a, **kw: recall_batch)
    monkeypatch.setattr(connector, "fetch_complaints", lambda *a, **kw: complaint_batch)
    monkeypatch.setattr(connector, "fetch_vehicle_specs", lambda *a, **kw: spec_batch)


class TestMaxEnrichmentsBudget:
    """Total Cortex calls (discovery + synthesis) must be <= max_enrichments."""

    def test_max_enrichments_zero_skips_all_cortex_calls(
        self, base_config, captured_upserts, cortex_call_counter, stubbed_fetches
    ):
        """max_enrichments=0 means no Cortex calls at all. Discovery should
        bail out early (the connector already does this); synthesis must NOT
        run since there's nothing to synthesize."""
        base_config["max_enrichments"] = "0"
        connector.update(base_config, {})
        assert (
            len(cortex_call_counter) == 0
        ), f"max_enrichments=0 should permit zero Cortex calls, got {len(cortex_call_counter)}"

    def test_max_enrichments_one_skips_synthesis(
        self, base_config, captured_upserts, cortex_call_counter, stubbed_fetches
    ):
        """max_enrichments=1 means exactly ONE Cortex call (discovery only).
        Synthesis must be skipped to honor the budget. Pre-fix this test fails
        because synthesis runs unconditionally — the actual call count is 2."""
        base_config["max_enrichments"] = "1"
        connector.update(base_config, {})
        assert len(cortex_call_counter) == 1, (
            f"max_enrichments=1 should permit exactly 1 Cortex call (discovery), "
            f"got {len(cortex_call_counter)}. "
            f"Bug: synthesis runs without checking the enrichment budget."
        )

    def test_max_enrichments_two_allows_both_phases(
        self, base_config, captured_upserts, cortex_call_counter, stubbed_fetches
    ):
        """max_enrichments=2 is the natural maximum (discovery + synthesis).
        Both phases run."""
        base_config["max_enrichments"] = "2"
        connector.update(base_config, {})
        assert (
            len(cortex_call_counter) <= 2
        ), f"max_enrichments=2 caps at 2 calls, got {len(cortex_call_counter)}"

    @pytest.mark.parametrize("budget", [0, 1, 2, 5])
    def test_actual_calls_never_exceed_budget(
        self, budget, base_config, captured_upserts, cortex_call_counter, stubbed_fetches
    ):
        """Across realistic budget values, the cumulative Cortex call count
        stays at or below the configured max."""
        base_config["max_enrichments"] = str(budget)
        connector.update(base_config, {})
        assert (
            len(cortex_call_counter) <= budget
        ), f"max_enrichments={budget} exceeded — actual calls: {len(cortex_call_counter)}"


class TestMaxDiscoveriesCap:
    """`max_discoveries` caps the FAN-OUT (number of agent-recommended vehicles
    fetched), not the Cortex call count. The two budgets are independent. The
    Best Buy retrofit (#572) revealed that off-by-fan-out bugs are a real bug
    class even when the Cortex budget is honored."""

    def test_max_discoveries_caps_vehicle_fetches(
        self,
        base_config,
        captured_upserts,
        cortex_call_counter,
        monkeypatch,
        recall_batch,
        complaint_batch,
        spec_batch,
    ):
        """When the agent recommends 5 vehicles but max_discoveries=2, only
        the first 2 are fetched."""
        # Override discovery response to recommend more vehicles than the cap
        big_recommendation = {
            "top_components": [],
            "severity_score": 5,
            "crash_count": 0,
            "fire_count": 0,
            "recommended_vehicles": [
                {"make": f"Make{i}", "model": f"Model{i}", "year": "2022", "reason": "x"}
                for i in range(5)
            ],
            "analysis_summary": "x",
        }

        def fake_call(configuration, prompt, cortex_session=None):
            if "AGGREGATE DATA" in prompt:
                return {"fleet_safety_grade": "B", "executive_summary": "x"}
            return big_recommendation

        monkeypatch.setattr(connector, "call_cortex_agent", fake_call)

        recall_call_count = [0]

        def counting_recalls(session, make, model, year):
            recall_call_count[0] += 1
            return recall_batch

        monkeypatch.setattr(connector, "fetch_recalls", counting_recalls)
        monkeypatch.setattr(connector, "fetch_complaints", lambda *a, **kw: complaint_batch)
        monkeypatch.setattr(connector, "fetch_vehicle_specs", lambda *a, **kw: spec_batch)

        base_config["max_discoveries"] = "2"
        base_config["max_enrichments"] = "5"  # generous enough to allow synthesis
        connector.update(base_config, {})

        # 1 seed call + 2 discovered = 3 total fetch_recalls invocations
        assert recall_call_count[0] == 3, (
            f"max_discoveries=2 should cap discovered fetches at 2 (plus 1 seed = 3 total), "
            f"got {recall_call_count[0]}"
        )
