"""Lock the README contract for max_debates.

Each enrichment = 3 Cortex calls (Optimist + Skeptic + Consensus). The cap
is on the number of *trials* debated, not the raw Cortex call count.

# see snowflake-cortex-code-nvd-cve-threat-intelligence/tests/integration/test_budget_enforcement.py
# for the analogous Cortex-side test.
"""

import pytest

import connector


@pytest.mark.parametrize("max_debates", [1, 2, 3, 5])
def test_max_debates_caps_debated_trials(
    max_debates,
    base_config,
    trial_batch,
    captured_upserts,
    sample_optimist_response,
    sample_skeptic_response,
    sample_consensus_response,
    monkeypatch,
):
    """README contract: max_debates caps the trials debated."""
    config = dict(base_config)
    config["max_debates"] = str(max_debates)
    config["max_seed_trials"] = "10"

    cortex_calls = []
    response_iter = iter(
        [sample_optimist_response, sample_skeptic_response, sample_consensus_response] * 200
    )

    def fake_call_cortex(configuration, prompt, cortex_session=None):
        cortex_calls.append(prompt[:60])
        try:
            return next(response_iter)
        except StopIteration:
            return None

    # Skip the discovery phase: it makes its own Cortex call before debate.
    def fake_run_discovery(session, configuration, seed_records, state):
        return [], None

    # Fetch returns trial_batch as raw studies for the seed phase
    def fake_fetch_seed(session, configuration, state):
        records = {}
        for trial in trial_batch[: int(config["max_seed_trials"])]:
            record = connector.build_trial_record(trial)
            records[record["nct_id"]] = record
        return records

    monkeypatch.setattr(connector, "call_cortex_agent", fake_call_cortex)
    monkeypatch.setattr(connector, "fetch_and_upsert_seed_trials", fake_fetch_seed)
    monkeypatch.setattr(connector, "run_discovery_phase", fake_run_discovery)

    connector.update(config, {})

    # Each debated trial = up to 3 Cortex calls (optimist, skeptic, consensus).
    assert len(cortex_calls) <= 3 * max_debates, (
        f"Budget violated: max_debates={max_debates} permits at most "
        f"{3 * max_debates} Cortex calls but {len(cortex_calls)} were made."
    )

    optimist_count = sum(
        1 for u in captured_upserts["upserts"] if u["table"] == "optimist_assessments"
    )
    assert optimist_count <= max_debates


def test_cortex_disabled_makes_zero_cortex_calls(
    base_config, trial_batch, captured_upserts, monkeypatch
):
    config = dict(base_config)
    config["enable_cortex"] = "false"
    config["max_debates"] = "20"
    config["max_seed_trials"] = "10"

    cortex_calls = []

    def fake_call_cortex(*a, **kw):
        cortex_calls.append(a)
        return None

    def fake_fetch_seed(session, configuration, state):
        records = {}
        for trial in trial_batch:
            record = connector.build_trial_record(trial)
            records[record["nct_id"]] = record
        return records

    monkeypatch.setattr(connector, "call_cortex_agent", fake_call_cortex)
    monkeypatch.setattr(connector, "fetch_and_upsert_seed_trials", fake_fetch_seed)

    connector.update(config, {})

    assert len(cortex_calls) == 0, "enable_cortex=false must not call Cortex"
