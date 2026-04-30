"""Unit tests for connector helper functions.

Covers flatten_dict (Fivetran-required nested-to-flat transform), the three
upsert_* helpers (raw API record → snake_case warehouse record), the two
build_*_prompt helpers (Cortex Agent prompt construction), and _build_aggregates.
"""

import json

import connector


class TestFlattenDict:
    def test_flat_unchanged(self):
        assert connector.flatten_dict({"a": 1}) == {"a": 1}

    def test_nested_underscore(self):
        assert connector.flatten_dict({"a": {"b": 1}}) == {"a_b": 1}

    def test_deep_nesting(self):
        assert connector.flatten_dict({"a": {"b": {"c": 1}}}) == {"a_b_c": 1}

    def test_list_serialized_to_json(self):
        result = connector.flatten_dict({"items": [1, 2, 3]})
        assert json.loads(result["items"]) == [1, 2, 3]

    def test_empty_list_becomes_none(self):
        assert connector.flatten_dict({"items": []}) == {"items": None}

    def test_tuple_serialized_to_json(self):
        result = connector.flatten_dict({"pair": (1, 2)})
        assert json.loads(result["pair"]) == [1, 2]

    def test_custom_separator(self):
        assert connector.flatten_dict({"a": {"b": 1}}, sep=".") == {"a.b": 1}

    def test_preserves_none_values(self):
        assert connector.flatten_dict({"a": None}) == {"a": None}


class TestUpsertRecalls:
    """upsert_recalls: NHTSA Recalls API uses CAPITALIZED keys
    (NHTSACampaignNumber, Component, Summary, etc.). Records without a
    campaign number must be skipped, not silently emitted with NULL primary key."""

    def test_emits_one_upsert_per_record(self, recall_batch, captured_upserts):
        count = connector.upsert_recalls(recall_batch, "Toyota", "Tundra", "2022", "seed")
        assert count == 5
        assert len(captured_upserts["upserts"]) == 5
        assert all(u["table"] == "recalls" for u in captured_upserts["upserts"])

    def test_skips_record_without_campaign_number(self, captured_upserts):
        records = [
            {"NHTSACampaignNumber": "22V001000", "Component": "Brakes"},
            {"Component": "Engine"},  # missing campaign number
            {"NHTSACampaignNumber": "22V002000", "Component": "Steering"},
        ]
        count = connector.upsert_recalls(records, "Toyota", "Tundra", "2022", "seed")
        assert count == 2
        assert len(captured_upserts["upserts"]) == 2
        emitted_ids = [u["data"]["nhtsa_campaign_number"] for u in captured_upserts["upserts"]]
        assert emitted_ids == ["22V001000", "22V002000"]

    def test_normalizes_to_snake_case(self, captured_upserts):
        connector.upsert_recalls(
            [{"NHTSACampaignNumber": "22V001000", "Component": "Brakes", "Summary": "x"}],
            "Toyota",
            "Tundra",
            "2022",
            "seed",
        )
        record = captured_upserts["upserts"][0]["data"]
        assert "nhtsa_campaign_number" in record
        assert record["component"] == "Brakes"
        assert record["summary"] == "x"
        # Source label is set
        assert record["source"] == "seed"

    def test_falls_back_to_arg_when_record_missing_make(self, captured_upserts):
        """If API record omits Make/Model/ModelYear, fall back to the function arg."""
        connector.upsert_recalls(
            [{"NHTSACampaignNumber": "22V001000"}],
            "Toyota",
            "Tundra",
            "2022",
            "seed",
        )
        record = captured_upserts["upserts"][0]["data"]
        assert record["make"] == "Toyota"
        assert record["model"] == "Tundra"
        assert record["model_year"] == "2022"


class TestUpsertComplaints:
    """upsert_complaints: complaints API uses LOWERCASED keys (odiNumber,
    crash, fire, components — note plural). Skip records without odiNumber."""

    def test_emits_one_upsert_per_record(self, complaint_batch, captured_upserts):
        count = connector.upsert_complaints(complaint_batch, "seed")
        assert count == 5

    def test_skips_record_without_odi_number(self, captured_upserts):
        records = [
            {"odiNumber": "10000001", "summary": "x"},
            {"summary": "y"},  # missing odiNumber
        ]
        count = connector.upsert_complaints(records, "seed")
        assert count == 1

    def test_normalizes_camelCase_to_snake_case(self, captured_upserts):
        connector.upsert_complaints(
            [{"odiNumber": "10000001", "numberOfInjuries": 2, "dateOfIncident": "20220101"}],
            "seed",
        )
        record = captured_upserts["upserts"][0]["data"]
        assert record["odi_number"] == "10000001"
        assert record["number_of_injuries"] == 2
        assert record["date_of_incident"] == "20220101"


class TestUpsertVehicleSpecs:
    """upsert_vehicle_specs: vPIC API uses Make_ID / Model_ID / Make_Name /
    Model_Name. Skip records missing either ID since they form the composite PK."""

    def test_emits_one_upsert_per_record(self, spec_batch, captured_upserts):
        count = connector.upsert_vehicle_specs(spec_batch, "seed")
        assert count == 5

    def test_skips_record_without_make_id(self, captured_upserts):
        records = [
            {"Make_ID": 448, "Model_ID": 1000},
            {"Model_ID": 1001},  # missing Make_ID
            {"Make_ID": 448},  # missing Model_ID
        ]
        count = connector.upsert_vehicle_specs(records, "seed")
        assert count == 1


class TestBuildAggregates:
    def test_returns_all_keys(self):
        agg = connector._build_aggregates({"a": 1}, {"b": 2}, 3, 4, 5, 6)
        assert agg == {
            "recall_components": {"a": 1},
            "complaint_components": {"b": 2},
            "crash_count": 3,
            "fire_count": 4,
            "total_recalls": 5,
            "total_complaints": 6,
        }


class TestBuildDiscoveryPrompt:
    def test_includes_seed_vehicle(self, recall_batch, complaint_batch):
        prompt = connector.build_discovery_prompt(
            "Toyota", "Tundra", "2022", recall_batch, complaint_batch
        )
        assert "Toyota Tundra 2022" in prompt
        assert "Total recalls: 5" in prompt
        assert "Total complaints: 5" in prompt

    def test_includes_top_components(self, recall_batch, complaint_batch):
        prompt = connector.build_discovery_prompt(
            "Toyota", "Tundra", "2022", recall_batch, complaint_batch
        )
        assert "TOP RECALL COMPONENTS:" in prompt
        # Recall batch alternates "Brakes" / "Engine"
        assert "Brakes" in prompt
        assert "Engine" in prompt

    def test_returns_only_json_instruction(self, recall_batch, complaint_batch):
        """The prompt should ask for JSON-only output (no markdown fences)."""
        prompt = connector.build_discovery_prompt(
            "Toyota", "Tundra", "2022", recall_batch, complaint_batch
        )
        assert "Return ONLY valid JSON" in prompt


class TestBuildSynthesisPrompt:
    def test_includes_all_vehicles(self):
        agg = connector._build_aggregates({"Brakes": 5}, {}, 0, 0, 10, 20)
        prompt = connector.build_synthesis_prompt(
            [("Toyota", "Tundra", "2022"), ("Ford", "F-150", "2022")], agg
        )
        assert "Toyota Tundra 2022" in prompt
        assert "Ford F-150 2022" in prompt

    def test_uses_aggregate_totals(self):
        agg = connector._build_aggregates({}, {}, 7, 3, 100, 200)
        prompt = connector.build_synthesis_prompt([("Toyota", "Tundra", "2022")], agg)
        assert "Total recalls across all vehicles: 100" in prompt
        assert "Total complaints across all vehicles: 200" in prompt
        assert "Total crash incidents: 7" in prompt
        assert "Total fire incidents: 3" in prompt
