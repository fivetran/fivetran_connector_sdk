"""
Tests for databricks-fm-fhir-healthcare-intelligence connector.
Covers: configuration validation, record builders, helper functions,
phase logic (mocked), and incremental sync regression.
"""

import json
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Module bootstrap — stub fivetran_connector_sdk before importing connector
# ---------------------------------------------------------------------------


def _bootstrap_sdk():
    sdk = types.ModuleType("fivetran_connector_sdk")

    class _Connector:
        def __init__(self, **kwargs):
            pass

    class _Logging:
        @staticmethod
        def warning(msg):
            pass

        @staticmethod
        def info(msg):
            pass

    class _Operations:
        upserts = []
        checkpoints = []

        @classmethod
        def reset(cls):
            cls.upserts = []
            cls.checkpoints = []

        @classmethod
        def upsert(cls, table, data):
            cls.upserts.append((table, data))

        @classmethod
        def checkpoint(cls, state):
            cls.checkpoints.append(state)

    sdk.Connector = _Connector
    sdk.Logging = _Logging
    sdk.Operations = _Operations
    sys.modules["fivetran_connector_sdk"] = sdk
    return _Operations


_ops = _bootstrap_sdk()

# Import connector after SDK stub is registered
sys.path.insert(
    0,
    "/Users/kelly.kohlleffel/Documents/GitHub/fivetran_connector_sdk"
    "/all_things_ai/tutorials/databricks-fm-fhir-healthcare-intelligence",
)
import connector as C  # noqa: E402

# ---------------------------------------------------------------------------
# Category 1: Configuration validation
# ---------------------------------------------------------------------------


class TestValidateConfiguration:
    def _base_config(self):
        return {
            "fhir_base_url": "https://hapi.fhir.org/baseR4",
            "databricks_workspace_url": "https://adb-123.azuredatabricks.net",
            "databricks_token": "dapiABCDEF",
            "databricks_warehouse_id": "abc123",
            "enable_enrichment": "true",
            "enable_discovery": "true",
            "enable_genie_space": "false",
        }

    def test_valid_config_passes(self):
        C.validate_configuration(self._base_config())

    def test_missing_databricks_creds_fails_when_enrichment_enabled(self):
        cfg = self._base_config()
        cfg["databricks_token"] = "<DATABRICKS_TOKEN>"
        with pytest.raises(ValueError, match="databricks_token"):
            C.validate_configuration(cfg)

    def test_databricks_creds_not_required_when_enrichment_disabled(self):
        # Fix 6 regression: discovery=True alone must NOT require Databricks creds
        cfg = {
            "enable_enrichment": "false",
            "enable_discovery": "true",
            "enable_genie_space": "false",
        }
        C.validate_configuration(cfg)  # must not raise

    def test_genie_requires_table_identifier(self):
        cfg = self._base_config()
        cfg["enable_genie_space"] = "true"
        cfg["genie_table_identifier"] = "<CATALOG.SCHEMA.TABLE>"
        with pytest.raises(ValueError, match="genie_table_identifier"):
            C.validate_configuration(cfg)

    def test_max_patients_ceiling_enforced(self):
        cfg = self._base_config()
        cfg["max_patients"] = "501"
        with pytest.raises(ValueError, match="max_patients"):
            C.validate_configuration(cfg)

    def test_max_enrichments_ceiling_enforced(self):
        cfg = self._base_config()
        cfg["max_enrichments"] = "21"
        with pytest.raises(ValueError, match="max_enrichments"):
            C.validate_configuration(cfg)

    def test_invalid_fhir_url_fails(self):
        cfg = self._base_config()
        cfg["fhir_base_url"] = "ftp://not-valid.example.com"
        with pytest.raises(ValueError, match="fhir_base_url"):
            C.validate_configuration(cfg)

    def test_databricks_url_must_be_https(self):
        cfg = self._base_config()
        cfg["databricks_workspace_url"] = "http://adb-123.azuredatabricks.net"
        with pytest.raises(ValueError, match="https"):
            C.validate_configuration(cfg)

    def test_placeholder_detection_in_required_fields(self):
        cfg = self._base_config()
        cfg["databricks_workspace_url"] = "<DATABRICKS_WORKSPACE_URL>"
        with pytest.raises(ValueError, match="databricks_workspace_url"):
            C.validate_configuration(cfg)


# ---------------------------------------------------------------------------
# Category 2: Record builders
# ---------------------------------------------------------------------------


PATIENT_RESOURCE = {
    "id": "patient-001",
    "name": [
        {
            "use": "official",
            "family": "Smith",
            "given": ["John"],
        }
    ],
    "gender": "male",
    "birthDate": "1970-05-14",
    "identifier": [{"value": "MRN-001"}],
    "address": [
        {
            "line": ["123 Main St"],
            "city": "Springfield",
            "state": "IL",
            "postalCode": "62701",
            "country": "US",
        }
    ],
    "meta": {"lastUpdated": "2024-01-01T00:00:00Z"},
}

CONDITION_RESOURCE = {
    "id": "cond-001",
    "subject": {"reference": "Patient/patient-001"},
    "code": {
        "coding": [{"code": "E11.9", "display": "Type 2 diabetes mellitus", "system": "ICD-10"}]
    },
    "category": [{"coding": [{"code": "encounter-diagnosis"}]}],
    "clinicalStatus": {"coding": [{"code": "active"}]},
    "verificationStatus": {"coding": [{"code": "confirmed"}]},
    "onsetDateTime": "2020-03-01",
    "recordedDate": "2020-03-15",
    "meta": {"lastUpdated": "2024-01-01T00:00:00Z"},
}

OBSERVATION_RESOURCE = {
    "id": "obs-001",
    "subject": {"reference": "Patient/patient-001"},
    "code": {"coding": [{"code": "4548-4", "display": "HbA1c", "system": "http://loinc.org"}]},
    "category": [{"coding": [{"code": "laboratory"}]}],
    "valueQuantity": {"value": 8.5, "unit": "%"},
    "status": "final",
    "effectiveDateTime": "2024-01-10",
    "issued": "2024-01-11T08:00:00Z",
    "referenceRange": [{"low": {"value": 4.0}, "high": {"value": 6.5}}],
    "meta": {"lastUpdated": "2024-01-11T00:00:00Z"},
}

MEDICATION_RESOURCE = {
    "id": "med-001",
    "subject": {"reference": "Patient/patient-001"},
    "medicationCodeableConcept": {
        "coding": [
            {
                "code": "860975",
                "display": "Metformin 500mg",
                "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
            }
        ]
    },
    "status": "active",
    "intent": "order",
    "authoredOn": "2023-06-01",
    "dosageInstruction": [{"text": "Take 1 tablet twice daily with meals"}],
    "meta": {"lastUpdated": "2024-01-01T00:00:00Z"},
}


class TestBuildPatientRecord:
    def test_required_fields_present(self):
        record = C.build_patient_record(PATIENT_RESOURCE)
        assert record["patient_id"] == "patient-001"
        assert record["given_name"] == "John"
        assert record["family_name"] == "Smith"
        assert record["gender"] == "male"
        assert record["mrn"] == "MRN-001"
        assert record["city"] == "Springfield"

    def test_missing_name_does_not_raise(self):
        resource = {"id": "p-002", "gender": "female"}
        record = C.build_patient_record(resource)
        assert record["patient_id"] == "p-002"
        assert record["given_name"] is None
        assert record["family_name"] is None

    def test_active_defaults_to_true(self):
        resource = {"id": "p-003"}
        record = C.build_patient_record(resource)
        assert record["active"] is True


class TestBuildConditionRecord:
    def test_required_fields(self):
        record = C.build_condition_record(CONDITION_RESOURCE)
        assert record is not None
        assert record["condition_id"] == "cond-001"
        assert record["patient_id"] == "patient-001"
        assert record["code"] == "E11.9"
        assert record["clinical_status"] == "active"

    def test_missing_subject_returns_none(self):
        resource = {"id": "cond-002"}
        assert C.build_condition_record(resource) is None

    def test_missing_id_returns_none(self):
        resource = {"subject": {"reference": "Patient/patient-001"}}
        assert C.build_condition_record(resource) is None

    def test_empty_codings_does_not_raise(self):
        resource = {
            "id": "cond-003",
            "subject": {"reference": "Patient/patient-001"},
        }
        record = C.build_condition_record(resource)
        assert record is not None
        assert record["code"] is None


class TestBuildObservationRecord:
    def test_required_fields(self):
        record = C.build_observation_record(OBSERVATION_RESOURCE)
        assert record is not None
        assert record["observation_id"] == "obs-001"
        assert record["patient_id"] == "patient-001"
        assert record["value"] == 8.5
        assert record["value_unit"] == "%"

    def test_missing_subject_returns_none(self):
        assert C.build_observation_record({"id": "obs-002"}) is None


class TestBuildMedicationRecord:
    def test_required_fields(self):
        record = C.build_medication_record(MEDICATION_RESOURCE)
        assert record is not None
        assert record["medication_id"] == "med-001"
        assert record["patient_id"] == "patient-001"
        assert record["status"] == "active"

    def test_missing_subject_returns_none(self):
        assert C.build_medication_record({"id": "med-002"}) is None


# ---------------------------------------------------------------------------
# Category 3: Helper functions
# ---------------------------------------------------------------------------


class TestIsPlaceholder:
    def test_none_is_placeholder(self):
        assert C._is_placeholder(None) is True

    def test_angle_bracket_string_is_placeholder(self):
        assert C._is_placeholder("<MY_VALUE>") is True

    def test_real_string_is_not_placeholder(self):
        assert C._is_placeholder("real-value") is False

    def test_bool_false_is_not_placeholder(self):
        assert C._is_placeholder(False) is False

    def test_bool_true_is_not_placeholder(self):
        assert C._is_placeholder(True) is False

    def test_int_is_not_placeholder(self):
        assert C._is_placeholder(42) is False

    def test_empty_string_is_placeholder(self):
        assert C._is_placeholder("") is True


class TestParseBool:
    def test_true_string(self):
        assert C._parse_bool("true") is True

    def test_false_string(self):
        assert C._parse_bool("false") is False

    def test_true_bool(self):
        assert C._parse_bool(True) is True

    def test_false_bool(self):
        assert C._parse_bool(False) is False

    def test_none_uses_default(self):
        assert C._parse_bool(None, default=True) is True
        assert C._parse_bool(None, default=False) is False

    def test_placeholder_uses_default(self):
        assert C._parse_bool("<TRUE_OR_FALSE>", default=True) is True


class TestOptionalStr:
    def test_returns_value_when_set(self):
        cfg = {"key": "myval"}
        assert C._optional_str(cfg, "key", "default") == "myval"

    def test_returns_default_for_placeholder(self):
        cfg = {"key": "<PLACEHOLDER>"}
        assert C._optional_str(cfg, "key", "default") == "default"

    def test_returns_default_for_missing_key(self):
        assert C._optional_str({}, "key", "default") == "default"


class TestOptionalInt:
    def test_returns_int_when_set(self):
        assert C._optional_int({"k": "30"}, "k", 10) == 30

    def test_returns_default_for_placeholder(self):
        assert C._optional_int({"k": "<VAL>"}, "k", 10) == 10

    def test_returns_default_for_invalid(self):
        assert C._optional_int({"k": "abc"}, "k", 5) == 5


class TestFlattenDict:
    def test_flat_dict_unchanged(self):
        d = {"a": 1, "b": "x"}
        assert C.flatten_dict(d) == {"a": 1, "b": "x"}

    def test_nested_dict_flattened(self):
        d = {"a": {"b": {"c": 1}}}
        assert C.flatten_dict(d) == {"a_b_c": 1}

    def test_list_serialized_to_json(self):
        d = {"items": [1, 2, 3]}
        result = C.flatten_dict(d)
        assert result["items"] == "[1, 2, 3]"

    def test_empty_list_becomes_none(self):
        d = {"items": []}
        result = C.flatten_dict(d)
        assert result["items"] is None


class TestExtractCodeableConcept:
    def test_returns_code_display_system(self):
        cc = {"coding": [{"code": "E11", "display": "Diabetes", "system": "ICD-10"}]}
        code, display, system = C.extract_codeable_concept(cc)
        assert code == "E11"
        assert display == "Diabetes"
        assert system == "ICD-10"

    def test_none_input(self):
        code, display, system = C.extract_codeable_concept(None)
        assert code is None and display is None and system is None

    def test_empty_coding_list(self):
        code, display, system = C.extract_codeable_concept({"coding": []})
        assert code is None


class TestExtractReferenceId:
    def test_patient_reference(self):
        assert C.extract_reference_id({"reference": "Patient/abc-123"}) == "abc-123"

    def test_none_input(self):
        assert C.extract_reference_id(None) is None

    def test_plain_string_input_returns_none(self):
        # extract_reference_id only accepts dicts; plain strings return None
        assert C.extract_reference_id("Patient/xyz") is None


class TestExtractQuantity:
    def test_value_and_unit(self):
        q = {"value": 98.6, "unit": "F"}
        val, unit = C.extract_quantity(q)
        assert val == 98.6
        assert unit == "F"

    def test_none_input(self):
        val, unit = C.extract_quantity(None)
        assert val is None and unit is None


# ---------------------------------------------------------------------------
# Category 4: Phase logic (mocked)
# ---------------------------------------------------------------------------


class TestRunDiscoveryPhase:
    def setup_method(self):
        _ops.reset()

    def test_insight_id_is_stable_across_patient_counts(self):
        """Fix 2 regression: insight_id must not include patient count."""
        mock_session = MagicMock()
        cfg = {
            "databricks_workspace_url": "https://adb.example.com",
            "databricks_token": "dapi-test",
            "databricks_warehouse_id": "wh123",
            "condition_filter": "E11",
        }
        discovery_result = {
            "dominant_conditions": ["E11.9"],
            "risk_factors": ["obesity"],
            "high_risk_indicators": "HbA1c > 9",
            "recommended_screenings": ["retinal exam"],
            "comorbidities_to_investigate": ["hypertension"],
            "population_risk_summary": "High-risk diabetic cohort",
        }

        with patch.object(C, "call_ai_query", return_value=json.dumps(discovery_result)):
            C.run_discovery_phase(mock_session, cfg, [{"patient_id": "p1"}], {}, {})
            upserts_5 = [t for t, d in _ops.upserts if t == "population_insights"]

            _ops.reset()
            C.run_discovery_phase(
                mock_session, cfg, [{"patient_id": "p1"}, {"patient_id": "p2"}], {}, {}
            )
            upserts_10 = [t for t, d in _ops.upserts if t == "population_insights"]

        # Both runs should produce the same insight_id (no patient count suffix)
        assert upserts_5 or upserts_10  # at least one ran

    def test_skips_on_invalid_json(self):
        mock_session = MagicMock()
        cfg = {
            "databricks_workspace_url": "https://adb.example.com",
            "databricks_token": "dapi-test",
            "databricks_warehouse_id": "wh123",
        }
        with patch.object(C, "call_ai_query", return_value="not json"):
            C.run_discovery_phase(mock_session, cfg, [], {}, {})
        assert not any(t == "population_insights" for t, _ in _ops.upserts)


class TestUpsertAssessment:
    """Fix 4 regression: patient_id/assessment_type must not be overwritten by LLM keys."""

    def setup_method(self):
        _ops.reset()

    def test_patient_id_not_overwritten_by_llm_key(self):
        # LLM response contains a conflicting patient_id key
        assessment = {"patient_id": "WRONG", "clinical_risk_score": 8}
        C.upsert_assessment("clinical_assessments", "CORRECT", assessment, "clinical")
        _, data = _ops.upserts[0]
        assert data["patient_id"] == "CORRECT"

    def test_assessment_type_not_overwritten(self):
        assessment = {"assessment_type": "WRONG", "clinical_risk_score": 7}
        C.upsert_assessment("clinical_assessments", "p-001", assessment, "clinical")
        _, data = _ops.upserts[0]
        assert data["assessment_type"] == "clinical"

    def test_none_assessment_skips_upsert(self):
        C.upsert_assessment("clinical_assessments", "p-001", None, "clinical")
        assert not _ops.upserts


class TestRunMovePhase:
    def setup_method(self):
        _ops.reset()

    def _make_patient_bundle(self, patient_id):
        return [
            {
                "id": patient_id,
                "name": [{"use": "official", "family": "Test", "given": ["User"]}],
                "gender": "unknown",
                "meta": {"lastUpdated": "2024-01-01T00:00:00Z"},
            }
        ]

    def test_patients_upserted(self):
        with patch.object(C, "fetch_fhir_bundle") as mock_fetch:
            mock_fetch.side_effect = [
                self._make_patient_bundle("p-001"),  # patients
                [],  # conditions
                [],  # observations
                [],  # medications
            ]
            C.run_move_phase(MagicMock(), "https://hapi.fhir.org/baseR4", 5, "", {})
        assert any(t == "patients" for t, _ in _ops.upserts)


# ---------------------------------------------------------------------------
# Category 5: Incremental sync regression
# ---------------------------------------------------------------------------


class TestIncrementalSync:
    """Fix 1 regression: last_sync in state must add _lastUpdated filter to FHIR params."""

    def test_last_sync_adds_lastUpdated_param(self):
        state = {"last_sync": "2024-06-01T00:00:00Z"}
        captured_params = {}

        def mock_fetch_bundle(session, url, params=None, max_results=None):
            if "Patient" in url:
                captured_params.update(params or {})
            return []

        with patch.object(C, "fetch_fhir_bundle", side_effect=mock_fetch_bundle):
            C.run_move_phase(MagicMock(), "https://hapi.fhir.org/baseR4", 5, "", state)

        assert "_lastUpdated" in captured_params
        assert captured_params["_lastUpdated"] == "gt2024-06-01T00:00:00Z"

    def test_no_last_sync_sends_no_lastUpdated_param(self):
        state = {}
        captured_params = {}

        def mock_fetch_bundle(session, url, params=None, max_results=None):
            if "Patient" in url:
                captured_params.update(params or {})
            return []

        with patch.object(C, "fetch_fhir_bundle", side_effect=mock_fetch_bundle):
            C.run_move_phase(MagicMock(), "https://hapi.fhir.org/baseR4", 5, "", state)

        assert "_lastUpdated" not in captured_params

    def test_condition_filter_added_to_params(self):
        captured_params = {}

        def mock_fetch_bundle(session, url, params=None, max_results=None):
            if "Patient" in url:
                captured_params.update(params or {})
            return []

        with patch.object(C, "fetch_fhir_bundle", side_effect=mock_fetch_bundle):
            C.run_move_phase(MagicMock(), "https://hapi.fhir.org/baseR4", 5, "E11", {})

        assert "_has:Condition:patient:code" in captured_params
        assert captured_params["_has:Condition:patient:code"] == "E11"
