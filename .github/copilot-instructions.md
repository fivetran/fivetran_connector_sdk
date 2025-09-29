# Fivetran Connector SDK
A public collection of Python examples, templates, and guides for building custom connectors using the Fivetran Connector SDK. It includes ready-to-run example connectors, best-practice patterns, and quickstart examples to help users understand the usage of Fivetran Connector SDK.

**Primary directories**
- `template_example_connector/` – canonical template: [connector.py](https://github.com/fivetran/fivetran_connector_sdk/blob/main/template_example_connector/connector.py), [configuration.json](https://github.com/fivetran/fivetran_connector_sdk/blob/main/template_example_connector/configuration.json), `requirements.txt`, `README_template.md`.
- `examples/` – quickstarts and pattern examples.
- `connectors/` – API and specific source examples.
- `fivetran_platform_features/schema_change/` – shows handling of schema/type changes.

**Runtime & tooling (assume unless PR updates it)**
- Python supported across 3.9–3.13 
- Linting: `flake8` via repo config.
- Python standard coding and formatting guidelines are followed

# How to validate PRs
When a PR changes or adds a connector/example/template, perform these checks:
- Each connector folder should include:
  - `connector.py` (imports from `fivetran_connector_sdk`),
  - `requirements.txt` (minimal + compatible),
  - `configuration.json` (no secrets committed),
  - README (following the template present at `template_example_connector/README_template.md`).
- Data operations use `op.upsert(...)`, `op.update(...)`, `op.delete(...)`, `op.checkpoint(...)` without `yield`.
- Imports: `from fivetran_connector_sdk import Connector, Logging as log, Operations as op`.
- No secrets in repo (including `configuration.json`, examples, READMEs, comments).
- Logging: never log credentials, tokens, PII; use `log` facilities appropriately.
- `flake8` clean; follow PEP 8; clear docstrings on public helpers.
- Minimal, necessary dependencies; avoid heavyweight libs for trivial tasks; pin only when required.
- The `requirements.txt` should not include `fivetran_connector_sdk` or `requests`.

# Review rubric & common red flags
**Blockers**
- Uses old generator pattern (`yield op.upsert(...)`) in new/updated code.
- Secrets or real credentials present in code, configs, or docs.
- Failing CI/lint; missing required files (`connector.py`, `requirements.txt`, `configuration.json`).
- Logging secrets/PII; broad `except:` swallowing errors.

**Major concerns**
- Over-broad dependencies; incompatible Python version claims; platform-specific code without guards.
- README/config not updated for new connector; unclear run instructions.

**Nits**
- Minor style issues flake8 would catch; naming clarity; missing docstrings on helpers.

# When to search vs. trust this guide
Default to these instructions. Only search the repo/docs if the PR introduces new SDK features or contradicts the guidance (e.g., newly supported Python versions, new operations). If you find inconsistency (e.g., README vs. release notes), call it out and cite the newest official docs. The docs at https://fivetran.com/docs/connectors/connector-sdk are the source of truth.
