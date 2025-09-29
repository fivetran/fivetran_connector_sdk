# Fivetran Connector SDK
A public collection of Python examples, templates, and guides for building custom connectors using the Fivetran Connector SDK. It includes ready-to-run example connectors, best-practice patterns, and quickstart examples to help users understand the usage of Fivetran Connector SDK.

**Primary directories**
- `template_example_connector/` – canonical template: [connector.py](https://github.com/fivetran/fivetran_connector_sdk/blob/main/template_example_connector/connector.py), [configuration.json](https://github.com/fivetran/fivetran_connector_sdk/blob/main/template_example_connector/configuration.json), `requirements.txt`, [README_template.md](https://github.com/fivetran/fivetran_connector_sdk/blob/main/template_example_connector/README_template.md).
- `examples/` – quickstarts and pattern examples.
- `connectors/` – API and specific source examples.
- `fivetran_platform_features/schema_change/` – shows handling of schema/type changes.

**Runtime & tooling (assume unless PR updates it)**
- Python supported across 3.9–3.13 
- Linting: `flake8`.
- Python standard coding and formatting guidelines are followed

# How to validate PRs
When a PR changes or adds a connector/example/template, perform these checks:
- Each connector folder should include:
  - `connector.py` (imports from `fivetran_connector_sdk`),
  - `requirements.txt` (Required only when external dependencies are needed),
  - `configuration.json` (no secrets committed),
  - README (following the template present at `template_example_connector/README_template.md`).
- The `requirements.txt` should not include `fivetran_connector_sdk` or `requests`.
- Data operations use `op.upsert(...)`, `op.update(...)`, `op.delete(...)`, `op.checkpoint(...)` without `yield`.
- `flake8` clean; follow PEP 8 coding guidelines; clear docstrings on public helpers.
- Minimal, necessary dependencies; avoid heavyweight libraries for trivial tasks.

# When to search vs. trust this guide
Default to these instructions. Only search the repo/docs if the PR introduces new SDK features or contradicts the guidance (e.g., newly supported Python versions, new operations). If you find inconsistency (e.g., README vs. release notes), call it out and cite the newest official docs. The docs at https://fivetran.com/docs/connectors/connector-sdk are the source of truth.
