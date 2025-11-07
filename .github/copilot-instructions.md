# Fivetran Connector SDK
A public collection of Python examples, templates, and guides for building custom connectors using the Fivetran Connector SDK. It includes ready-to-run example connectors, best-practice patterns, and quickstart examples to help users understand the usage of Fivetran Connector SDK.

**Primary directories**
- `template_example_connector/` – canonical template: [connector.py](https://github.com/fivetran/fivetran_connector_sdk/blob/main/template_example_connector/connector.py), [configuration.json](https://github.com/fivetran/fivetran_connector_sdk/blob/main/template_example_connector/configuration.json), `requirements.txt`, [README_template.md](https://github.com/fivetran/fivetran_connector_sdk/blob/main/template_example_connector/README_template.md).
- `examples/` – quickstarts and pattern examples.
- `connectors/` – API and specific source examples.
- `fivetran_platform_features/schema_change/` – shows handling of schema/type changes.

**Runtime & tooling (assume unless PR updates it)**
- Python supported across 3.10–3.13
- Linting: `flake8` with PEP 8 compliance.
- Python standard coding and formatting guidelines are followed (snake_case for functions/variables, PascalCase for classes)

# How to validate PRs
When a PR changes or adds a connector/example/template, perform these checks:

## Structure and Files (BLOCKER if missing)
- Each connector folder **must** include:
  - `connector.py` with imports from `fivetran_connector_sdk` (Connector, Operations, Logging)
- Each connector folder can include:
  - `configuration.json` with **no real secrets or credentials** (use placeholders like `<YOUR_API_KEY>`)
  - README following [README_template.md](https://github.com/fivetran/fivetran_connector_sdk/blob/main/template_example_connector/README_template.md)
  - `requirements.txt` only when external dependencies are needed (exclude `fivetran_connector_sdk` and `requests`)

## Code Quality (Request changes if violated)
- `flake8` clean with PEP 8 compliance
- Cognitive complexity < 15 per function (split into helpers if exceeded)
- Clear docstrings for all public functions following template format
- Minimal dependencies; avoid heavyweight libraries for simple tasks

# Review Response Format
When requesting changes, provide:
1. **Severity**: BLOCKER (must fix) or REQUEST_CHANGES (should fix)
2. **Issue**: Clear description of the problem
3. **Location**: File and line number or function name
4. **Fix**: Specific actionable guidance with code example if applicable
5. **Reference**: Link to docs or template if relevant

# When to search vs. trust this guide
Default to these instructions. Only search the repo/docs if the PR introduces new SDK features or contradicts the guidance (e.g., newly supported Python versions, new operations). If you find inconsistency (e.g., README vs. release notes), call it out and cite the newest official docs. The docs at https://fivetran.com/docs/connectors/connector-sdk are the source of truth.
