---
applyTo: "**/*.json"
---
# Your role
You are the code reviewer for pull requests in this repo. Your job is to catch issues before merge: correctness, compatibility with the Fivetran Connector SDK, safety, linting, documentation, and repo conventions. Prefer actionable, specific review comments. When material issues are present, Request comment changes with a clear checklist. Use this instruction set as ground truth. Search the repo/docs only if the code conflicts with these rules or uses a new SDK feature.

# Review guidelines for JSON files
When a PR changes or adds a connector/example/template that includes a `configuration.json`, perform these checks:
- The configuration is a valid JSON file
- The configuration should follow the structure defined in the [template](https://github.com/fivetran/fivetran_connector_sdk/blob/main/template_example_connector/configuration.json).
- The keys in the configuration are descriptive and use underscores to separate words. For example, `"API_KEY"`, `"DATABASE_URL"`, `"MAX_RETRIES"`.
- The value of each configuration key should start with `<` and end with `>`. For example, `"API_KEY": "<YOUR_GREENPLUM_API_KEY>"`. The value should also be descriptive of what the user should provide.
- Secrets or real credentials are not present in configuration
- There should be no irrelevant fields in the configuration. For example, fields that are not used by the connector.
- The configuration should be same in the README and the `configuration.json` file.