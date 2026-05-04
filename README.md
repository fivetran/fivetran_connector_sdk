<p align="center">
  <a href="https://www.fivetran.com/">
    <img src="https://cdn.prod.website-files.com/6130fa1501794ed4d11867ba/63d9599008ad50523f8ce26a_logo.svg" alt="Fivetran">
  </a>
</p>

<p align="center">
  Fivetran Connector SDK allows Real-time, efficient data replication to your destination of choice.
</p>

<p align="center">
  <a href="https://github.com/fivetran/fivetran_connector_sdk/stargazers" target="_blank"><img src="https://img.shields.io/github/stars/fivetran/fivetran_connector_sdk?style=social&label=Star"></a>
  <a href="https://github.com/fivetran/fivetran_connector_sdk/blob/main/LICENSE" target="_blank"><img src="https://img.shields.io/badge/License-MIT-blue" alt="License"></a>
  <a href="https://pypi.org/project/fivetran-connector-sdk/" target="_blank"><img src="https://img.shields.io/pypi/v/fivetran-connector-sdk" alt="PyPI Release"></a>
  <a href="https://pepy.tech/project/fivetran-connector-sdk" target="_blank"><img src="https://static.pepy.tech/badge/fivetran-connector-sdk" alt="PyPI Downloads"></a>
<a href="https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md" target="_blank"><img src="https://img.shields.io/badge/Managed-Yes-green/" alt="Managed"></a>
</p>

# Overview

Explore practical examples and helpful resources for building custom data connectors with the Fivetran [Connector SDK](https://fivetran.com/docs/connectors/connector-sdk). Learn how to develop and deploy [custom data connectors](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) in Python, and extend Fivetran’s capabilities to fit your data integration needs.

You’ll also find tips on [using AI to help you code an SDK connector](https://github.com/fivetran/fivetran_connector_sdk/tree/main/all_things_ai/tutorials/README.md) quickly.

## Why Connector SDK?

Fivetran Connector SDK allows you to code a custom data connector using Python and deploy it as an extension of Fivetran. Fivetran automatically manages running Connector SDK connections on your scheduled frequency and manages the required compute resources, eliminating the need for a third-party provider.

Connector SDK provides native support for many Fivetran features and relies on existing Fivetran technology. It also eliminates timeout and data size limitations seen in AWS Lambda.

## Requirements

- Python version ≥3.10 and ≤3.13
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

See [Setup guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

Run the `.github/scripts/setup-hooks.sh` script from the root of the repository to set up pre-commit hooks. This ensures that your code is formatted correctly and passes all tests before you commit them.

## Examples

Explore working connector code examples for common Connector SDK use cases. These [examples](examples/README.md) help you understand core implementation patterns and quickly adapt them to your own connector.

## Community connectors

Explore ready-to-use full connectors that require only minimal changes to get started. These connectors are useful when you want a stronger starting point or want to adapt an existing implementation for your source. For the full list, see the [Community Connectors Catalog](https://github.com/fivetran/fivetran_csdk_connectors/blob/main/README.md).

## AI and Connector SDK

AI coding tools can significantly speed up Connector SDK development. Start with the [Readme](https://github.com/fivetran/fivetran_connector_sdk/blob/main/all_things_ai/tutorials/README.md) to learn how to configure AI tools for Connector SDK work, then use [agents.md](https://github.com/fivetran/fivetran_connector_sdk/blob/main/all_things_ai/ai_agents/AGENTS.md) in any IDE, API call, or AI conversation to develop Connector SDK solutions rapidly while following best practices. For examples, see [Connectors built using an AI agent](examples/README.md#ai-assisted-connector-examples).

## Issue

Found an issue? Submit the [issue](https://github.com/fivetran/fivetran_connector_sdk/issues) and get connected to a Fivetran developer.

## Fivetran platform features

- [schema_change](https://github.com/fivetran/fivetran_connector_sdk/blob/main/examples/common_patterns_for_connectors/schema_change/README.md) - This is an example that illustrates how a deployed Connector SDK connection uses Fivetran's native [data type changes](https://fivetran.com/docs/core-concepts#changingdatatype) to change data types in the destination if they are changed in the source data.


## Support

Learn how we [support Fivetran Connector SDK](https://fivetran.com/docs/connector-sdk#support).

## Contributing

We welcome contributions to the Fivetran Connector SDK repo.

This repository is open source and intended specifically for Connector SDK examples. We encourage the community to contribute by suggesting improvements, bug fixes, new examples, and additional use cases that expand and strengthen the collection.

### How to contribute

1. Click **Fork** on the GitHub repository page to create your own copy of the repo.
2. Make your changes in a new branch: `git checkout -b feature/your-example-name`
3. Add new connectors, fix bugs, improve documentation, or enhance existing features and commit your changes.
4. Ensure your code works correctly and follows our coding standards.
- [Python coding standards](https://github.com/fivetran/fivetran_connector_sdk/blob/main/PYTHON_CODING_STANDARDS.md)
- [Fivetran coding principles](https://github.com/fivetran/fivetran_connector_sdk/blob/main/FIVETRAN_CODING_PRINCIPLES.md)
5. Open a pull request with a clear description of your changes.
- If you're part of the AI Accelerate Google hackathon, please use the `accel Google hack 2025` tag
- If you're part of the Fivetran internal hackathon, please use the `hackathon` tag

### What we're looking for

- New connector examples for popular APIs and databases
- Bug fixes and performance improvements
- Documentation enhancements
- Code quality improvements
- New common patterns and utilities

### Getting started

Before contributing, please:
- Read through existing [examples](examples/README.md#quickstart-examples) to understand our coding patterns
- Run the setup script: `.github/scripts/setup-hooks.sh`
- Follow the [Python coding standards](https://github.com/fivetran/fivetran_connector_sdk/blob/main/PYTHON_CODING_STANDARDS.md)
- Test your connector thoroughly before submitting

We appreciate all contributions, whether they're small bug fixes or major new features.

## Additional considerations

We provide examples to help you effectively use Fivetran's Connector SDK. While we've tested the code provided in these examples, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples.

Note that API calls made by your Connector SDK connection may count towards your service’s API call allocation. Exceeding this limit could trigger rate limits, potentially impacting other uses of the source API.

It's important to choose the right design pattern for your target API. Using an inappropriate pattern may lead to data integrity issues. We recommend that you review all our examples carefully to select the one that best suits your target API. Keep in mind that some APIs may not support patterns for which we currently have examples.

As with other new connectors, SDK connectors have a [14-day trial period](https://fivetran.com/docs/getting-started/free-trials#newconnectorfreeuseperiod) during which your usage counts towards free [MAR](https://fivetran.com/docs/usage-based-pricing). After the 14-day trial period, your usage counts towards paid MAR. To avoid incurring charges, pause or delete any connections you created to run these examples before the trial ends.

## Maintenance

The `fivetran_connector_sdk` repository is actively maintained by Fivetran Developers. Reach out to our [Support team](https://support.fivetran.com/hc/en-us) for any inquiries.