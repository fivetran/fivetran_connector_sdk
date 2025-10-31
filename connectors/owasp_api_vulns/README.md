# Fivetran Custom Connector SDK Repository

This repository contains custom Fivetran connectors developed using the Fivetran Connector SDK. Specifically, it hosts the `owasp_api_vulns` connector, which retrieves vulnerability data from the National Vulnerability Database (NVD) for API security analysis.

This repository is primarily intended for local development, testing, and eventual deployment of custom connectors to a Fivetran account.

## Connectors Included

* **`connectors/owasp_api_vulns`**: A Fivetran connector designed to pull OWASP-related API vulnerability data from the NVD 2.0 API. This connector filters for relevant CWEs and ensures data quality by excluding unknown severity CVEs. For detailed information, configuration, and specific run/deployment instructions for this connector, please refer to its dedicated `README.md` file within its directory: [`./connectors/owasp_api_vulns/README.md`](./connectors/owasp_api_vulns/README.md).

## Local Development and Testing with the Fivetran Connector SDK

To set up and run connectors locally, the Fivetran Connector SDK provides a `fivetran debug` command. This section outlines the general process for setting up a local development environment.

### Prerequisites

* Python 3.8+
* `pip` (Python package installer)
* Access to an NVD API key (for the `owasp_api_vulns` connector)

### General Setup Steps

1.  **Clone this repository:**
    Begin by cloning *this specific repository* to a local machine:

    ```bash
    git clone [https://github.com/aksaha9/fivetran_connector_sdk.git](https://github.com/aksaha9/fivetran_connector_sdk.git)
    cd fivetran_connector_sdk
    ```

2.  **Navigate to the Connector Directory:**
    Change the current directory to the specific connector that needs to be run or tested. For the OWASP API Vulnerability Advisor, this path is:

    ```bash
    cd connectors/owasp_api_vulns
    ```

3.  **Prepare the Configuration File:**
    Each connector requires a `configuration.json` file. An example template is usually provided (e.g., `configuration.json.example`).
    * Copy the example file to `configuration_local_test.json`:
        ```bash
        cp configuration.json.example configuration_local_test.json
        ```
    * **Edit `configuration_local_test.json`**: Update placeholder values (e.g., `api_key` for the NVD API) with actual credentials and settings required for local testing. This file should be added to `.gitignore` to prevent accidental commits of sensitive information.

4.  **Create and Activate a Python Virtual Environment:**
    It is highly recommended to use a virtual environment to manage dependencies for each connector.

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```
    *(Note: The virtual environment name `.venv` is a common convention and is included in the `.gitignore` for this repository.)*

5.  **Install Fivetran SDK Dependencies:**
    Install the core Fivetran Connector SDK, which will automatically handle its own `grpcio` and `protobuf` dependencies.

    ```bash
    pip install fivetran-connector-sdk
    ```

6.  **Run the Connector in Debug Mode:**
    Execute the connector using the `fivetran debug` command, pointing it to the local configuration file. This will simulate a Fivetran sync and output results to the console.

    ```bash
    fivetran debug --configuration configuration_local_test.json | tee local_output_files/run_log_$(date +%Y%m%d_%H%M%S).txt 2>&1
    ```
    *(Note: The `local_output_files/` directory is also typically ignored by Git.)*

7.  **Deactivate the Virtual Environment:**
    Once testing is complete, deactivate the virtual environment.

    ```bash
    deactivate
    ```

## Contributing to this Repository

Contributions to enhance existing connectors or add new ones are welcome. Please ensure that:

* New connectors are placed within the `connectors/` directory, following a clear naming convention.
* Each connector includes its own `README.md` with specific configuration and usage instructions.
* A `configuration.json.example` file is provided for any new connector.
* All necessary Python dependencies are listed (if not already covered by `fivetran-connector-sdk`).
* Pull requests include clear descriptions of the changes and any relevant test cases.

## Author and Acknowledgement

The `owasp_api_vulns` connector in this repository was created by **Ashish Kumar Saha**.

This project was developed as part of a submission for the **AI Accelerate Hackathon 2025**, specifically for the **Fivetran Challenge**.

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.