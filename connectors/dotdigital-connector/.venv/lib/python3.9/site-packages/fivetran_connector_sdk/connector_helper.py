import os
import re
import ast
import json
import sys
import time
import socket
import platform
import subprocess
import requests as rq

from http import HTTPStatus
from typing import Optional, Tuple
from zipfile import ZipFile, ZIP_DEFLATED

from fivetran_connector_sdk.protos import common_pb2

from fivetran_connector_sdk import constants
from fivetran_connector_sdk.logger import Logging
from fivetran_connector_sdk.helpers import (
    print_library_log, get_renamed_table_name,
    get_renamed_column_name, get_input_from_cli,
    validate_and_load_state,
    validate_and_load_configuration
)
from fivetran_connector_sdk.constants import (
    OS_MAP,
    ARCH_MAP,
    WIN_OS,
    X64,
    TESTER_FILENAME,
    UPLOAD_FILENAME,
    LAST_VERSION_CHECK_FILE,
    ROOT_LOCATION,
    CONFIG_FILE,
    OUTPUT_FILES_DIR,
    REQUIREMENTS_TXT,
    PYPI_PACKAGE_DETAILS_URL,
    ONE_DAY_IN_SEC,
    MAX_RETRIES,
    VIRTUAL_ENV_CONFIG,
    ROOT_FILENAME,
    EXCLUDED_DIRS,
    EXCLUDED_PIPREQS_DIRS,
    INSTALLATION_SCRIPT,
    INSTALLATION_SCRIPT_MISSING_MESSAGE,
    DRIVERS,
    UTF_8,
    CONNECTION_SCHEMA_NAME_PATTERN,
    TABLES,
)

def get_destination_group(args):
    ft_group = args.destination if args.destination else None
    env_destination_name = os.getenv('FIVETRAN_DESTINATION_NAME', None)
    if not ft_group:
        ft_group = get_input_from_cli("Provide the destination name (as displayed in your dashboard destination list)", env_destination_name)
    return ft_group

def get_connection_name(args, retrying=0):
    ft_connection = args.connection if args.connection else None
    env_connection_name = os.getenv('FIVETRAN_CONNECTION_NAME', None)
    if not ft_connection:
        ft_connection = get_input_from_cli("Provide the connection name", env_connection_name)
    if not is_connection_name_valid(ft_connection):
        print_library_log(
            f"Connection name: {ft_connection} is invalid!\n The connection name should start with an "
            f"underscore or a lowercase letter (a-z), followed by any combination of underscores, lowercase "
            f"letters, or digits (0-9). Uppercase characters are not allowed.", Logging.Level.SEVERE)
        args.connection = None
        if retrying >= MAX_RETRIES or args.force:
            sys.exit(1)
        else:
            print_library_log("Please retry...", Logging.Level.INFO)
            return get_connection_name(args, retrying + 1)
    return ft_connection

def get_api_key(args):
    ft_deploy_key = args.api_key if args.api_key else None
    env_api_key = os.getenv('FIVETRAN_API_KEY', None)
    if not ft_deploy_key:
        ft_deploy_key = get_input_from_cli("Provide your API Key (Base 64 Encoded)", env_api_key, True)
    return ft_deploy_key

def get_python_version(args):
    python_version = args.python_version if args.python_version else None
    env_python_version = os.getenv('FIVETRAN_PYTHON_VERSION', None)
    if env_python_version and not python_version and not args.force:
        python_version = get_input_from_cli("Provide your python version", env_python_version)
    return python_version

def get_hd_agent_id(args):
    hd_agent_id = args.hybrid_deployment_agent_id if args.hybrid_deployment_agent_id else None
    env_hd_agent_id = os.getenv('FIVETRAN_HD_AGENT_ID', None)

    if env_hd_agent_id and not hd_agent_id and not args.force:
        hd_agent_id = get_input_from_cli("Provide the Hybrid Deployment Agent ID", env_hd_agent_id)
    return hd_agent_id

def get_state(args):
    if args.command.lower() == "deploy" and args.state:
        print_library_log("Unrecognised argument for deploy: --state."
                          "'state' is not set using the 'deploy' command. You can manage the state for deployed connections via "
                          "Fivetran API, https://fivetran.com/docs/connector-sdk/working-with-connector-sdk#workingwithstatejsonfile",
                          Logging.Level.WARNING)
        sys.exit(1)
    state = args.state if args.state else os.getenv('FIVETRAN_STATE', None)
    state = validate_and_load_state(args, state)
    return state

def get_configuration(args, retrying = 0):
    configuration = args.configuration if args.configuration else None
    env_configuration = os.getenv('FIVETRAN_CONFIGURATION', None)
    try:
        if not configuration and not args.force and args.command.lower() == "deploy":
            confirm = 'y'
            if not retrying:
                json_filepath = os.path.join(args.project_path, "configuration.json")
                if os.path.exists(json_filepath):
                    print_library_log("configuration.json file detected in the project, "
                                      "but no configuration input provided via the command line", Logging.Level.WARNING)
                    env_configuration = env_configuration if env_configuration else "configuration.json"
                confirm = input(f"Does this debug run/deploy need configuration (y/N):")
            if confirm.lower()=='y':
                configuration = get_input_from_cli("Provide the configuration file path", env_configuration)
                config_values = validate_and_load_configuration(args.project_path, configuration)
                return config_values, configuration
            else:
                print_library_log("No input required for configuration. Continuing without configuration.", Logging.Level.INFO)
                return {}, None
        config_values = validate_and_load_configuration(args.project_path, configuration)
        return config_values, configuration
    except ValueError as e:
        args.configuration = None
        if retrying >= MAX_RETRIES or args.force:
            print_library_log(f"{e}. Invalid Configuration, Exiting..", Logging.Level.WARNING)
            sys.exit(1)
        else:
            print_library_log(f"{e}. Please retry..", Logging.Level.INFO)
            return get_configuration(args, retrying + 1)


def check_newer_version(version: str):
    """Periodically checks for a newer version of the SDK and notifies the user if one is available."""
    tester_root_dir = tester_root_dir_helper()
    last_check_file_path = os.path.join(tester_root_dir, LAST_VERSION_CHECK_FILE)
    if not os.path.isdir(tester_root_dir):
        os.makedirs(tester_root_dir, exist_ok=True)

    if os.path.isfile(last_check_file_path):
        # Is it time to check again?
        with open(last_check_file_path, 'r', encoding=UTF_8) as f_in:
            timestamp = int(f_in.read())
            if (int(time.time()) - timestamp) < ONE_DAY_IN_SEC:
                return

    for index in range(MAX_RETRIES):
        try:
            # check version and save current time
            response = rq.get(PYPI_PACKAGE_DETAILS_URL)
            response.raise_for_status()
            data = json.loads(response.text)
            latest_version = data["info"]["version"]
            if version < latest_version:
                print_library_log(f"[notice] A new release of 'fivetran-connector-sdk' is available: {latest_version}")
                print_library_log("[notice] To update, run: pip install --upgrade fivetran-connector-sdk")

            with open(last_check_file_path, 'w', encoding=UTF_8) as f_out:
                f_out.write(f"{int(time.time())}")
            break
        except Exception:
            retry_after = 2 ** index
            print_library_log(f"Unable to check if a newer version of `fivetran-connector-sdk` is available. "
                              f"Retrying after {retry_after} seconds", Logging.Level.WARNING)
            time.sleep(retry_after)


def tester_root_dir_helper() -> str:
    """Returns the root directory for the tester."""
    return os.path.join(os.path.expanduser("~"), ROOT_LOCATION)

def _warn_exit_usage(filename, line_no, func):
    print_library_log(f"Avoid using {func} to exit from the Python code as this can cause the connector to become stuck. Throw an error if required " +
                      f"at: {filename}:{line_no}. See the Technical Reference for details: https://fivetran.com/docs/connector-sdk/technical-reference#handlingexceptions",
                      Logging.Level.WARNING)

def exit_check(project_path):
    """Checks for the presence of 'exit()' in the calling code.
    Args:
        project_path: The absolute project_path to check exit in the connector.py file in the project.
    """
    # We expect the connector.py to catch errors or throw exceptions
    # This is a warning shown to let the customer know that we expect either the yield call or error thrown
    # exit() or sys.exit() in between some yields can cause the connector to be stuck without processing further upsert calls

    filepath = os.path.join(project_path, ROOT_FILENAME)
    with open(filepath, "r", encoding=UTF_8) as f:
        try:
            tree = ast.parse(f.read())
            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Name) and node.func.id == "exit":
                        _warn_exit_usage(ROOT_FILENAME, node.lineno, "exit()")
                    elif isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
                        if node.func.attr == "_exit" and node.func.value.id == "os":
                            _warn_exit_usage(ROOT_FILENAME, node.lineno, "os._exit()")
                        if node.func.attr == "exit" and node.func.value.id == "sys":
                            _warn_exit_usage(ROOT_FILENAME, node.lineno, "sys.exit()")
        except SyntaxError as e:
            print_library_log(f"SyntaxError in {ROOT_FILENAME}: {e}", Logging.Level.SEVERE)


def check_dict(incoming: dict, string_only: bool = False) -> dict:
    """Validates the incoming dictionary.
    Args:
        incoming (dict): The dictionary to validate.
        string_only (bool): Whether to allow only string values.

    Returns:
        dict: The validated dictionary.
    """

    if not incoming:
        return {}

    if not isinstance(incoming, dict):
        raise ValueError(
            "Configuration must be provided as a JSON dictionary. Check your input. Reference: https://fivetran.com/docs/connectors/connector-sdk/detailed-guide#workingwithconfigurationjsonfile")

    if string_only:
        for k, v in incoming.items():
            if not isinstance(v, str):
                print_library_log(
                    "All values in the configuration must be STRING. Check your configuration and ensure that every value is a STRING.", Logging.Level.SEVERE)
                sys.exit(1)

    return incoming


def is_connection_name_valid(connection: str):
    """Validates if the incoming connection schema name is valid or not.
    Args:
        connection (str): The connection schema name being validated.

    Returns:
        bool: True if connection name is valid.
    """

    pattern = re.compile(CONNECTION_SCHEMA_NAME_PATTERN)
    return pattern.match(connection)


def log_unused_deps_error(package_name: str, version: str):
    print_library_log(f"Remove `{package_name}` from requirements.txt."
          f" The latest version of `{package_name}` is always available when executing your code."
          f" Current version: {version}", Logging.Level.SEVERE)


def is_port_in_use(port: int):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0


def get_available_port():
    for port in range(50051, 50061):
        if not is_port_in_use(port):
            return port
    return None


def update_base_url_if_required():
    config_file_path = os.path.join(tester_root_dir_helper(), CONFIG_FILE)
    if os.path.isfile(config_file_path):
        with open(config_file_path, 'r', encoding=UTF_8) as f:
            data = json.load(f)
            base_url = data.get('production_base_url')
            if base_url is not None:
                constants.PRODUCTION_BASE_URL = base_url
                print_library_log(f"Updating PRODUCTION_BASE_URL to: {base_url}")

def fetch_requirements_from_file(file_path: str) -> list[str]:
    """Reads the requirements file and returns a list of dependencies.

    Args:
        file_path (str): The path to the requirements file.

    Returns:
        list[str]: A list of dependencies as strings.
    """
    with open(file_path, 'r', encoding=UTF_8) as f:
        return f.read().splitlines()

def fetch_requirements_as_dict(file_path: str) -> dict:
    """Converts a list of dependencies from the requirements file into a dictionary.

    Args:
        file_path (str): The path to the requirements file.

    Returns:
        dict: A dictionary where keys are package names (lowercase) and
        values are the full dependency strings.
    """
    requirements_dict = {}
    if not os.path.exists(file_path):
        return requirements_dict
    for requirement in fetch_requirements_from_file(file_path):
        requirement = requirement.strip()
        if not requirement or requirement.startswith("#"):  # Skip empty lines and comments
            continue
        try:
            key = re.split(r"==|>=|<=|>|<", requirement)[0]
            requirements_dict[key.lower().replace('-', '_')] = requirement.lower()
        except ValueError:
            print_library_log(f"Invalid requirement format: '{requirement}'", Logging.Level.SEVERE)
    return requirements_dict

def validate_requirements_file(project_path: str, is_deploy: bool, version: str):
    """Validates the `requirements.txt` file against the project's actual dependencies.

    This method generates a temporary requirements file using `pipreqs`, compares
    it with the existing `requirements.txt`, and checks for version mismatches,
    missing dependencies, and unused dependencies. It will issue warnings, errors,
    or even terminate the process depending on whether it's being run for deployment.

    Args:
        project_path (str): The path to the project directory containing the `requirements.txt`.
        is_deploy (bool): If `True`, the method will exit the process on critical errors.
        version (str): The current version of the connector.

    """
    # Detect and exclude virtual environment directories
    venv_dirs = [name for name in os.listdir(project_path)
                 if os.path.isdir(os.path.join(project_path, name)) and
                 VIRTUAL_ENV_CONFIG in os.listdir(os.path.join(project_path, name))]

    ignored_dirs = EXCLUDED_PIPREQS_DIRS + venv_dirs if venv_dirs else EXCLUDED_PIPREQS_DIRS

    # tmp_requirements is only generated when pipreqs command is successful
    requirements_file_path = os.path.join(project_path, REQUIREMENTS_TXT)
    tmp_requirements_file_path = os.path.join(project_path, 'tmp_requirements.txt')
    # copying packages of requirements file to tmp file to handle pipreqs fail use-case
    copy_requirements_file_to_tmp_requirements_file(requirements_file_path, tmp_requirements_file_path)
    # Run the pipreqs command and capture stderr
    attempt = 0
    while attempt < MAX_RETRIES:
        attempt += 1
        result = subprocess.run(
            ["pipreqs", project_path, "--savepath", tmp_requirements_file_path, "--ignore", ",".join(ignored_dirs)],
            stderr=subprocess.PIPE,
            text=True  # Ensures output is in string format
        )

        if result.returncode == 0:
            break

        print_library_log(f"Attempt {attempt}: pipreqs check failed.", Logging.Level.WARNING)

        if attempt < MAX_RETRIES:
            retry_after = 3 ** attempt
            print_library_log(f"Retrying in {retry_after} seconds...", Logging.Level.SEVERE)
            time.sleep(retry_after)
        else:
            print_library_log(f"pipreqs failed after {MAX_RETRIES} attempts with:", Logging.Level.SEVERE)
            print_library_log(result.stderr, Logging.Level.SEVERE)
            print_library_log(f"Skipping validation of requirements.txt due to error connecting to PyPI (Python Package Index) APIs. Continuing with {'deploy' if is_deploy else 'debug'}...", Logging.Level.SEVERE)

    tmp_requirements = fetch_requirements_as_dict(tmp_requirements_file_path)
    remove_unwanted_packages(tmp_requirements)
    delete_file_if_exists(tmp_requirements_file_path)

    # remove corrupt requirements listed by pipreqs
    corrupt_requirements = [key for key in tmp_requirements if key.startswith("~")]
    for requirement in corrupt_requirements:
        del tmp_requirements[requirement]

    update_version_requirements = False
    update_missing_requirements = False
    update_unused_requirements = False
    requirements = load_or_add_requirements_file(requirements_file_path)

    version_mismatch_deps = {key: tmp_requirements[key] for key in
                             (requirements.keys() & tmp_requirements.keys())
                             if requirements[key] != tmp_requirements[key]}
    if version_mismatch_deps:
        print_library_log("We recommend using the current stable version for the following libraries:", Logging.Level.WARNING)
        print(version_mismatch_deps)
        if is_deploy:
            confirm = input(
                    f"Would you like us to update {REQUIREMENTS_TXT} to the current stable versions of the dependent libraries? (y/N):")
            if confirm.lower() == "y":
                update_version_requirements = True
                for requirement in version_mismatch_deps:
                    requirements[requirement] = tmp_requirements[requirement]
                print_library_log(
                    f"Successfully updated {REQUIREMENTS_TXT} to the current stable versions of the dependent libraries.")
            elif confirm.lower() == "n":
                print_library_log(f"Changes identified for libraries with version conflicts have been ignored. These changes have NOT been made to {REQUIREMENTS_TXT}.")

    missing_deps = {key: tmp_requirements[key] for key in (tmp_requirements.keys() - requirements.keys())}
    if missing_deps:
        handle_missing_deps(missing_deps)
        if is_deploy:
            confirm = input(
                    f"Would you like us to update {REQUIREMENTS_TXT} to add missing dependent libraries? (y/N):")
            if confirm.lower() == "n":
                print_library_log(f"Changes identified as missing dependencies for libraries have been ignored. These changes have NOT been made to {REQUIREMENTS_TXT}.")
            elif confirm.lower() == "y":
                update_missing_requirements = True
                for requirement in missing_deps:
                    requirements[requirement] = tmp_requirements[requirement]
                print_library_log(f"Successfully added missing dependencies to {REQUIREMENTS_TXT}.")

    unused_deps = list(requirements.keys() - tmp_requirements.keys())
    if unused_deps:
        handle_unused_deps(unused_deps, version)
        if is_deploy:
            confirm = input(f"Would you like us to update {REQUIREMENTS_TXT} to remove the unused libraries? (y/N):")
            if confirm.lower() == "n":
                if 'fivetran_connector_sdk' in unused_deps or 'requests' in unused_deps:
                    print_library_log(
                        f"Fix your {REQUIREMENTS_TXT} file by removing pre-installed dependencies [fivetran_connector_sdk, requests] to proceed with the deployment.")
                    sys.exit(1)
                print_library_log(f"Changes identified for unused libraries have been ignored. These changes have NOT been made to {REQUIREMENTS_TXT}.")
            elif confirm.lower() == "y":
                update_unused_requirements = True
                for requirement in unused_deps:
                    del requirements[requirement]
                print_library_log(f"Successfully removed unused libraries from {REQUIREMENTS_TXT}.")

    if update_version_requirements or update_missing_requirements or update_unused_requirements:
        with open(requirements_file_path, "w", encoding=UTF_8) as file:
            file.write("\n".join(requirements.values()))
            print_library_log(f"`{REQUIREMENTS_TXT}` has been updated successfully.")
    elif not requirements:
        delete_file_if_exists(requirements_file_path)

    if is_deploy: print_library_log(f"Validation of {REQUIREMENTS_TXT} completed.")

def handle_unused_deps(unused_deps, version):
    if 'fivetran_connector_sdk' in unused_deps:
        log_unused_deps_error("fivetran_connector_sdk", version)
    if 'requests' in unused_deps:
        log_unused_deps_error("requests", "2.32.4")
    print_library_log("The following dependencies are not needed, "
          f"they are already installed or not in use. Remove them from {REQUIREMENTS_TXT}:", Logging.Level.WARNING)
    print(*unused_deps)

def handle_missing_deps(missing_deps):
    print_library_log(f"Include the following dependency libraries in {REQUIREMENTS_TXT}, to be used by "
          "Fivetran production. "
          "For more information, see our docs: "
          "https://fivetran.com/docs/connectors/connector-sdk/detailed-guide"
          "#workingwithrequirementstxtfile", Logging.Level.SEVERE)
    print(*list(missing_deps.values()))

def load_or_add_requirements_file(requirements_file_path):
    if os.path.exists(requirements_file_path):
        requirements = fetch_requirements_as_dict(requirements_file_path)
    else:
        with open(requirements_file_path, 'w', encoding=UTF_8):
            pass
        requirements = {}
        print_library_log("Adding `requirements.txt` file to your project folder.", Logging.Level.WARNING)
    return requirements

def copy_requirements_file_to_tmp_requirements_file(requirements_file_path: str, tmp_requirements_file_path):
    if os.path.exists(requirements_file_path):
        requirements_file_content = fetch_requirements_from_file(requirements_file_path)
        with open(tmp_requirements_file_path, 'w') as file:
            file.write("\n".join(requirements_file_content))

def remove_unwanted_packages(requirements: dict):
    # remove the `fivetran_connector_sdk` and `requests` packages from requirements as we already pre-installed them.
    if requirements.get("fivetran_connector_sdk") is not None:
        requirements.pop("fivetran_connector_sdk")
    if requirements.get('requests') is not None:
        requirements.pop("requests")


def upload_project(project_path: str, deploy_key: str, group_id: str, group_name: str, connection: str):
    print_library_log(
        f"Deploying '{project_path}' to connection '{connection}' in destination '{group_name}'.\n")
    upload_file_path = create_upload_file(project_path)
    upload_result = upload(
        upload_file_path, deploy_key, group_id, connection)
    delete_file_if_exists(upload_file_path)
    if not upload_result:
        sys.exit(1)


def cleanup_uploaded_project(deploy_key: str, group_id: str, connection: str):
    cleanup_result = cleanup_uploaded_code(deploy_key, group_id, connection)
    if not cleanup_result:
        sys.exit(1)

def update_connection(id: str, name: str, group: str, config: dict, deploy_key: str, hd_agent_id: str):
    """Updates the connection with the given ID, name, group, configuration, and deployment key.

    Args:
        args (dict): The command arguments.
        id (str): The connection ID.
        name (str): The connection name.
        group (str): The group name.
        config (dict): The configuration dictionary.
        deploy_key (str): The deployment key.
        hd_agent_id (str): The hybrid deployment agent ID within the Fivetran system.
    """
    if not config.get("secrets_list"):
        del config["secrets_list"]

    json_payload = {
        "config": config,
        "run_setup_tests": True
    }

    # hybrid_deployment_agent_id is optional when redeploying your connection.
    # Customer can use it to change existing hybrid_deployment_agent_id.
    if hd_agent_id:
        json_payload["hybrid_deployment_agent_id"] = hd_agent_id

    response = rq.patch(f"{constants.PRODUCTION_BASE_URL}/v1/connectors/{id}",
                        headers={"Authorization": f"Basic {deploy_key}"},
                        json=json_payload)

    if response.ok and response.status_code == HTTPStatus.OK:
        if are_setup_tests_failing(response):
            handle_failing_tests_message_and_exit(response,"The connection was updated, but setup tests failed!")
        else:
            print_library_log(f"Connection '{name}' in group '{group}' updated successfully.", Logging.Level.INFO)

    else:
        print_library_log(
            f"Unable to update Connection '{name}' in destination '{group}', failed with error: '{response.json()['message']}'.",
            Logging.Level.SEVERE)
        sys.exit(1)
    return response

def handle_failing_tests_message_and_exit(resp, log_message):
    print_library_log(log_message, Logging.Level.SEVERE)
    print_failing_setup_tests(resp)
    connection_id = resp.json().get('data', {}).get('id')
    print_library_log(f"Connection ID: {connection_id}")
    print_library_log("Try again with the deploy command after resolving the issue!")
    sys.exit(1)

def are_setup_tests_failing(response) -> bool:
    """Checks for failed setup tests in the response and returns True if any test has failed, otherwise False."""
    response_json = response.json()
    setup_tests = response_json.get("data", {}).get("setup_tests", [])

    # Return True if any test has "FAILED" status, otherwise False
    return any(test.get("status") == "FAILED" or test.get("status") == "JOB_FAILED" for test in setup_tests)

def print_failing_setup_tests(response):
    """Checks for failed setup tests in the response and print errors."""
    response_json = response.json()
    setup_tests = response_json.get("data", {}).get("setup_tests", [])

    # Collect failed setup tests
    failed_tests = [test for test in setup_tests if
                    test.get("status") == "FAILED" or test.get("status") == "JOB_FAILED"]

    if failed_tests:
        print_library_log("The following setup tests have failed!", Logging.Level.WARNING)
        for test in failed_tests:
            print_library_log(f"Test: {test.get('title')}", Logging.Level.WARNING)
            print_library_log(f"Status: {test.get('status')}", Logging.Level.WARNING)
            print_library_log(f"Message: {test.get('message')}", Logging.Level.WARNING)

def get_connection_id(name: str, group: str, group_id: str, deploy_key: str) -> Optional[Tuple[str, str]]:
    """Retrieves the connection ID for the specified connection schema name, group, and deployment key.

    Args:
        name (str): The connection name.
        group (str): The group name.
        group_id (str): The group ID.
        deploy_key (str): The deployment key.

    Returns:
        str: The connection ID, or None
    """
    resp = rq.get(f"{constants.PRODUCTION_BASE_URL}/v1/groups/{group_id}/connectors",
                  headers={"Authorization": f"Basic {deploy_key}"},
                  params={"schema": name})
    if not resp.ok:
        print_library_log(
            f"Unable to fetch connection list in destination '{group}'", Logging.Level.SEVERE)
        sys.exit(1)

    if resp.json()['data']['items']:
        return resp.json()['data']['items'][0]['id'], resp.json()['data']['items'][0]['service']

    return None

def create_connection(deploy_key: str, group_id: str, config: dict, hd_agent_id: str) -> rq.Response:
    """Creates a new connection with the given deployment key, group ID, and configuration.

    Args:
        deploy_key (str): The deployment key.
        group_id (str): The group ID.
        config (dict): The configuration dictionary.
        hd_agent_id (str): The hybrid deployment agent ID within the Fivetran system.

    Returns:
        rq.Response: The response object.
    """
    response = rq.post(f"{constants.PRODUCTION_BASE_URL}/v1/connectors",
                       headers={"Authorization": f"Basic {deploy_key}"},
                       json={
                           "group_id": group_id,
                           "service": "connector_sdk",
                           "config": config,
                           "paused": True,
                           "run_setup_tests": True,
                           "sync_frequency": "360",
                           "hybrid_deployment_agent_id": hd_agent_id
                       })
    return response


def create_upload_file(project_path: str) -> str:
    """Creates an upload file for the given project path.

    Args:
        project_path (str): The path to the project.

    Returns:
        str: The path to the upload file.
    """
    print_library_log("Packaging your project for upload...")
    zip_file_path = zip_folder(project_path)
    print("✓")
    return zip_file_path


def zip_folder(project_path: str) -> str:
    """Zips the folder at the given project path.

    Args:
        project_path (str): The path to the project.

    Returns:
        str: The path to the zip file.
    """
    upload_filepath = os.path.join(project_path, UPLOAD_FILENAME)
    connector_file_exists = False
    custom_drivers_exists = False
    custom_driver_installation_script_exists = False

    with ZipFile(upload_filepath, 'w', ZIP_DEFLATED) as zipf:
        for root, files in dir_walker(project_path):
            if os.path.basename(root) == DRIVERS:
                custom_drivers_exists = True
            if INSTALLATION_SCRIPT in files:
                custom_driver_installation_script_exists = True
            for file in files:
                if file == ROOT_FILENAME:
                    connector_file_exists = True
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, project_path)
                zipf.write(file_path, arcname)

    if not connector_file_exists:
        print_library_log(
            "The 'connector.py' file is missing. Ensure that 'connector.py' is present in your project directory, and that the file name is in lowercase. All custom connectors require this file because Fivetran calls it to start a sync.",
            Logging.Level.SEVERE)
        sys.exit(1)

    if custom_drivers_exists and not custom_driver_installation_script_exists:
        print_library_log(INSTALLATION_SCRIPT_MISSING_MESSAGE, Logging.Level.SEVERE)
        sys.exit(1)

    return upload_filepath


def dir_walker(top):
    """Walks the directory tree starting at the given top directory.

    Args:
        top (str): The top directory to start the walk.

    Yields:
        tuple: A tuple containing the current directory path and a list of files.
    """
    dirs, files = [], []
    for name in os.listdir(top):
        path = os.path.join(top, name)
        if os.path.isdir(path):
            if (name not in EXCLUDED_DIRS) and (not name.startswith(".")):
                if VIRTUAL_ENV_CONFIG not in os.listdir(path):  # Check for virtual env indicator
                    dirs.append(name)
        else:
            # Include all files if in `drivers` folder
            if os.path.basename(top) == DRIVERS:
                files.append(name)
            if name.endswith(".py") or name == "requirements.txt":
                files.append(name)

    yield top, files
    for name in dirs:
        new_path = os.path.join(top, name)
        for x in dir_walker(new_path):
            yield x

def upload(local_path: str, deploy_key: str, group_id: str, connection: str) -> bool:
    """Uploads the local code file for the specified group and connection.

    Args:
        local_path (str): The local file path.
        deploy_key (str): The deployment key.
        group_id (str): The group ID.
        connection (str): The connection name.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    print_library_log("Uploading your project...")
    response = rq.post(f"{constants.PRODUCTION_BASE_URL}/v1/deploy/{group_id}/{connection}",
                       files={'file': open(local_path, 'rb')},
                       headers={"Authorization": f"Basic {deploy_key}"})
    if response.ok:
        print("✓")
        return True

    print_library_log(f"Unable to upload the project, failed with error: {response.reason}", Logging.Level.SEVERE)
    return False

def cleanup_uploaded_code(deploy_key: str, group_id: str, connection: str) -> bool:
    """Cleans up the uploaded code file for the specified group and connection, if creation fails.

    Args:
        deploy_key (str): The deployment key.
        group_id (str): The group ID.
        connection (str): The connection name.

    Returns:
        bool: True if the cleanup was successful, False otherwise.
    """
    print_library_log("INFO: Cleaning up your uploaded project ")
    response = rq.post(f"{constants.PRODUCTION_BASE_URL}/v1/cleanup_code/{group_id}/{connection}",
                       headers={"Authorization": f"Basic {deploy_key}"})
    if response.ok:
        print("✓")
        return True

    print_library_log(f"SEVERE: Unable to cleanup the project, failed with error: {response.reason}",
                      Logging.Level.SEVERE)
    return False

def get_os_arch_suffix() -> str:
    """
    Returns the operating system and architecture suffix for the current operating system.
    """
    system = platform.system().lower()
    machine = platform.machine().lower()

    if system not in OS_MAP:
        raise RuntimeError(f"Unsupported OS: {system}")

    plat = OS_MAP[system]

    if machine not in ARCH_MAP or (plat == WIN_OS and ARCH_MAP[machine] != X64):
        raise RuntimeError(f"Unsupported architecture '{machine}' for {plat}")

    return f"{plat}-{ARCH_MAP[machine]}"

def get_group_info(group: str, deploy_key: str) -> tuple[str, str]:
    """Retrieves the group information for the specified group and deployment key.

    Args:
        group (str): The group name.
        deploy_key (str): The deployment key.

    Returns:
        tuple[str, str]: A tuple containing the group ID and group name.
    """
    groups_url = f"{constants.PRODUCTION_BASE_URL}/v1/groups"

    params = {"limit": 500}
    headers = {"Authorization": f"Basic {deploy_key}"}
    resp = rq.get(groups_url, headers=headers, params=params)

    if not resp.ok:
        print_library_log(
            f"The request failed with status code: {resp.status_code}. Ensure you're using a valid base64-encoded API key and try again.",
            Logging.Level.SEVERE)
        sys.exit(1)

    data = resp.json().get("data", {})
    groups = data.get("items")

    if not groups:
        print_library_log("No destinations defined in the account", Logging.Level.SEVERE)
        sys.exit(1)

    if not group:
        if len(groups) == 1:
            return groups[0]['id'], groups[0]['name']
        else:
            print_library_log(
                "Destination name is required when there are multiple destinations in the account",
                Logging.Level.SEVERE)
            sys.exit(1)
    else:
        while True:
            for grp in groups:
                if grp['name'] == group:
                    return grp['id'], grp['name']

            next_cursor = data.get("next_cursor")
            if not next_cursor:
                break

            params = {"cursor": next_cursor, "limit": 500}
            resp = rq.get(groups_url, headers=headers, params=params)
            data = resp.json().get("data", {})
            groups = data.get("items", [])

    print_library_log(
        f"We couldn't find the specified destination '{group}' in your account.", Logging.Level.SEVERE)
    sys.exit(1)

def java_exe_helper(location: str, os_arch_suffix: str) -> str:
    """Returns the path to the Java executable.

    Args:
        location (str): The location of the Java executable.
        os_arch_suffix (str): The name of the operating system and architecture

    Returns:
        str: The path to the Java executable.
    """
    java_exe_base = os.path.join(location, "bin", "java")
    return f"{java_exe_base}.exe" if os_arch_suffix == f"{WIN_OS}-{X64}" else java_exe_base

def process_stream(stream):
    """Processes a stream of text lines, replacing occurrences of a specified pattern.

    This method reads each line from the provided stream, searches for occurrences of
    a predefined pattern, and replaces them with a specified replacement string.

    Args:
        stream (iterable): An iterable stream of text lines, typically from a file or another input source.

    Yields:
        str: Each line from the stream after replacing the matched pattern with the replacement string.
    """
    pattern = r'com\.fivetran\.partner_sdk.*\.tools\.testers\.\S+'

    for line in iter(stream.readline, ""):
        if not re.search(pattern, line):
            yield line

def run_tester(java_exe_str: str, root_dir: str, project_path: str, port: int, state_json: str,
               configuration_json: str):
    """Runs the connector tester.

    Args:
        java_exe_str (str): The path to the Java executable.
        root_dir (str): The root directory.
        project_path (str): The path to the project.
        port (int): The port number to use for the tester.
        state_json (str): The state JSON string to pass to the tester.
        configuration_json (str): The configuration JSON string to pass to the tester.

    Yields:
        str: The log messages from the tester.
    """
    working_dir = os.path.join(project_path, OUTPUT_FILES_DIR)
    try:
        os.mkdir(working_dir)
    except FileExistsError:
        pass

    cmd = [java_exe_str,
           "-jar",
           os.path.join(root_dir, TESTER_FILENAME),
           "--connector-sdk=true",
           f"--port={port}",
           f"--working-dir={working_dir}",
           "--tester-type=source",
           f"--state={state_json}",
           f"--configuration={configuration_json}"]

    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    for line in process_stream(popen.stderr):
        yield _maybe_colorize_jar_output(line)

    for line in process_stream(popen.stdout):
        yield _maybe_colorize_jar_output(line)
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)

def _maybe_colorize_jar_output(line: str) -> str:
    if not constants.DEBUGGING:
        return line

    if "SEVERE" in line or "ERROR" in line or "Exception" in line or "FAILED" in line:
        return f"\033[196m{line}\033[0m"  # ANSI Red color #ff0000
    elif "WARN" in line or "WARNING" in line:
        return f"\033[130m{line}\033[0m"  # ANSI Orange-like color #af5f00
    return line

def process_tables(response, table_list):
    for entry in response:
        if 'table' not in entry:
            raise ValueError("Entry missing table name: " + entry)

        table_name = get_renamed_table_name(entry['table'])

        if table_name in table_list:
            raise ValueError("Table already defined: " + table_name)

        table = common_pb2.Table(name=table_name)
        columns = {}

        if "primary_key" in entry:
            process_primary_keys(columns, entry)

        if "columns" in entry:
            process_columns(columns, entry)

        table.columns.extend(columns.values())
        TABLES[table_name] = table
        table_list[table_name] = table

def process_primary_keys(columns, entry):
    for pkey_name in entry["primary_key"]:
        column_name = get_renamed_column_name(pkey_name)
        column = columns[column_name] if column_name in columns else common_pb2.Column(name=column_name)
        column.primary_key = True
        columns[column_name] = column

def process_columns(columns, entry):
    for name, type in entry["columns"].items():
        column_name = get_renamed_column_name(name)
        column = columns[column_name] if column_name in columns else common_pb2.Column(name=column_name)

        if isinstance(type, str):
            process_data_type(column, type)

        elif isinstance(type, dict):
            if type['type'].upper() != "DECIMAL":
                error_message = (
                    f"Expecting DECIMAL data type for dictionary column entry, but got: {type['type']} in entry: {entry} "
                    f"for column: {column_name}. "
                    "Dictionary type is only allowed for DECIMAL columns with 'precision' and 'scale' fields, "
                    "as in: {'type': 'DECIMAL', 'precision': <int>, 'scale': <int>}. "
                    "For all other data types, use a string as the column type."
                )
                raise ValueError(error_message)
            column.type = common_pb2.DataType.DECIMAL
            column.params.decimal.precision = type['precision']
            column.params.decimal.scale = type['scale']

        else:
            raise ValueError(
                f"Unrecognized column type for column: {column_name} in entry: {entry}. Got: {str(type)}"
            )

        if "primary_key" in entry and name in entry["primary_key"]:
            column.primary_key = True


        columns[column_name] = column

def process_data_type(column, type):
    if type.upper() == "BOOLEAN":
        column.type = common_pb2.DataType.BOOLEAN
    elif type.upper() == "SHORT":
        column.type = common_pb2.DataType.SHORT
    elif type.upper() == "INT":
        column.type = common_pb2.DataType.INT
    elif type.upper() == "LONG":
        column.type = common_pb2.DataType.LONG
    elif type.upper() == "DECIMAL":
        raise ValueError(
            "DECIMAL data type missing precision and scale. "
            "Use a dictionary for DECIMAL column type like: "
            '''"col_name": {  # Decimal data type with precision and scale.\n'''
            '''    "type": "DECIMAL",\n'''
            '''    "precision": 15,\n'''
            '''    "scale": 2\n'''
            '''}'''
        )
    elif type.upper() == "FLOAT":
        column.type = common_pb2.DataType.FLOAT
    elif type.upper() == "DOUBLE":
        column.type = common_pb2.DataType.DOUBLE
    elif type.upper() == "NAIVE_DATE":
        column.type = common_pb2.DataType.NAIVE_DATE
    elif type.upper() == "NAIVE_DATETIME":
        column.type = common_pb2.DataType.NAIVE_DATETIME
    elif type.upper() == "UTC_DATETIME":
        column.type = common_pb2.DataType.UTC_DATETIME
    elif type.upper() == "BINARY":
        column.type = common_pb2.DataType.BINARY
    elif type.upper() == "XML":
        column.type = common_pb2.DataType.XML
    elif type.upper() == "STRING":
        column.type = common_pb2.DataType.STRING
    elif type.upper() == "JSON":
        column.type = common_pb2.DataType.JSON
    else:
        raise ValueError("Unrecognized column type encountered:: ", str(type))

def delete_file_if_exists(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
