import os
import sys
import grpc
import json
import shutil
import argparse
import traceback
import requests as rq
import threading
import queue
from types import GeneratorType
from http import HTTPStatus
from zipfile import ZipFile
from concurrent import futures

from fivetran_connector_sdk.protos import common_pb2
from fivetran_connector_sdk.protos import connector_sdk_pb2
from fivetran_connector_sdk.protos import connector_sdk_pb2_grpc

from fivetran_connector_sdk.logger import Logging
from fivetran_connector_sdk.operations import Operations
from fivetran_connector_sdk import constants
from fivetran_connector_sdk.constants import (
    TESTER_VER, VERSION_FILENAME, UTF_8,
    VALID_COMMANDS, DEFAULT_PYTHON_VERSION, SUPPORTED_PYTHON_VERSIONS, TABLES
)
from fivetran_connector_sdk.helpers import (
    print_library_log, reset_local_file_directory, find_connector_object, suggest_correct_command,
)
from fivetran_connector_sdk.connector_helper import (
    validate_requirements_file, upload_project,
    update_connection, are_setup_tests_failing, get_connection_id,
    handle_failing_tests_message_and_exit, delete_file_if_exists,
    create_connection, get_os_arch_suffix, get_group_info,
    java_exe_helper, run_tester, process_tables,
    update_base_url_if_required, exit_check,
    get_available_port, tester_root_dir_helper,
    check_dict, check_newer_version, cleanup_uploaded_project,
    get_destination_group, get_connection_name, get_api_key, get_state,
    get_python_version, get_hd_agent_id, get_configuration
)

# Version format: <major_version>.<minor_version>.<patch_version>
# (where Major Version = 2, Minor Version is incremental MM from Aug 25 onwards, Patch Version is incremental within a month)
__version__ = "2.0.1"
TESTER_VERSION = TESTER_VER
MAX_MESSAGE_LENGTH = 32 * 1024 * 1024 # 32MB

__all__ = [cls.__name__ for cls in [Logging, Operations]]

class Connector(connector_sdk_pb2_grpc.SourceConnectorServicer):
    def __init__(self, update, schema=None):
        """Initializes the Connector instance.
        Args:
            update: The update method.
            schema: The schema method.
        """

        self.schema_method = schema
        self.update_method = update

        self.configuration = None
        self.state = None

        update_base_url_if_required()

    # Call this method to deploy the connector to Fivetran platform
    def deploy(self, project_path: str, deploy_key: str, group: str, connection: str, hd_agent_id: str,
               configuration: dict = None, config_path = None, python_version: str = None, force: bool = False):
        """Deploys the connector to the Fivetran platform.

        Args:
            args (dict): The command arguments.
            deploy_key (str): The deployment key.
            group (str): The group name.
            connection (str): The connection name.
            hd_agent_id (str): The hybrid deployment agent ID within the Fivetran system.
            configuration (dict): The configuration dictionary.
        """
        deploy_cmd = f"Deploying with parameters: Fivetran deploy --destination {group} --connection {connection} --api-key {deploy_key[0:8]}******** "
        if config_path:
            deploy_cmd += f"--configuration {config_path} "
        if python_version:
            deploy_cmd += f"--python-version {python_version} "
        if hd_agent_id:
            deploy_cmd += f"--hd-agent-id {hd_agent_id} "
        if force:
            deploy_cmd += "--force"
        print_library_log(deploy_cmd)

        constants.EXECUTED_VIA_CLI = True

        print_library_log("We support only `.py` files and a `requirements.txt` file as part of the code upload. *No other code files* are supported or uploaded during the deployment process. Ensure that your code is structured accordingly and all dependencies are listed in `requirements.txt`")

        check_dict(configuration, True)

        secrets_list = []
        if configuration:
            for k, v in configuration.items():
                secrets_list.append({"key": k, "value": v})

        connection_config = {
            "schema": connection,
            "secrets_list": secrets_list,
            "sync_method": "DIRECT"
        }

        if python_version:
            connection_config["python_version"] = python_version

        if not force:
            validate_requirements_file(project_path, True, __version__)
        else:
            print_library_log(
                "Skipping requirements.txt validation as --force flag is set. Ensure that your code is structured accordingly and all dependencies are listed in `requirements.txt`")

        group_id, group_name = get_group_info(group, deploy_key)
        connection_id, service = get_connection_id(connection, group, group_id, deploy_key) or (None, None)

        if connection_id:
            if service != 'connector_sdk':
                print_library_log(
                    f"The connection '{connection}' already exists and does not use the 'Connector SDK' service. You cannot update this connection.", Logging.Level.SEVERE)
                sys.exit(1)
            else:
                if force:
                    confirm = "y"
                    if configuration:
                        confirm_config = "y"
                else:
                    confirm = input(
                        f"The connection '{connection}' already exists in the destination '{group}'. Updating it will overwrite the existing code. Do you want to proceed with the update? (y/N): ")
                    if confirm.lower() == "y" and configuration:
                        confirm_config = input(f"Your deploy will overwrite the configuration using the values provided in '{config_path}': key-value pairs not present in the new configuration will be removed; existing keys' values set in the configuration file or in the dashboard will be overwritten with new (empty or non-empty) values; new key-value pairs will be added. Do you want to proceed with the update? (y/N): ")
                if confirm.lower() == "y" and (not connection_config["secrets_list"] or (confirm_config.lower() == "y")):
                    print_library_log("Updating the connection...\n")
                    upload_project(
                        project_path, deploy_key, group_id, group_name, connection)
                    response = update_connection(
                        connection_id, connection, group_name, connection_config, deploy_key, hd_agent_id)
                    print("✓")
                    print_library_log(f"Python version {response.json()['data']['config']['python_version']} to be used at runtime.",
                                      Logging.Level.INFO)
                    print_library_log(f"Connection ID: {connection_id}")
                    print_library_log(
                        f"Visit the Fivetran dashboard to manage the connection: https://fivetran.com/dashboard/connectors/{connection_id}/status")
                else:
                    print_library_log("Update canceled. The process is now terminating.")
                    sys.exit(1)
        else:
            upload_project(project_path, deploy_key,
                                  group_id, group_name, connection)
            response = create_connection(
                deploy_key, group_id, connection_config, hd_agent_id)
            if response.ok and response.status_code == HTTPStatus.CREATED:
                if are_setup_tests_failing(response):
                    handle_failing_tests_message_and_exit(response, "The connection was created, but setup tests failed!")
                else:
                    print_library_log(
                        f"The connection '{connection}' has been created successfully.\n")
                    connection_id = response.json()['data']['id']
                    print_library_log(f"Python version {response.json()['data']['config']['python_version']} to be used at runtime.",
                                      Logging.Level.INFO)
                    print_library_log(f"Connection ID: {connection_id}")
                    print_library_log(
                        f"Visit the Fivetran dashboard to start the initial sync: https://fivetran.com/dashboard/connectors/{connection_id}/status")
            else:
                print_library_log(
                    f"Unable to create a new connection, failed with error: {response.json()['message']}", Logging.Level.SEVERE)
                cleanup_uploaded_project(deploy_key,group_id, connection)
                print_library_log("Please try again with the deploy command after resolving the issue!")
                sys.exit(1)

    # Call this method to run the connector in production
    def run(self,
            port: int = 50051,
            configuration: dict = None,
            state: dict = None,
            log_level: Logging.Level = Logging.Level.INFO) -> grpc.Server:
        """Runs the connector server.

        Args:
            port (int): The port number to listen for incoming requests.
            configuration (dict): The configuration dictionary.
            state (dict): The state dictionary.
            log_level (Logging.Level): The logging level.

        Returns:
            grpc.Server: The gRPC server instance.
        """
        self.configuration = check_dict(configuration, True)
        self.state = check_dict(state)
        Logging.LOG_LEVEL = log_level

        if not constants.DEBUGGING:
            print_library_log(f"Running on fivetran_connector_sdk: {__version__}")

        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            options=[
                ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
                ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
            ]
        )
        connector_sdk_pb2_grpc.add_SourceConnectorServicer_to_server(self, server)
        server.add_insecure_port("[::]:" + str(port))
        server.start()
        if constants.DEBUGGING:
            return server
        server.wait_for_termination()

    # This method starts both the server and the local testing environment
    def debug(self,
              project_path: str = None,
              configuration: dict = None,
              state: dict = None,
              log_level: Logging.Level = Logging.Level.FINE):
        """Tests the connector code by running it with the connector tester.\n
        state.json docs: https://fivetran.com/docs/connectors/connector-sdk/detailed-guide#workingwithstatejsonfile\n
        configuration.json docs: https://fivetran.com/docs/connectors/connector-sdk/detailed-guide#workingwithconfigurationjsonfile

        Args:
            project_path (str): The path to the project.
            configuration (dict): The configuration dictionary, same as configuration.json if present.
            state (dict): The state dictionary, same as state.json if present.
            log_level (Logging.Level): The logging level.
        """
        constants.DEBUGGING = True

        check_newer_version(__version__)

        Logging.LOG_LEVEL = log_level
        os_arch_suffix = get_os_arch_suffix()
        tester_root_dir = tester_root_dir_helper()
        java_exe = java_exe_helper(tester_root_dir, os_arch_suffix)
        install_tester = False
        version_file = os.path.join(tester_root_dir, VERSION_FILENAME)
        if os.path.isfile(version_file):
            # Check version number & update if different
            with open(version_file, 'r', encoding=UTF_8) as fi:
                current_version = fi.readline()

            if current_version != TESTER_VERSION:
                shutil.rmtree(tester_root_dir)
                install_tester = True
        else:
            install_tester = True

        if install_tester:
            os.makedirs(tester_root_dir, exist_ok=True)
            download_filename = f"sdk-connector-tester-{os_arch_suffix}-{TESTER_VERSION}.zip"
            download_filepath = os.path.join(tester_root_dir, download_filename)
            try:
                print_library_log(f"Downloading connector tester version: {TESTER_VERSION} ")
                download_url = f"https://github.com/fivetran/fivetran_sdk_tools/releases/download/{TESTER_VERSION}/{download_filename}"
                r = rq.get(download_url)
                if r.ok:
                    with open(download_filepath, 'wb') as fo:
                        fo.write(r.content)
                else:
                    raise RuntimeError(
                        f"\nSEVERE: Failed to download the connector tester. Please check your access permissions or "
                        f"try again later ( status code: {r.status_code}), url: {download_url}")
            except RuntimeError:
                raise RuntimeError(
                    f"SEVERE: Failed to download the connector tester. Error details: {traceback.format_exc()}")

            try:
                # unzip it
                with ZipFile(download_filepath, 'r') as z_object:
                    z_object.extractall(path=tester_root_dir)
                # delete zip file
                delete_file_if_exists(download_filepath)
                # make java binary executable
                import stat
                st = os.stat(java_exe)
                os.chmod(java_exe, st.st_mode | stat.S_IEXEC)
                print("✓")
            except:
                shutil.rmtree(tester_root_dir)
                raise RuntimeError(f"\nSEVERE: Failed to install the connector tester. Error details: {traceback.format_exc()}")

        project_path = os.getcwd() if project_path is None else project_path
        validate_requirements_file(project_path, False, __version__)
        print_library_log(f"Debugging connector at: {project_path}")
        available_port = get_available_port()
        exit_check(project_path)

        if available_port is None:
            raise RuntimeError("SEVERE: Unable to allocate a port in the range 50051-50061. "
                               "Please ensure a port is available and try again")

        server = self.run(available_port, configuration, state, log_level=log_level)

        # Uncomment this to run the tester manually
        # server.wait_for_termination()

        try:
            print_library_log("Running connector tester...")
            for log_msg in run_tester(java_exe, tester_root_dir, project_path, available_port, json.dumps(self.state), json.dumps(self.configuration)):
                print(log_msg, end="")
        except:
            print(traceback.format_exc())
        finally:
            server.stop(grace=2.0)

    # -- Methods below override ConnectorServicer methods
    def ConfigurationForm(self, request, context):
        """Overrides the ConfigurationForm method from ConnectorServicer.

        Args:
            request: The gRPC request.
            context: The gRPC context.

        Returns:
            common_pb2.ConfigurationFormResponse: An empty configuration form response.
        """
        if not self.configuration:
            self.configuration = {}

        # Not going to use the tester's configuration file
        return common_pb2.ConfigurationFormResponse()

    def Test(self, request, context):
        """Overrides the Test method from ConnectorServicer.

        Args:
            request: The gRPC request.
            context: The gRPC context.

        Returns:
            None: As this method is not implemented.
        """
        return None

    def Schema(self, request, context):
        """Overrides the Schema method from ConnectorServicer.

        Args:
            request: The gRPC request.
            context: The gRPC context.

        Returns:
            connector_sdk_pb2.SchemaResponse: The schema response.
        """

        table_list = {}

        if not self.schema_method:
            return connector_sdk_pb2.SchemaResponse(schema_response_not_supported=True)
        else:
            try:
                configuration = self.configuration if self.configuration else request.configuration
                print_library_log("Initiating the 'schema' method call...", Logging.Level.INFO)
                response = self.schema_method(configuration)
                process_tables(response, table_list)
                return connector_sdk_pb2.SchemaResponse(without_schema=common_pb2.TableList(tables=TABLES.values()))

            except Exception as e:
                tb = traceback.format_exc()
                error_message = f"Error: {str(e)}\n{tb}"
                print_library_log(error_message, Logging.Level.SEVERE)
                raise RuntimeError(error_message) from e

    def Update(self, request, context):
        """Overrides the Update method from ConnectorServicer.

        Args:
            request: The gRPC request.
            context: The gRPC context.

        Yields:
            connector_sdk_pb2.UpdateResponse: The update response.
        """
        configuration = self.configuration if self.configuration else request.configuration
        state = self.state if self.state else json.loads(request.state_json)
        exception_queue = queue.Queue()

        try:
            print_library_log("Initiating the 'update' method call...", Logging.Level.INFO)

            def run_update():
                try:
                    result = self.update_method(configuration=configuration, state=state)
                    # If the customer's update method returns a generator (i.e., uses yield),
                    # exhaust the generator responses, they are None. From this point on, all operations
                    # push update_response to a queue, and we yield from the queue instead.
                    # We return None here intentionally.
                    if isinstance(result, GeneratorType):
                        for _ in result:
                            pass
                    # If the update method doesn't use yield, skip the response returned.
                    else:
                        pass
                except Exception as exc:
                    exception_queue.put(exc)
                finally:
                    Operations.operation_stream.mark_done()

            thread = threading.Thread(target=run_update)
            thread.start()

            # consumer - yield the operations in the operation_stream.
            for response in Operations.operation_stream:
                # checkpoint call always returns list of responses.
                if isinstance(response, list):
                    for res in response:
                        yield res
                    # checkpoint call blocks the queue (see _OperationStream.add method). unblock the queue after yielding all responses.
                    Operations.operation_stream.unblock()
                else:
                    yield response

            thread.join()

            # Check if any exception was raised during the update
            if not exception_queue.empty():
                raise exception_queue.get()

        except TypeError as e:
            if str(e) != "'NoneType' object is not iterable":
                raise e

        except Exception as e:
            tb = traceback.format_exc()
            error_message = f"Error: {str(e)}\n{tb}"
            print_library_log(error_message, Logging.Level.SEVERE)
            raise RuntimeError(error_message) from e

def print_version():
    print_library_log("fivetran_connector_sdk " + __version__)
    sys.exit(0)

def main():
    """The main entry point for the script.
    Parses command line arguments and passes them to connector object methods
    """

    constants.EXECUTED_VIA_CLI = True

    parser = argparse.ArgumentParser(allow_abbrev=False, add_help=True)
    parser._option_string_actions["-h"].help = "Show this help message and exit"

    # Positional
    parser.add_argument("--version", action="store_true", help="Print the version of the fivetran_connector_sdk and exit")
    parser.add_argument("command", nargs="?", help="|".join(VALID_COMMANDS))
    parser.add_argument("project_path", nargs='?', default=os.getcwd(), help="Path to connector project directory")

    # Optional (Not all of these are valid with every mutually exclusive option below)
    parser.add_argument("--state", type=str, default=None, help="Provide state as JSON string or file")
    parser.add_argument("--configuration", type=str, default=None, help="Provide secrets as JSON file")
    parser.add_argument("--api-key", type=str, default=None, help="Provide your base64-encoded API key for deployment")
    parser.add_argument("--destination", type=str, default=None, help="Destination name (aka 'group name')")
    parser.add_argument("--connection", type=str, default=None, help="Connection name (aka 'destination schema')")
    parser.add_argument("-f", "--force", action="store_true", help="Force update an existing connection")
    parser.add_argument("--python-version", "--python", type=str, help=f"Supported Python versions you can use: {SUPPORTED_PYTHON_VERSIONS}. Defaults to {DEFAULT_PYTHON_VERSION}")
    parser.add_argument("--hybrid-deployment-agent-id", type=str, help="The Hybrid Deployment agent within the Fivetran system. If nothing is passed, the default agent of the destination is used.")
    args = parser.parse_args()

    if args.version:
        print_version()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command.lower() == "version":
        print_version()
    elif args.command.lower() == "reset":
        reset_local_file_directory(args)
        sys.exit(0)

    connector_object = find_connector_object(args.project_path)

    if not connector_object:
        sys.exit(1)

    if args.command.lower() == "deploy":
        ft_group = get_destination_group(args)
        ft_connection = get_connection_name(args)
        ft_deploy_key = get_api_key(args)
        python_version = get_python_version(args)
        hd_agent_id = get_hd_agent_id(args)
        configuration, config_path = get_configuration(args)
        get_state(args)

        connector_object.deploy(args.project_path, ft_deploy_key, ft_group, ft_connection, hd_agent_id,
                                configuration, config_path, python_version, args.force)

    elif args.command.lower() == "debug":
        configuration, config_path = get_configuration(args)
        state = get_state(args)
        connector_object.debug(args.project_path, configuration, state)
    else:
        if not suggest_correct_command(args.command):
            raise NotImplementedError(f"Invalid command: {args.command}, see `fivetran --help`")


if __name__ == "__main__":
    main()
