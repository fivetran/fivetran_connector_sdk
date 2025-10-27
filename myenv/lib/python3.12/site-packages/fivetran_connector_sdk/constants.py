import os
import re

TESTER_VER = "2.25.0918.001"

WIN_OS = "windows"
ARM_64 = "arm64"
X64 = "x64"

OS_MAP = {
    "darwin": "mac",
    "linux": "linux",
    WIN_OS: WIN_OS
}

ARCH_MAP = {
    "x86_64": X64,
    "amd64": X64,
    ARM_64: ARM_64,
    "aarch64": ARM_64
}

# Global constants - use constants.<Global_variable> to access them as they can be overridden in the
DEBUGGING = False
EXECUTED_VIA_CLI = False
TABLES = {}

TESTER_FILENAME = "sdk_connector_tester.jar"
VERSION_FILENAME = "version.txt"
UPLOAD_FILENAME = "code.zip"
LAST_VERSION_CHECK_FILE = "_last_version_check"
ROOT_LOCATION = ".ft_sdk_connector_tester"
CONFIG_FILE = "_config.json"
OUTPUT_FILES_DIR = "files"
REQUIREMENTS_TXT = "requirements.txt"
EVALUATION_MARKDOWN = "evaluation_report.md"
CONFIGURATION_JSON = "configuration.json"
PYPI_PACKAGE_DETAILS_URL = "https://pypi.org/pypi/fivetran_connector_sdk/json"
ONE_DAY_IN_SEC = 24 * 60 * 60
CHECKPOINT_OP_TIMEOUT_IN_SEC = 120 # seconds
MAX_RETRIES = 3
LOGGING_PREFIX = "Fivetran-Connector-SDK"
LOGGING_DELIMITER = ": "
VIRTUAL_ENV_CONFIG = "pyvenv.cfg"
ROOT_FILENAME = "connector.py"
MAX_RECORDS_IN_BATCH = 100
MAX_BATCH_SIZE_IN_BYTES = 100000 # 100 KB
QUEUE_SIZE = 100

# Compile patterns used in the implementation
WORD_DASH_DOT_PATTERN = re.compile(r'^[\w.-]*$')
NON_WORD_PATTERN = re.compile(r'\W')
WORD_OR_DOLLAR_PATTERN = re.compile(r'[\w$]')
DROP_LEADING_UNDERSCORE = re.compile(r'_+([a-zA-Z]\w*)')
WORD_PATTERN = re.compile(r'\w')

EXCLUDED_DIRS = ["__pycache__", "lib", "include", OUTPUT_FILES_DIR]
EXCLUDED_PIPREQS_DIRS = ["bin,etc,include,lib,Lib,lib64,Scripts,share"]
VALID_COMMANDS = ["debug", "deploy", "reset", "version"]
MAX_ALLOWED_EDIT_DISTANCE_FROM_VALID_COMMAND = 3
COMMANDS_AND_SYNONYMS = {
    "debug": {"test", "verify", "diagnose", "check"},
    "deploy": {"upload", "ship", "launch", "release"},
    "reset": {"reinitialize", "reinitialise", "re-initialize", "re-initialise", "restart", "restore"},
}

CONNECTION_SCHEMA_NAME_PATTERN = r'^[_a-z][_a-z0-9]*$'
PRODUCTION_BASE_URL = "https://api.fivetran.com"
EVALUATE_ENDPOINT = "/v1/evaluate"
INSTALLATION_SCRIPT_MISSING_MESSAGE = "The 'installation.sh' file is missing in the 'drivers' directory. Please ensure that 'installation.sh' is present to properly configure drivers."
INSTALLATION_SCRIPT = "installation.sh"
DRIVERS = "drivers"
JAVA_LONG_MAX_VALUE = 9223372036854775807
MAX_CONFIG_FIELDS = 100
SUPPORTED_PYTHON_VERSIONS = ["3.13", "3.12", "3.11", "3.10", "3.9"]
DEFAULT_PYTHON_VERSION = "3.13"
FIVETRAN_HD_AGENT_ID = "FIVETRAN_HD_AGENT_ID"
UTF_8 = "utf-8"
