import re
import os
import shutil
import sys
import json
import unicodedata
import importlib.util
from datetime import datetime
from unidecode import unidecode
from prompt_toolkit import prompt
from prompt_toolkit.document import Document
from prompt_toolkit.history import FileHistory
from prompt_toolkit.completion import Completer, Completion, PathCompleter, merge_completers

from fivetran_connector_sdk.logger import Logging
from fivetran_connector_sdk import constants
from fivetran_connector_sdk.constants import (
    LOGGING_PREFIX,
    LOGGING_DELIMITER,
    WORD_DASH_DOT_PATTERN,
    NON_WORD_PATTERN,
    WORD_OR_DOLLAR_PATTERN,
    DROP_LEADING_UNDERSCORE,
    WORD_PATTERN,
    ROOT_FILENAME,
    MAX_CONFIG_FIELDS,
    MAX_ALLOWED_EDIT_DISTANCE_FROM_VALID_COMMAND,
    COMMANDS_AND_SYNONYMS,
    VALID_COMMANDS,
    OUTPUT_FILES_DIR, UTF_8
)

RENAMED_TABLE_NAMES = {}
RENAMED_COL_NAMES = {}

def print_library_log(message: str, level: Logging.Level = Logging.Level.INFO, dev_log: bool = False):
    """Logs a library message with the specified logging level.

    Args:
        level (Logging.Level): The logging level.
        message (str): The message to log.
        dev_log (bool): Boolean value to check if it is dev log and shouldn't be visible to customer
    """
    if constants.DEBUGGING or constants.EXECUTED_VIA_CLI:
        if dev_log:
            return
        current_time = datetime.now().strftime("%b %d, %Y %I:%M:%S %p")
        print(f"{Logging.get_color(level)}{current_time} {level.name} {LOGGING_PREFIX}: {message} {Logging.reset_color()}")
    else:
        message_origin = "library_dev" if dev_log else "library"
        escaped_message = json.dumps(LOGGING_PREFIX + LOGGING_DELIMITER + message)
        log_message = f'{{"level":"{level.name}", "message": {escaped_message}, "message_origin": "{message_origin}"}}'
        print(log_message)

def is_special(c):
    """Check if the character is a special character."""
    return not WORD_OR_DOLLAR_PATTERN.fullmatch(c)


def starts_word(previous, current):
    """
    Check if the current character starts a new word based on the previous character.
    """
    return (previous and previous.islower() and current.isupper()) or (
            previous and previous.isdigit() != current.isdigit()
    )


def underscore_invalid_leading_character(name, valid_leading_regex):
    """
    Ensure the name starts with a valid leading character.
    """
    if name and not valid_leading_regex.match(name[0]):
        name = f'_{name}'
    return name


def single_underscore_case(name):
    """
    Convert the input name to single underscore case, replacing special characters and spaces.
    """
    acc = []
    previous = None

    for char_index, c in enumerate(name):
        if char_index == 0 and c == '$':
            acc.append('_')
        elif is_special(c):
            acc.append('_')
        elif c == ' ':
            acc.append('_')
        elif starts_word(previous, c):
            acc.append('_')
            acc.append(c.lower())
        else:
            acc.append(c.lower())

        previous = c

    name = ''.join(acc)
    return re.sub(r'_+', '_', name)


def contains_only_word_dash_dot(name):
    """
    Check if the name contains only word characters, dashes, and dots.
    """
    return bool(WORD_DASH_DOT_PATTERN.fullmatch(name))


def transliterate(name):
    """
    Transliterate the input name if it contains non-word, dash, or dot characters.
    """
    if contains_only_word_dash_dot(name):
        return name
    # Step 1: Normalize the name to NFD form (decomposed form)
    normalized_name = unicodedata.normalize('NFD', name)
    # Step 2: Remove combining characters (diacritics, accents, etc.)
    normalized_name = ''.join(char for char in normalized_name if not unicodedata.combining(char))
    # Step 3: Normalize back to NFC form (composed form)
    normalized_name = unicodedata.normalize('NFC', normalized_name)
    # Step 4: Convert the string to ASCII using `unidecode` (removes any remaining non-ASCII characters)
    normalized_name = unidecode(normalized_name)
    # Step 5: Return the normalized name
    return normalized_name


def redshift_safe(name):
    """
    Make the name safe for use in Redshift.
    """
    name = transliterate(name)
    name = NON_WORD_PATTERN.sub('_', name)
    name = single_underscore_case(name)
    name = underscore_invalid_leading_character(name, WORD_PATTERN)
    return name


def safe_drop_underscores(name):
    """
    Drop leading underscores if the name starts with valid characters after sanitization.
    """
    safe_name = redshift_safe(name)
    match = DROP_LEADING_UNDERSCORE.match(safe_name)
    if match:
        return match.group(1)
    return safe_name


def get_renamed_table_name(source_table):
    """
    Process a source table name to ensure it conforms to naming rules.
    """
    if source_table not in RENAMED_TABLE_NAMES:
        RENAMED_TABLE_NAMES[source_table] = safe_drop_underscores(source_table)

    return RENAMED_TABLE_NAMES[source_table]


def get_renamed_column_name(source_column):
    """
    Process a source column name to ensure it conforms to naming rules.
    """
    if source_column not in RENAMED_COL_NAMES:
        RENAMED_COL_NAMES[source_column] = redshift_safe(source_column)

    return RENAMED_COL_NAMES[source_column]


# Functions used by main method only

def find_connector_object(project_path):
    """Finds the connector object in the given project path.
    Args:
        project_path (str): The path to the project.
    """

    sys.path.append(project_path)  # Allows python interpreter to search for modules in this path
    module_name = "connector_connector_code"
    connector_py = os.path.join(project_path, ROOT_FILENAME)
    try:
        spec = importlib.util.spec_from_file_location(module_name, connector_py)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        for obj in dir(module):
            if not obj.startswith('__'):  # Exclude built-in attributes
                obj_attr = getattr(module, obj)
                if '<fivetran_connector_sdk.Connector object at' in str(obj_attr):
                    return obj_attr
    except FileNotFoundError:
        print_library_log(
            "The connector object is missing in the current directory. Please ensure that you are running the command from correct directory or that you have defined a connector object using the correct syntax in your `connector.py` file. Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector",
            Logging.Level.SEVERE)
        return None

    print_library_log(
        "The connector object is missing. Please ensure that you have defined a connector object using the correct syntax in your `connector.py` file. Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector",
        Logging.Level.SEVERE)
    return None


def suggest_correct_command(input_command: str) -> bool:
    # for typos
    # calculate the edit distance of the input command (lowercased) with each of the valid commands
    edit_distances_of_commands = sorted(
        [(command, edit_distance(command, input_command.lower())) for command in VALID_COMMANDS], key=lambda x: x[1])

    if edit_distances_of_commands[0][1] <= MAX_ALLOWED_EDIT_DISTANCE_FROM_VALID_COMMAND:
        # if the closest command is within the max allowed edit distance, we suggest that command
        # threshold is kept to prevent suggesting a valid command for an obvious wrong command like `fivetran iknowthisisntacommandbuttryanyway`
        print_suggested_command_message(edit_distances_of_commands[0][0], input_command)
        return True

    # for synonyms
    for (command, synonyms) in COMMANDS_AND_SYNONYMS.items():
        # check if the input command (lowercased) is a recognised synonym of the valid commands, if yes, suggest that command
        if input_command.lower() in synonyms:
            print_suggested_command_message(command, input_command)
            return True

    return False


def print_suggested_command_message(valid_command: str, input_command: str) -> None:
    print_library_log(f"`fivetran {input_command}` is not a valid command.", Logging.Level.SEVERE)
    print_library_log(f"Did you mean `fivetran {valid_command}`?", Logging.Level.SEVERE)
    print_library_log("Use `fivetran --help` for more details.", Logging.Level.SEVERE)


def edit_distance(first_string: str, second_string: str) -> int:
    first_string_length: int = len(first_string)
    second_string_length: int = len(second_string)

    # Initialize the previous row of distances (for the base case of an empty first string) 'previous_row[j]' holds
    # the edit distance between an empty prefix of 'first_string' and the first 'j' characters of 'second_string'.
    # The first row is filled with values [0, 1, 2, ..., second_string_length]
    previous_row: list[int] = list(range(second_string_length + 1))

    # Rest of the rows
    for first_string_index in range(1, first_string_length + 1):
        # Start the current row with the distance for an empty second string
        current_row: list[int] = [first_string_index]

        # Iterate over each character in the second string
        for second_string_index in range(1, second_string_length + 1):
            if first_string[first_string_index - 1] == second_string[second_string_index - 1]:
                # If characters match, no additional cost
                current_row.append(previous_row[second_string_index - 1])
            else:
                # Minimum cost of insertion, deletion, or substitution
                current_row.append(
                    1 + min(current_row[-1], previous_row[second_string_index], previous_row[second_string_index - 1]))

        # Move to the next row
        previous_row = current_row

    # The last value in the last row is the edit distance
    return previous_row[second_string_length]

class EnvironmentVariableCompleter(Completer):
    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        if text.startswith('$'):
            # Get the variable name part (text after the '$')
            var_name = text[1:]

            # Suggest all environment variables that start with the typed name
            for env_var in os.environ:
                if env_var.startswith(var_name):
                    yield Completion(
                        f'${env_var}',
                        start_position=-len(text)
                    )

class EnvVarPathCompleter(Completer):
    def get_completions(self, document, complete_event):
        text_before_cursor = document.text_before_cursor
        expanded_text = os.path.expandvars(text_before_cursor)

        # Create a new document for the PathCompleter to use
        expanded_document = Document(
            text=expanded_text, cursor_position=len(expanded_text)
        )

        # Use a standard PathCompleter on our new, temporary document
        path_completer = PathCompleter(expanduser=True)
        yield from path_completer.get_completions(expanded_document, complete_event)


def get_input_from_cli(prompt_txt: str, default_value: str, hide_value = False) -> str:
    """
    Prompts the user for input.
    """
    final_completer = merge_completers([EnvironmentVariableCompleter(), EnvVarPathCompleter()])
    history = FileHistory(os.path.join(os.path.expanduser('~'), '.fivetran_history'))
    if default_value:
        if hide_value:
            default_value_hidden = default_value[0:8] + "********"
            value = prompt(f"{prompt_txt} [Default : {default_value_hidden}]: ",
                           completer=final_completer, history=history, complete_while_typing=False, complete_style='readline_like'
                           ).strip() or default_value
        else:
            value = prompt(f"{prompt_txt} [Default : {default_value}]: ",
                           completer=final_completer, history=history, complete_while_typing=False, complete_style='readline_like'
                           ).strip() or default_value
    else:
        value = prompt(f"{prompt_txt}: ",
                       completer=final_completer, history=history, complete_while_typing=False, complete_style='readline_like'
                       ).strip()

    if not value:
        raise ValueError("Missing required input: Expected a value but received None")
    return os.path.expandvars(value)

def validate_and_load_configuration(project_path, configuration):
    if configuration:
        configuration = os.path.expanduser(configuration)
        if os.path.isabs(configuration):
            json_filepath = os.path.abspath(configuration)
        else:
            relative_path = os.path.join(project_path, configuration)
            json_filepath = os.path.abspath(str(relative_path))
        if os.path.isfile(json_filepath):
            with open(json_filepath, 'r', encoding=UTF_8) as fi:
                try:
                    configuration = json.load(fi)
                except:
                    raise ValueError(
                        "Configuration must be provided as a JSON file. Please check your input. Reference: "
                        "https://fivetran.com/docs/connectors/connector-sdk/detailed-guide#workingwithconfigurationjsonfile")
            if len(configuration) > MAX_CONFIG_FIELDS:
                raise ValueError(f"Configuration field count exceeds maximum of {MAX_CONFIG_FIELDS}. Reduce the field count.")
        else:
            raise ValueError(
                f"Configuration path is incorrect, cannot find file at the location {json_filepath}")
    else:
        print_library_log("No configuration file passed.", Logging.Level.INFO)
        configuration = {}
    return configuration


def validate_and_load_state(args, state):
    if state:
        json_filepath =  os.path.abspath(os.path.join(args.project_path, args.state))
    else:
        json_filepath = os.path.join(args.project_path, "files", "state.json")

    if os.path.exists(json_filepath):
        if os.path.isfile(json_filepath):
            with open(json_filepath, 'r', encoding=UTF_8) as fi:
                state = json.load(fi)
        elif state.lstrip().startswith("{"):
            state = json.loads(state)
    else:
        state = {}
    return state


def reset_local_file_directory(args):
    files_path = os.path.join(args.project_path, OUTPUT_FILES_DIR)
    if args.force:
        confirm = "y"
    else:
        confirm = input(
            "This will delete your current state and `warehouse.db` files. Do you want to continue? (Y/n): ")
    if confirm.lower() != "y":
        print_library_log("Reset canceled")
    else:
        try:
            if os.path.exists(files_path) and os.path.isdir(files_path):
                shutil.rmtree(files_path)
            print_library_log("Reset Successful")
        except Exception as e:
            print_library_log("Reset Failed", Logging.Level.SEVERE)
            raise e
