import json
import sys

from datetime import datetime, date
from google.protobuf import timestamp_pb2

from fivetran_connector_sdk.constants import (
    JAVA_LONG_MAX_VALUE,
    TABLES
)
from fivetran_connector_sdk.helpers import (
    get_renamed_table_name,
    get_renamed_column_name,
    print_library_log,
)
from fivetran_connector_sdk.logger import Logging
from fivetran_connector_sdk.protos import connector_sdk_pb2, common_pb2
from fivetran_connector_sdk.operation_stream import _OperationStream


_LOG_DATA_TYPE_INFERENCE = {}

class Operations:
    operation_stream = _OperationStream()

    @staticmethod
    def upsert(table: str, data: dict):
        """Updates records with the same primary key if already present in the destination. Inserts new records if not already present in the destination.

        Args:
            table (str): The name of the table.
            data (dict): The data to upsert.

        Returns:
            list[connector_sdk_pb2.UpdateResponse]: A list of update responses.
        """
        table = get_renamed_table_name(table)
        columns = _get_columns(table)
        if not columns:
            for field in data.keys():
                field_name = get_renamed_column_name(field)
                columns[field_name] = common_pb2.Column(
                    name=field_name, type=common_pb2.DataType.UNSPECIFIED, primary_key=False)
            new_table = common_pb2.Table(name=table, columns=columns.values())
            TABLES[table] = new_table

        mapped_data = _map_data_to_columns(data, columns, table)
        record = connector_sdk_pb2.Record(
            schema_name=None,
            table_name=table,
            type=common_pb2.RecordType.UPSERT,
            data=mapped_data
        )

        Operations.operation_stream.add(record)

    @staticmethod
    def update(table: str, modified: dict):
        """Performs an update operation on the specified table with the given modified data.

        Args:
            table (str): The name of the table.
            modified (dict): The modified data.

        Returns:
            connector_sdk_pb2.UpdateResponse: The update response.
        """
        table = get_renamed_table_name(table)
        columns = _get_columns(table)
        mapped_data = _map_data_to_columns(modified, columns, table)
        record = connector_sdk_pb2.Record(
            schema_name=None,
            table_name=table,
            type=common_pb2.RecordType.UPDATE,
            data=mapped_data
        )

        Operations.operation_stream.add(record)

    @staticmethod
    def delete(table: str, keys: dict):
        """Performs a soft delete operation on the specified table with the given keys.

        Args:
            table (str): The name of the table.
            keys (dict): The keys to delete.

        Returns:
            connector_sdk_pb2.UpdateResponse: The delete response.
        """
        table = get_renamed_table_name(table)
        columns = _get_columns(table)
        mapped_data = _map_data_to_columns(keys, columns, table)
        record = connector_sdk_pb2.Record(
            schema_name=None,
            table_name=table,
            type=common_pb2.RecordType.DELETE,
            data=mapped_data
        )

        Operations.operation_stream.add(record)

    @staticmethod
    def checkpoint(state: dict):
        """Checkpoint saves the connector's state. State is a dict which stores information to continue the
        sync from where it left off in the previous sync. For example, you may choose to have a field called
        "cursor" with a timestamp value to indicate up to when the data has been synced. This makes it possible
        for the next sync to fetch data incrementally from that time forward. See below for a few example fields
        which act as parameters for use by the connector code.\n
        {
            "initialSync": true,\n
            "cursor": "1970-01-01T00:00:00.00Z",\n
            "last_resync": "1970-01-01T00:00:00.00Z",\n
            "thread_count": 5,\n
            "api_quota_left": 5000000
        }

        Args:
            state (dict): The state to checkpoint/save.

        Returns:
            connector_sdk_pb2.UpdateResponse: The checkpoint response.
        """
        checkpoint = connector_sdk_pb2.Checkpoint(state_json=json.dumps(state))

        Operations.operation_stream.add(checkpoint)

def _get_columns(table: str) -> dict:
    """Retrieves the columns for the specified table.

    Args:
        table (str): The name of the table.

    Returns:
        dict: The columns for the table.
    """
    columns = {}
    if table in TABLES:
        for column in TABLES[table].columns:
            columns[column.name] = column

    return columns

def _get_table_pk(table: str) -> bool:
    """Retrieves the columns for the specified table.

    Args:
        table (str): The name of the table.

    Returns:
        dict: The columns for the table.
    """
    if table in TABLES:
        for column in TABLES[table].columns:
            if column.primary_key:
                return True
    return False


def _map_data_to_columns(data: dict, columns: dict, table: str = "") -> dict:
    """Maps data to the specified columns.

    Args:
        data (dict): The data to map.
        columns (dict): The columns to map the data to.

    Returns:
        dict: The mapped data.
    """
    mapped_data = {}
    for k, v in data.items():
        key = get_renamed_column_name(k)
        if v is None:
            mapped_data[key] = common_pb2.ValueType(null=True)
        elif (key in columns) and columns[key].type != common_pb2.DataType.UNSPECIFIED:
            map_defined_data_type(columns[key].type, key, mapped_data, v)
        else:
            map_inferred_data_type(key, mapped_data, v, table)
    return mapped_data

def _log_boolean_inference_once(table):
    """Log boolean inference once per table and mark it as logged."""
    key = f"boolean_{table}"
    if _LOG_DATA_TYPE_INFERENCE.get(key, True):
        print_library_log("Fivetran: Boolean Datatype has been inferred for " + table, Logging.Level.INFO, True)
        if not _get_table_pk(table):
            print_library_log("Fivetran: Boolean Datatype inference issue for " + table, Logging.Level.INFO, True)
        _LOG_DATA_TYPE_INFERENCE[key] = False

def map_inferred_data_type(k, mapped_data, v, table=""):
    # We can infer type from the value
    if isinstance(v, bool):
        mapped_data[k] = common_pb2.ValueType(bool=v)
        _log_boolean_inference_once(table)
    elif isinstance(v, int):
        if abs(v) > JAVA_LONG_MAX_VALUE:
            mapped_data[k] = common_pb2.ValueType(float=v)
        else:
            mapped_data[k] = common_pb2.ValueType(long=v)
    elif isinstance(v, float):
        mapped_data[k] = common_pb2.ValueType(float=v)
    elif isinstance(v, bytes):
        mapped_data[k] = common_pb2.ValueType(binary=v)
    elif isinstance(v, list):
        raise ValueError(
            "Values for the columns cannot be of type 'list'. Please ensure that all values are of a supported type. Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#supporteddatatypes")
    elif isinstance(v, dict):
        mapped_data[k] = common_pb2.ValueType(json=json.dumps(v))
    elif isinstance(v, str):
        mapped_data[k] = common_pb2.ValueType(string=v)
    else:
        # Convert arbitrary objects to string
        mapped_data[k] = common_pb2.ValueType(string=str(v))

_TYPE_HANDLERS = {
    common_pb2.DataType.BOOLEAN: lambda val: common_pb2.ValueType(bool=val),
    common_pb2.DataType.SHORT: lambda val: common_pb2.ValueType(short=val),
    common_pb2.DataType.INT: lambda val: common_pb2.ValueType(int=val),
    common_pb2.DataType.LONG: lambda val: common_pb2.ValueType(long=val),
    common_pb2.DataType.DECIMAL: lambda val: common_pb2.ValueType(decimal=val),
    common_pb2.DataType.FLOAT: lambda val: common_pb2.ValueType(float=val),
    common_pb2.DataType.DOUBLE: lambda val: common_pb2.ValueType(double=val),
    common_pb2.DataType.NAIVE_DATE: lambda val: common_pb2.ValueType(naive_date= _parse_naive_date_str(val)),
    common_pb2.DataType.NAIVE_DATETIME: lambda val: common_pb2.ValueType(naive_datetime= _parse_naive_datetime_str(val)),
    common_pb2.DataType.UTC_DATETIME: lambda val: common_pb2.ValueType(utc_datetime= _parse_utc_datetime_str(val)),
    common_pb2.DataType.BINARY: lambda val: common_pb2.ValueType(binary=val),
    common_pb2.DataType.XML: lambda val: common_pb2.ValueType(xml=val),
    common_pb2.DataType.STRING: lambda val: common_pb2.ValueType(string=val if isinstance(val, str) else str(val)),
    common_pb2.DataType.JSON: lambda val: common_pb2.ValueType(json=json.dumps(val))
}

def map_defined_data_type(data_type, k, mapped_data, v):
    handler = _TYPE_HANDLERS.get(data_type)
    if handler:
        mapped_data[k] = handler(v)
    else:
        raise ValueError(f"Unsupported data type encountered: {data_type}. Please use valid data types.")

def _parse_utc_datetime_str(v):
    """
    Accepts a timezone-aware datetime object or a datetime string in one of the following formats:
    - '%Y-%m-%dT%H:%M:%S.%f%z' (e.g., '2025-08-12T15:00:00.123456+00:00')
    - '%Y-%m-%dT%H:%M:%S%z'    (e.g., '2025-08-12T15:00:00+00:00')
    - Same as above formats but with a space instead of 'T' as the separator (e.g., '2025-08-12 15:00:00+00:00')

    Returns a `google.protobuf.timestamp_pb2.Timestamp` object.
    """
    timestamp = timestamp_pb2.Timestamp()
    dt = v
    if not isinstance(v, datetime):
        dt = dt.strip().replace(' ', 'T', 1)  # Replace first space with 'T' for ISO format
        dt = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%f%z" if '.' in dt else "%Y-%m-%dT%H:%M:%S%z")
    timestamp.FromDatetime(dt)
    return timestamp

def _parse_naive_datetime_str(v):
    """
    Accepts a datetime.datetime object or naive datetime string in one of the following formats:
    - '%Y-%m-%dT%H:%M:%S.%f'
    - '%Y-%m-%d %H:%M:%S.%f'
    Returns a `google.protobuf.timestamp_pb2.Timestamp` object.
    """
    timestamp = timestamp_pb2.Timestamp()
    dt = v
    if not isinstance(v, datetime):
        dt = dt.strip().replace(' ', 'T', 1)  # Replace first space with 'T' for ISO format
        dt = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%f" if '.' in dt else "%Y-%m-%dT%H:%M:%S")

    timestamp.FromDatetime(dt)
    return timestamp

def _parse_naive_date_str(v):
    """
    Accepts a `datetime.date` object or a date string ('YYYY-MM-DD'),
    Returns a `google.protobuf.timestamp_pb2.Timestamp` object.
    """
    timestamp = timestamp_pb2.Timestamp()

    if isinstance(v, str):
        dt = datetime.strptime(v, "%Y-%m-%d")
    elif isinstance(v, (datetime, date)):
        dt = datetime.combine(v if isinstance(v, date) else v.date(), datetime.min.time())
    else:
        raise TypeError(f"Expected str, datetime.date, or datetime.datetime, got {type(v)}")

    timestamp.FromDatetime(dt)
    return timestamp

def _yield_check(stack):
    """Checks for the presence of 'yield' in the calling code.
    Args:
        stack: The stack frame to check.
    """

    # Known issue with inspect.getmodule() and yield behavior in a frozen application.
    # When using inspect.getmodule() on stack frames obtained by inspect.stack(), it fails
    # to resolve the modules in a frozen application due to incompatible assumptions about
    # the file paths. This can lead to unexpected behavior, such as yield returning None or
    # the failure to retrieve the module inside a frozen app
    # (Reference: https://github.com/pyinstaller/pyinstaller/issues/5963)

    called_method = stack[0].function
    calling_code = stack[1].code_context[0]
    if f"{called_method}(" in calling_code:
        if 'yield' not in calling_code:
            print_library_log(
                f"Please add 'yield' to '{called_method}' operation on line {stack[1].lineno} in file '{stack[1].filename}'", Logging.Level.SEVERE)
            sys.exit(1)
    else:
        # This should never happen
        raise RuntimeError(
            f"The '{called_method}' function is missing in the connector calling code '{calling_code}'. Please ensure that the '{called_method}' function is properly defined in your code to proceed. Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsmethods")
