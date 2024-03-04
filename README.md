# Requirements
- Python >= 3.10
- Docker >= 4.23.0

# Installation
Create a virtual environment and install package from PyPI:
```
$ pip install fivetran-customer-sdk
```
# Development
1. Create a folder for your project or open one of the examples in the `/examples` folder
2. To debug the project in your local environment, execute the following command:
```
$ cd <project-folder-path>
$ fivetran debug
```
3. If you'd like to debug the project in an IDE, add a `main` to `main.py` file: 
```python
if __name__ == "__main__":
    connector.debug()
```
4. When you run `main.py` in your local environment, your connector will sync and generate files in `<project-folder-path>/files`. Once the sync is done running, it will persist the records in `warehouse.db` file. This is an instance of DuckDB database. You can connect to it to validate the results of your sync using [DuckDB CLI](https://duckdb.org/docs/api/cli) or [DBeaver](https://duckdb.org/docs/guides/sql_editors/dbeaver).

# Deployment
When your connector is ready to deploy to Fivetran, you can do so with the following command:
```
$ fivetran deploy --deploy-key <DEPLOY-KEY> --group <GROUP-NAME> --connector <CONNECTOR-NAME>
```

# Examples

There are several examples available under `/examples` folder

## hello
Simplest example, append-only

## local
Emulated source, without any calls out to the internet

## weather
Real API, fetching data from NOAA

## specified_types
Declares a schema and upserts all data types

## unspecified_types
Upserts all data types without specifying a schema
