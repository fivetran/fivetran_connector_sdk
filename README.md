# Requirements
- Python >= 3.9
- Docker Desktop >= 4.23.0 or [Rancher Desktop](https://rancherdesktop.io/) >= 1.12.1

# Installation
Create a virtual environment and install Fivetran Customer SDK package from PyPI:
```
$ pip install fivetran-customer-sdk
```
# Development
1. Create a folder for your project with a `connector.py` file to contain your connector code. Tip: Use one of the examples in the `/examples` folder as a template.
2. Write your connector.
3. The following command will debug your project locally on your computer:
```
$ cd <project-folder-path>
$ fivetran debug
```
3. If you'd like to debug your project in an IDE, add the following code block to `connector.py` file: 
```python
if __name__ == "__main__":
    connector.debug()
```
4. When you run `connector.py` in your local environment, your connector will sync and generate files in `<project-folder-path>/files`. Once the sync is done running, it will persist the records in `warehouse.db` file. This is an instance of DuckDB database. You can connect to it to validate the results of your sync using [DuckDB CLI](https://duckdb.org/docs/api/cli) or [DBeaver](https://duckdb.org/docs/guides/sql_editors/dbeaver).

# Deployment
Use the following command to deploy your project to Fivetran:
```
$ fivetran deploy --deploy-key <DEPLOY-KEY> --group <GROUP-NAME> --connection <CONNECTION-NAME>
```
`DEPLOY-KEY` is your regular [Fivetran API key](https://fivetran.com/docs/rest-api/getting-started#scopedapikey)  

# Examples
There are several examples available under `/examples`:

## hello
Simplest example, append-only

## local
Emulated source, without any calls out to the internet

## specified_types
Declares a schema and upserts all data types

## unspecified_types
Upserts all data types without specifying a schema

## weather
Real API, fetching data from NOAA
