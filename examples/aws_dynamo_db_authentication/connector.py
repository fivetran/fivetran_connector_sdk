# Refer boto3 docs (https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html)

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log
import boto3
import aws_dynamodb_parallel_scan

def getDynamoDbClient(configuration: dict):
    # create security token service client
    sts = boto3.client(
        'sts',
       aws_access_key_id=configuration['AWS_ACCESS_KEY_ID'],
       aws_secret_access_key=configuration['AWS_SECRET_ACCESS_KEY']
    )

    # get role credentials
    credentials = sts.assume_role(
        RoleArn=configuration['ROLE_ARN'],
        RoleSessionName="sdkSession"
    )['Credentials']

    # create dynamoDbClient using role credentials
    dynamodbClient = boto3.client(
        'dynamodb',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name=configuration['REGION']
    )

    return dynamodbClient

def schema(configuration: dict):
    dynamoClient = getDynamoDbClient(configuration)
    schema = []

    try:
        # fetch and iterate over table names
        for table in dynamoClient.list_tables()['TableNames']:
            # fetch tableMetaData, see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/describe_table.html
            tableMetaData = dynamoClient.describe_table(TableName=table)
            tableSchema = {
                "table": table,
                "primary_key": list(map(lambda keySchema: keySchema['AttributeName'], tableMetaData['Table']['KeySchema']))
            }
            schema.append(tableSchema)
    except Exception as e:
        log.severe(str(e))

    return schema

def update(configuration: dict, state: dict):
    dynamoClient = getDynamoDbClient(configuration)

    try:
        # get paginator for parallel scan
        paginator = aws_dynamodb_parallel_scan.get_paginator(dynamoClient)
        # fetch and iterate over table names
        for table in dynamoClient.list_tables()['TableNames']:
            # get pages of 10 items and 4 parallel threads
            page_iterator = paginator.paginate(
                TableName=table,
                Limit=10,
                TotalSegments=4,
                ConsistentRead=True
            )
            for page in page_iterator:
                items = list(map(mapItem, page['Items']))
                for item in items:
                    yield op.upsert(table, item)

        yield op.checkpoint(state)
        log.info('Finished syncing...')
    except Exception as e:
        log.severe(str(e))
        raise

def mapItem(item: dict):
    result = {}
    for key, value in item.items():
        for nested_key, nested_value in value.items():
            if type(nested_value) is list: result[key] = str(nested_value)
            else: result[key] = nested_value
    return result

# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the "fivetran debug" command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug()