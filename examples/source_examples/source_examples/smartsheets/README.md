# Smartsheets Connector Example

This example demonstrates how to use the Fivetran Connector SDK to integrate with the Smartsheets API, specifically focusing on the `getSheet` endpoint to sync sheet data into your destination.

## Requirements

- Python 3.7 or later
- Operating System:
  - Windows 10 or later
  - macOS 13 or later
- Smartsheets account with API access
- Valid Smartsheets API token
- Sheet ID of the Smartsheets document you want to sync

## Getting Started

Follow the [Fivetran Connector SDK setup guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Direct integration with Smartsheets API
- Automatic schema discovery based on sheet structure
- Efficient data synchronization
- State checkpointing for reliable resumption
- Error handling and logging
- Support for column type mapping

## Configuration File

The connector requires the following parameters in the configuration file (`configuration.json`):

```json
{
    "smartsheet_api_token": "<YOUR_API_TOKEN>",
    "smartsheet_sheet_id": "<YOUR_SHEET_ID>"
}
```

## Authentication

The connector uses API token authentication. You'll need to:
1. Generate an API token from your Smartsheets account
2. Add the token to your configuration file
3. Keep the token secure and never commit it to version control

## Data Handling

### Sheet Data
- The connector fetches data from a specified Smartsheets document
- Column names and types are automatically mapped to appropriate database types
- Data is synchronized incrementally based on modification timestamps
- Rows are uniquely identified for accurate updates

### Error Handling
- Validation of API credentials
- Connection error management
- Rate limiting compliance
- Comprehensive error logging

## Additional Considerations for Production Use

1. **API Rate Limits**
   - Implement appropriate delays between requests
   - Monitor API usage
   - Handle rate limit errors gracefully

2. **Data Volume**
   - Consider implementing batch processing for large sheets
   - Optimize sync frequency based on update patterns

3. **Error Retry Mechanism**
   - Implement exponential backoff for failed requests
   - Set appropriate timeout values

4. **Monitoring**
   - Add detailed logging
   - Track sync statistics
   - Monitor API response times

5. **Security**
   - Rotate API tokens regularly
   - Use environment variables for sensitive data
   - Implement audit logging

## Disclaimer

This example is provided for learning purposes and should be thoroughly tested and modified before use in a production environment. Fivetran is not responsible for any consequences resulting from the use of this example. 