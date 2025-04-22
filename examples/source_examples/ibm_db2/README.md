# IBM DB2 Connector

This connector allows you to sync data from IBM DB2 to a destination using the Fivetran Connector SDK.

The IBM DB2 connector establishes a connection to your DB2 database, reads data from tables, and incrementally syncs changes using timestamp-based tracking. This example connector demonstrates extracting product data but can be modified to work with any DB2 tables. 

### Configuration

Update `configuration.json` with your IBM DB2 details:

```json
{
    "hostname" : "<YOUR_DB2_HOSTNAME>",
    "port" : "<YOUR_DB2_PORT>",
    "database" : "<YOUR_DB2_DATABASE_NAME>",
    "user_id" : "<YOUR_DB2_USER_ID>",
    "password" : "<YOUR_DB2_PASSWORD>",
    "protocol" : "<YOUR_DB2_PROTOCOL>"
}
```
The configuration parameters are:
- `hostname`: The hostname of your IBM DB2 server 
- `port`: The port number for the DB2 connection
- `database`: The name of the DB2 database
- `user_id`: The username to authenticate with DB2
- `password`: The password to authenticate with DB2
- `protocol`: The protocol to use for the connection (default is `TCPIP`)

### Customizing for Your Use Case

To adapt this connector for your needs:

1. **Database Connection**: Replace the placeholders in `configuration.json` with your actual DB2 connection details.
2. **Table Selection**: Update the `schema()` method to define the tables. The example uses a `products` table, but you can modify it to include your tables.
3. **Upsert Logic**: Modify the `update()` method to match your upsert logic. The example uses a simple fetch and upsert approach, but you can customize it to fit your needs.
4. You can add more tables to the `schema()` method and implement the corresponding upsert logic in the `update()` method.
5. The connector has methods for creating sample database and table and inserting test data into the table, but these are only for testing purposes. In a real-world scenario, you would typically have the tables already created in your destination. These methods are:
   - `sample_data_to_insert()`: This method is used to create sample data to be inserted into the table.
   - `create_sample_table_into_db()`: This method is used to create a sample table in the database.
   - `insert_sample_data_into_table()`: This method is used to insert sample data into the table.

> IMPORTANT: This example code has not been fully tested in all environments. If you encounter any issues implementing this connector, please use Fivetran's [Save Me Time](https://support.fivetran.com/hc/en-us/requests/new?isSdkIssue=true)  service. Our support team will be happy to help troubleshoot and finalize any issues with this example.
