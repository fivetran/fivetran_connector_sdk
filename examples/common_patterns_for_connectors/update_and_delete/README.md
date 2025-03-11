## Composite Primary Key Operations

This directory contains examples demonstrating how to handle composite primary keys in Fivetran connectors using the Fivetran Connector SDK. It includes two examples:  
- Update operations with composite primary keys
- Delete operations with composite primary keys

### Composite Primary Keys
A composite primary key consists of two or more columns that, together, uniquely identify each record in a table. In both examples, we're working with tables that have composite primary keys:  
- Update Example: The `product_inventory` table has a composite primary key of (`product_id`, `warehouse_id`)
- Delete Example: The `sample_table` table has a composite primary key of (`id`, `department_id`)

### Update Operations
The [`connector.py`](update_example/connector.py) script demonstrates how to handle update operations with composite primary keys. The script includes functions to update records in the `product_inventory` table based on the composite primary key (`product_id`, `warehouse_id`).

> NOTE: When updating records with composite primary keys, you need to ensure that the primary key values are included in the update payload. If there are multiple primary key columns, you need to include all of them in the payload to identify the record to update. If you include only some primary key columns, all the columns that are identified by those columns will be updated.

### Delete Operations
The [`connector.py`](delete_example/connector.py) script demonstrates how to handle delete operations with composite primary keys. The script includes functions to delete records from the `sample_table` table based on the composite primary key (`id`, `department_id`).

> NOTE: When deleting records with composite primary keys, you need to ensure that the primary key values are included in the delete payload. If there are multiple primary key columns, you need to include all of them in the payload to identify the record to mark as deleted. If you include only some primary key columns, all the columns that are identified by those columns will be marked as deleted.