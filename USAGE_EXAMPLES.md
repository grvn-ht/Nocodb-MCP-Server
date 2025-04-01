# Nocodb MCP Server Usage Examples

This document provides examples of how to use the Nocodb MCP Server with Claude or other LLMs.

## Setup

After installing the MCP server in Claude Desktop (as described in the README.md), you can interact with your Nocodb database using the tools provided.

## Important Note on Base ID

This MCP server requires the Nocodb base ID to be set in the `NOCODB_BASE_ID` environment variable. Make sure this is properly configured before using the tools. You don't need to specify the base ID in your requests as it's automatically retrieved from the environment.

## Examples

### 1. Retrieving Records

#### Basic Retrieval

```
You can get all customers from our database.
```

Claude might respond with:

```
I'll retrieve the customers from your Nocodb database.

First, I need to use the retrieve_records tool to get this information:

[Using retrieve_records with parameters:
- table_name: "customers"
- limit: 10 (default)]

Here are the customers I found:
[List of customers would appear here based on the API response]
```

#### Filtered Retrieval

```
Show me all active customers who are older than 30 years.
```

Claude might respond with:

```
I'll search for active customers who are older than 30 years.

[Using retrieve_records with parameters:
- table_name: "customers"
- filters: "(age,gt,30)~and(status,eq,active)"]

Here are the active customers over 30 years old:
[Filtered list of customers would appear here]
```

### 2. Creating Records

```
Add a new customer named John Doe with email john@example.com and age 35.
```

Claude might respond with:

```
I'll add John Doe as a new customer to your database.

[Using create_records with parameters:
- table_name: "customers"
- data: {"name": "John Doe", "email": "john@example.com", "age": 35}]

Customer added successfully.
```

### 3. Updating Records

```
Update customer with ID 123 to change their status to inactive.
```

Claude might respond with:

```
I'll update the customer with ID 123 to inactive status.

[Using update_records with parameters:
- table_name: "customers"
- row_id: "123"
- data: {"status": "inactive"}]

Customer updated successfully.
```

### 4. Deleting Records

```
Delete the customer with ID 456.
```

Claude might respond with:

```
I'll delete the customer with ID 456 from your database.

[Using delete_records with parameters:
- table_name: "customers"
- row_id: "456"]

Customer deleted successfully.
```

### 5. Bulk Operations

#### Bulk Creation

```
Add these three customers to our database:
1. Jane Smith, jane@example.com, 28 years old
2. Bob Johnson, bob@example.com, 42 years old
3. Alice Williams, alice@example.com, 35 years old
```

Claude might respond with:

```
I'll add these three customers to your database using a bulk operation.

[Using create_records with parameters:
- table_name: "customers"
- data: [
    {"name": "Jane Smith", "email": "jane@example.com", "age": 28},
    {"name": "Bob Johnson", "email": "bob@example.com", "age": 42},
    {"name": "Alice Williams", "email": "alice@example.com", "age": 35}
  ]
- bulk: true]

All three customers added successfully.
```

#### Bulk Update

```
Mark customers with IDs 123, 456, and 789 as inactive.
```

Claude might respond with:

```
I'll mark these three customers as inactive in a single operation.

[Using update_records with parameters:
- table_name: "customers"
- data: {"status": "inactive"}
- bulk: true
- bulk_ids: ["123", "456", "789"]]

All three customers were updated to inactive status.
```

#### Bulk Deletion

```
Delete customers with IDs 123, 456, and 789.
```

Claude might respond with:

```
I'll delete these three customers in a single operation.

[Using delete_records with parameters:
- table_name: "customers"
- bulk: true
- bulk_ids: ["123", "456", "789"]]

All three customers were successfully deleted.
```

## Common Scenarios

### Complex Filtering

```
Find all customers who signed up in the last month and have made a purchase over $100.
```

Claude might respond with:

```
I'll search for recent customers with large purchases.

[Using retrieve_records with parameters:
- table_name: "customers"
- filters: "(signup_date,isWithin,pastMonth)~and(purchase_amount,gt,100)"]

Here are the customers who signed up in the last month and made purchases over $100:
[Filtered list would appear here]
```

### Data Analysis

```
What's the average age of our active customers?
```

Claude might respond with:

```
I'll analyze the age of your active customers.

First, I need to retrieve the customer data:

[Using retrieve_records with parameters:
- table_name: "customers"
- filters: "(status,eq,active)"]

Based on the retrieved data, I can calculate that the average age of your active customers is 34.6 years.
```

## Behind the Scenes

When using the MCP server with Nocodb, several things happen behind the scenes:

1. The server retrieves the base ID from the NOCODB_BASE_ID environment variable
2. The server automatically looks up the table ID from the table name
3. All API calls use the v2 API endpoints with the correct paths
4. Authentication is handled via the `xc-token` header (required for Nocodb v2)
5. All operations include error handling to provide meaningful feedback

This abstraction makes it easy to work with Nocodb tables without needing to know the technical details of the API implementation.
