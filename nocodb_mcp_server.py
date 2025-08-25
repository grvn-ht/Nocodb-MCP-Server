#!/usr/bin/env python3
"""
Nocodb MCP Server

This server provides tools to interact with a Nocodb database through the Model Context Protocol.
It offers CRUD operations (Create, Read, Update, Delete) for Nocodb tables.

Environment Variables:
- NOCODB_URL: The base URL of your Nocodb instance
- NOCODB_API_TOKEN: The API token for authentication
- NOCODB_BASE_ID: The ID of the Nocodb base to use

Usage:
1. Ensure the environment variables are set
2. Run this script directly or use the MCP CLI
"""

import os
import json
import httpx
import logging
from typing import Dict, List, Optional, Union, Any
from pydantic import BaseModel, Field
from mcp.server.fastmcp import FastMCP, Context
import sys
import re

print(f"Python version: {sys.version}")
print(f"Starting NocoDB MCP server")
print(f"Args: {sys.argv}")
print(f"Env vars: NOCODB_URL exists: {'NOCODB_URL' in os.environ}")

# Set up logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s [%(levelname)s] %(message)s',
#     datefmt='%Y-%m-%d %H:%M:%S'
# )
logger = logging.getLogger("nocodb-mcp")

# Create the MCP server
mcp = FastMCP("Nocodb MCP Server", log_level="ERROR")

# Hardcoded base ID
NOCODB_BASE_ID = os.environ.get("NOCODB_BASE_ID")


async def get_nocodb_client(ctx: Context = None) -> httpx.AsyncClient:
    """Create and return an authenticated httpx client for Nocodb API requests"""
    # Get environment variables
    nocodb_url = os.environ.get("NOCODB_URL")
    api_token = os.environ.get("NOCODB_API_TOKEN")
    
    if not nocodb_url:
        error_msg = "NOCODB_URL environment variable is not set"
        logger.error(error_msg)
        raise ValueError(error_msg)
    if not api_token:
        error_msg = "NOCODB_API_TOKEN environment variable is not set"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Remove trailing slash from URL if present
    if nocodb_url.endswith("/"):
        nocodb_url = nocodb_url[:-1]
    
    # Create httpx client with authentication headers - using xc-token as required by Nocodb v2 API
    headers = {
        "xc-token": api_token,
        "Content-Type": "application/json"
    }
    
    logger.debug(f"Creating client for Nocodb API at {nocodb_url}")
    return httpx.AsyncClient(base_url=nocodb_url, headers=headers, timeout=30.0)


async def get_table_id(client: httpx.AsyncClient, table_name: str) -> str:
    """Get the table ID from the table name using the hardcoded base ID"""
    if not NOCODB_BASE_ID:
        error_msg = "NOCODB_BASE_ID environment variable is not set"
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    logger.info(f"Looking up table ID for '{table_name}' in base '{NOCODB_BASE_ID}'")
    
    # Get the list of tables in the base
    try:
        response = await client.get(f"/api/v2/meta/bases/{NOCODB_BASE_ID}/tables")
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        error_msg = f"Failed to get tables list: HTTP {e.response.status_code}"
        logger.error(error_msg)
        logger.debug(f"Response body: {e.response.text}")
        raise ValueError(error_msg)
    
    tables = response.json().get("list", [])
    logger.debug(f"Found {len(tables)} tables in base")
    
    # Find the table with the matching name
    for table in tables:
        if table.get("title") == table_name:
            table_id = table.get("id")
            logger.info(f"Found table ID for '{table_name}': {table_id}")
            return table_id
    
    error_msg = f"Table '{table_name}' not found in base '{NOCODB_BASE_ID}'"
    logger.error(error_msg)
    logger.debug(f"Available tables: {[t.get('title') for t in tables]}")
    raise ValueError(error_msg)


@mcp.tool()
async def retrieve_records(
    table_name: str,
    row_id: Optional[str] = None,
    filters: Optional[str] = None,
    limit: Optional[int] = 10,
    offset: Optional[int] = 0,
    sort: Optional[str] = None,
    fields: Optional[str] = None,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Retrieve one or multiple records from a Nocodb table.
    
    This tool allows you to query data from your Nocodb database tables with various options
    for filtering, sorting, and pagination. It supports both single record retrieval by ID
    and multi-record retrieval with conditions.
    
    Parameters:
    - table_name: Name of the table to query
    - row_id: (Optional) Specific row ID to retrieve a single record
    - filters: (Optional) Filter conditions in Nocodb format, e.g. "(column,eq,value)"
                See Nocodb docs for comparison operators like eq, neq, gt, lt, etc.
    - limit: (Optional) Maximum number of records to return (default: 10)
    - offset: (Optional) Number of records to skip for pagination (default: 0)
    - sort: (Optional) Column to sort by, use "-" prefix for descending order
    - fields: (Optional) Comma-separated list of fields to include in the response
    
    Returns:
    - Dictionary containing the retrieved record(s) or error information
    
    Examples:
    1. Get all records from a table (limited to 10):
       retrieve_records(table_name="customers")
       
    2. Get a specific record by ID:
       retrieve_records(table_name="customers", row_id="123")
       
    3. Filter records with conditions:
       retrieve_records(
           table_name="customers", 
           filters="(age,gt,30)~and(status,eq,active)"
       )
       
    4. Paginate results:
       retrieve_records(table_name="customers", limit=20, offset=40)
       
    5. Sort results:
       retrieve_records(table_name="customers", sort="-created_at")
       
    6. Select specific fields:
       retrieve_records(table_name="customers", fields="id,name,email")
    """
    logger.info(f"Retrieve records request for table '{table_name}'")
    
    # Parameter validation
    if not table_name:
        error_msg = "Table name is required"
        logger.error(error_msg)
        return {"error": True, "message": error_msg}
    
    # normalize table name so first letter of each word is uppercase
    table_name = ' '.join(word.capitalize() for word in table_name.split(' '))
    
    # Log query parameters for debugging
    params_info = {
        "row_id": row_id,
        "filters": filters,
        "limit": limit,
        "offset": offset,
        "sort": sort,
        "fields": fields
    }
    logger.debug(f"Query parameters: {params_info}")
    
    try:
        client = await get_nocodb_client(ctx)
        
        # Get the table ID from the table name
        table_id = await get_table_id(client, table_name)
        
        # Determine the endpoint based on whether we're fetching a single record or multiple
        if row_id:
            # Single record endpoint
            url = f"/api/v2/tables/{table_id}/records/{row_id}"
            logger.info(f"Retrieving single record with ID: {row_id}")
            response = await client.get(url)
        else:
            # Multiple records endpoint
            url = f"/api/v2/tables/{table_id}/records"
            
            # Build query parameters
            params = {}
            if limit is not None:
                params["limit"] = limit
            if offset is not None:
                params["offset"] = offset
            if sort:
                params["sort"] = sort
            if fields:
                params["fields"] = fields
            if filters:
                params["where"] = filters
            
            logger.info(f"Retrieving records with params: {params}")    
            response = await client.get(url, params=params)
        
        # Handle response
        response.raise_for_status()
        result = response.json()
        
        # Print the number of records retrieved
        if row_id:
            # For single record retrieval
            record_count = 1 if result and not result.get("error") else 0
            logger.info(f"Retrieved {record_count} record from table '{table_name}'")
        else:
            # For multiple records retrieval
            records = result.get("list", [])
            record_count = len(records)
            logger.info(f"Retrieved {record_count} records from table '{table_name}'")
            
            # Log pagination info if available
            if "pageInfo" in result:
                page_info = result.get("pageInfo", {})
                logger.debug(f"Page info: {page_info}")
        
        return result
        
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP error {e.response.status_code} retrieving records from '{table_name}'"
        logger.error(error_msg)
        logger.debug(f"Response body: {e.response.text}")
        return {
            "error": True,
            "status_code": e.response.status_code,
            "message": f"HTTP error: {e.response.text}"
        }
    except Exception as e:
        error_msg = f"Error retrieving records from '{table_name}': {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.debug(traceback.format_exc())
        return {
            "error": True,
            "message": f"Error: {str(e)}"
        }
    finally:
        if 'client' in locals():
            await client.aclose()


@mcp.tool()
async def create_records(
    table_name: str,
    data: Dict[str, Any],
    bulk: bool = False,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Create one or multiple records in a Nocodb table.
    
    This tool allows you to insert new data into your Nocodb database tables.
    It supports both single record creation and bulk operations for inserting
    multiple records at once.
    
    Parameters:
    - table_name: Name of the table to insert into
    - data: For single record: Dict with column:value pairs
            For bulk creation: List of dicts with column:value pairs
    - bulk: (Optional) Set to True for bulk creation with multiple records
    
    Returns:
    - Dictionary containing the created record(s) or error information
    
    Examples:
    1. Create a single record:
       create_records(
           table_name="customers",
           data={"name": "John Doe", "email": "john@example.com", "age": 35}
       )
       
    2. Create multiple records in bulk:
       create_records(
           table_name="customers",
           data=[
               {"name": "John Doe", "email": "john@example.com", "age": 35},
               {"name": "Jane Smith", "email": "jane@example.com", "age": 28}
           ],
           bulk=True
       )
    """
    logger.info(f"Create records request for table '{table_name}'")
    
    # Parameter validation
    if not table_name:
        error_msg = "Table name is required"
        logger.error(error_msg)
        return {"error": True, "message": error_msg}
        
    if not data:
        error_msg = "Data is required for record creation"
        logger.error(error_msg)
        return {"error": True, "message": error_msg}
    
    # Ensure data is a list for bulk, or single dict otherwise
    original_data = data # Keep a reference before potential modification
    if bulk:
        if not isinstance(data, list):
            logger.warning(f"Bulk creation requested but data is not a list, converting single record to list")
            data = [data]
        elif not data: # Handle empty list for bulk
             error_msg = "Data list cannot be empty for bulk creation"
             logger.error(error_msg)
             return {"error": True, "message": error_msg}
    elif isinstance(data, list):
        logger.warning(f"Single record creation requested but data is a list, using first item only")
        data = data[0] if data else {}
        if not data:
            error_msg = "Data dictionary cannot be empty for single record creation"
            logger.error(error_msg)
            return {"error": True, "message": error_msg}
            
    # Log operation details
    operation_type = "bulk" if bulk else "single record"
    # Use original_data for accurate count if it was modified
    record_count = len(data) if isinstance(data, list) else 1 
    logger.debug(f"Creating {record_count} records ({operation_type})")
    
    try:
        logger.info(f"Creating {record_count} records ({operation_type})")
        client = await get_nocodb_client(ctx)
        
        # Get the table ID from the table name
        table_id = await get_table_id(client, table_name)
        
        # Determine the endpoint based on whether we're doing bulk creation or single record
        if bulk:
            # Bulk creation endpoint
            url = f"/api/v2/tables/{table_id}/records/bulk"
            logger.info(f"Performing bulk creation of {len(data)} records")
        else:
            # Single record creation endpoint
            url = f"/api/v2/tables/{table_id}/records"
            logger.info(f"Creating single record")
        
        logger.info(f"Sending data to {url}")
        # Make the request - Pass the Python dictionary/list directly to the json parameter
        response = await client.post(url, json=data) 
        
        logger.info(f"Response Status: {response.status_code}")
        logger.debug(f"Response Body: {response.text}")
        # Handle response
        response.raise_for_status()
        result = response.json()
        
        logger.info(f"Successfully created record(s) in table '{table_name}'")
        return result
        
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP error {e.response.status_code} creating records in '{table_name}'"
        logger.error(error_msg)
        logger.error(f"Request Data: {data}") # Log data on error
        logger.error(f"Response body: {e.response.text}")
        return {
            "error": True,
            "status_code": e.response.status_code,
            "message": f"HTTP error: {e.response.text}"
        }
    except ValueError as e: # Catch errors from get_table_id or data validation
        error_msg = f"Error creating records in '{table_name}': {str(e)}"
        logger.error(error_msg)
        return {
            "error": True,
            "message": error_msg
        }
    except Exception as e:
        error_msg = f"Error creating records in '{table_name}': {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.debug(traceback.format_exc())
        return {
            "error": True,
            "message": f"Error: {str(e)}"
        }
    finally:
        if 'client' in locals():
            await client.aclose()


@mcp.tool()
async def update_records(
    table_name: str,
    row_id: Optional[str] = None,
    data: Dict[str, Any] = None,
    bulk: bool = False,
    bulk_ids: Optional[List[str]] = None,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Update one or multiple records in a Nocodb table.
    
    This tool allows you to modify existing data in your Nocodb database tables.
    It supports both single record updates by ID and bulk updates for multiple records.
    
    Parameters:
    - table_name: Name of the table to update
    - row_id: ID of the record to update (required for single record update)
    - data: Dictionary with column:value pairs to update
    - bulk: (Optional) Set to True for bulk updates
    - bulk_ids: (Optional) List of record IDs to update when bulk=True
    
    Returns:
    - Dictionary containing the updated record(s) or error information
    
    Examples:
    1. Update a single record by ID:
       update_records(
           table_name="customers",
           row_id="123",
           data={"name": "John Smith", "status": "inactive"}
       )
       
    2. Update multiple records in bulk by IDs:
       update_records(
           table_name="customers",
           data={"status": "inactive"},  # Same update applied to all records
           bulk=True,
           bulk_ids=["123", "456", "789"]
       )
    """
    logger.info(f"Update records request for table '{table_name}'")
    
    # Parameter validation
    if not table_name:
        error_msg = "Table name is required"
        logger.error(error_msg)
        return {"error": True, "message": error_msg}
        
    if not data:
        error_msg = "Data parameter is required for updates"
        logger.error(error_msg)
        return {"error": True, "message": error_msg}
    
    # Validate update operation parameters
    if bulk and not bulk_ids:
        error_msg = "Bulk IDs are required for bulk updates"
        logger.error(error_msg)
        return {"error": True, "message": error_msg}
    elif not bulk and not row_id:
        error_msg = "Row ID is required for single record update"
        logger.error(error_msg)
        return {"error": True, "message": error_msg}
    
    # Log operation details
    operation_type = "bulk" if bulk else "single record"
    if bulk:
        logger.debug(f"Updating {len(bulk_ids)} records in bulk")
    else:
        logger.debug(f"Updating single record with ID: {row_id}")
        
    try:
        client = await get_nocodb_client(ctx)
        
        # Get the table ID from the table name
        table_id = await get_table_id(client, table_name)
        
        # Determine the endpoint based on whether we're doing bulk update or single record
        if bulk and bulk_ids:
            # Bulk update by IDs endpoint
            url = f"/api/v2/tables/{table_id}/records/bulk"
            # For bulk updates with IDs, we need to include both ids and data
            payload = {"ids": bulk_ids, "data": data}
            logger.info(f"Performing bulk update of {len(bulk_ids)} records")
            response = await client.patch(url, json=payload)
        elif row_id:
            # Single record update endpoint
            url = f"/api/v2/tables/{table_id}/records/{row_id}"
            logger.info(f"Updating record with ID: {row_id}")
            response = await client.patch(url, json=data)
        else:
            error_msg = "Either row_id (for single update) or bulk=True with bulk_ids (for bulk update) must be provided"
            logger.error(error_msg)
            return {
                "error": True,
                "message": error_msg
            }
        
        # Handle response
        response.raise_for_status()
        result = response.json()
        
        logger.info(f"Successfully updated record(s) in table '{table_name}'")
        return result
        
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP error {e.response.status_code} updating records in '{table_name}'"
        logger.error(error_msg)
        logger.debug(f"Response body: {e.response.text}")
        return {
            "error": True,
            "status_code": e.response.status_code,
            "message": f"HTTP error: {e.response.text}"
        }
    except Exception as e:
        error_msg = f"Error updating records in '{table_name}': {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.debug(traceback.format_exc())
        return {
            "error": True,
            "message": f"Error: {str(e)}"
        }
    finally:
        if 'client' in locals():
            await client.aclose()


@mcp.tool()
async def delete_records(
    table_name: str,
    row_id: Optional[str] = None,
    bulk: bool = False,
    bulk_ids: Optional[List[str]] = None,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Delete one or multiple records from a Nocodb table.
    
    This tool allows you to remove data from your Nocodb database tables.
    It supports both single record deletion by ID and bulk deletions for multiple records.
    
    Parameters:
    - table_name: Name of the table to delete from
    - row_id: ID of the record to delete (required for single record deletion)
    - bulk: (Optional) Set to True for bulk deletion
    - bulk_ids: (Optional) List of record IDs to delete when bulk=True
    
    Returns:
    - Dictionary containing the operation result or error information
    
    Examples:
    1. Delete a single record by ID:
       delete_records(
           table_name="customers",
           row_id="123"
       )
       
    2. Delete multiple records in bulk by IDs:
       delete_records(
           table_name="customers",
           bulk=True,
           bulk_ids=["123", "456", "789"]
       )
    """
    logger.info(f"Delete records request for table '{table_name}'")
    
    # Parameter validation
    if not table_name:
        error_msg = "Table name is required"
        logger.error(error_msg)
        return {"error": True, "message": error_msg}
    
    # normalize table name so first letter of each word is uppercase
    table_name = ' '.join(word.capitalize() for word in table_name.split(' '))

    # Validate delete operation parameters
    if bulk and not bulk_ids:
        error_msg = "Bulk IDs are required for bulk deletion"
        logger.error(error_msg)
        return {"error": True, "message": error_msg}
    elif not bulk and not row_id:
        error_msg = "Row ID is required for single record deletion"
        logger.error(error_msg)
        return {"error": True, "message": error_msg}
    
    # Log operation details
    operation_type = "bulk" if bulk else "single record"
    if bulk:
        logger.debug(f"Deleting {len(bulk_ids)} records in bulk")
    else:
        logger.debug(f"Deleting single record with ID: {row_id}")
        
    try:
        client = await get_nocodb_client(ctx)
        
        # Get the table ID from the table name
        table_id = await get_table_id(client, table_name)
        
        # Determine the endpoint based on whether we're doing bulk deletion or single record
        if bulk and bulk_ids:
            # Bulk deletion endpoint
            url = f"/api/v2/tables/{table_id}/records/bulk"
            # For bulk deletions with IDs, we need to send the ids in the request body
            logger.info(f"Performing bulk deletion of {len(bulk_ids)} records")
            response = await client.request("DELETE", url, json={"ids": bulk_ids}) # Use explicit DELETE with body
        elif row_id:
            # Single record deletion endpoint
            url = f"/api/v2/tables/{table_id}/records/{row_id}"
            logger.info(f"Deleting record with ID: {row_id}")
            response = await client.delete(url)
        else:
            error_msg = "Either row_id (for single deletion) or bulk=True with bulk_ids (for bulk deletion) must be provided"
            logger.error(error_msg)
            return {
                "error": True,
                "message": error_msg
            }
        
        # Handle response
        response.raise_for_status()
        
        # Delete operations may return 200 or 204 with different body content
        if response.status_code == 204: # No content
             result = {"success": True, "message": "Record(s) deleted successfully"}
        else:
            try:
                result = response.json()
                # NocoDB bulk delete might return a number (count) or an object
                if isinstance(result, (int, float)):
                    result = {"success": True, "message": f"{result} record(s) deleted successfully"}
                elif not isinstance(result, dict): # Handle unexpected formats
                     result = {"success": True, "message": "Record(s) deleted successfully", "response_data": result}

            except json.JSONDecodeError:
                logger.warning("Delete operation returned non-empty, non-JSON response body")
                result = {"success": True, "message": "Record(s) deleted successfully (non-JSON response)"}

        logger.info(f"Successfully deleted record(s) from table '{table_name}'")
        return result
        
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP error {e.response.status_code} deleting records from '{table_name}'"
        logger.error(error_msg)
        logger.debug(f"Response body: {e.response.text}")
        return {
            "error": True,
            "status_code": e.response.status_code,
            "message": f"HTTP error: {e.response.text}"
        }
    except Exception as e:
        error_msg = f"Error deleting records from '{table_name}': {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.debug(traceback.format_exc())
        return {
            "error": True,
            "message": f"Error: {str(e)}"
        }
    finally:
        if 'client' in locals():
            await client.aclose()


@mcp.tool()
async def get_schema(
    table_name: str,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Retrieve the schema (columns) of a Nocodb table.
    
    This tool fetches the metadata for a specific table, including details about its columns.
    
    Parameters:
    - table_name: Name of the table to get the schema for
    
    Returns:
    - Dictionary containing the table schema or error information. 
      The schema details, including the list of columns, are typically nested within the response.
    
    Example:
    Get the schema for the "products" table:
       get_schema(table_name="products")
    """
    logger.info(f"Get schema request for table '{table_name}'")

    # Parameter validation
    if not table_name:
        error_msg = "Table name is required"
        logger.error(error_msg)
        return {"error": True, "message": error_msg}
    
    try:
        client = await get_nocodb_client(ctx)
        
        # Get the table ID from the table name
        table_id = await get_table_id(client, table_name)
        
        # Fetch table metadata using the table ID
        # The endpoint /api/v2/meta/tables/{tableId} provides table details including columns
        url = f"/api/v2/meta/tables/{table_id}"
        logger.info(f"Retrieving schema for table ID: {table_id} using url {url}")
        
        response = await client.get(url)
        response.raise_for_status()
        
        result = response.json()
        
        # Log success and potentially the number of columns found
        columns = result.get("columns", [])
        logger.info(f"Successfully retrieved schema for table '{table_name}'. Found {len(columns)} columns.")
        logger.debug(f"Schema details: {result}") # Log full schema for debugging if needed
        
        return result # Return the full table metadata which includes the columns

    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP error {e.response.status_code} retrieving schema for '{table_name}'"
        logger.error(error_msg)
        logger.debug(f"Response body: {e.response.text}")
        return {
            "error": True,
            "status_code": e.response.status_code,
            "message": f"HTTP error: {e.response.text}"
        }
    except ValueError as e: # Catch errors from get_table_id
        error_msg = f"Error retrieving schema for '{table_name}': {str(e)}"
        logger.error(error_msg)
        return {
            "error": True,
            "message": error_msg
        }
    except Exception as e:
        error_msg = f"Error retrieving schema for '{table_name}': {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.debug(traceback.format_exc())
        return {
            "error": True,
            "message": f"Error: {str(e)}"
        }
    finally:
        if 'client' in locals():
            await client.aclose()


@mcp.tool()
async def update_field(
    field_id: str,
    field_data: Dict[str, Any],
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Update the details of a specific field in a NocoDB base.

    This tool allows updating a field’s metadata (title, type, default value, description, and options).

    Parameters:
    - field_id: The unique identifier of the field to update
    - field_data: A dictionary with the field properties to update.
      Example:
      {
        "title": "New Field Name",
        "type": "Number",
        "default_value": "0",
        "description": "A numeric field",
        "options": { "precision": "2" }
      }

    Returns:
    - Dictionary containing the updated field metadata or error information.

    Example:
    Update a field:
       update_field(
         field_id="f456",
         field_data={
           "title": "New Field",
           "type": "SingleLineText",
           "default_value": "Default",
           "description": "Updated description"
         }
       )
    """
    logger.info(f"Update field request: base_id='{NOCODB_BASE_ID}', field_id='{field_id}'")

    # Parameter validation
    if not NOCODB_BASE_ID or not field_id:
        error_msg = "Both base_id and field_id are required"
        logger.error(error_msg)
        return {"error": True, "message": error_msg}
    if not isinstance(field_data, dict) or not field_data:
        error_msg = "field_data must be a non-empty dictionary"
        logger.error(error_msg)
        return {"error": True, "message": error_msg}

    try:
        client = await get_nocodb_client(ctx)

        url = f"/api/v3/meta/bases/{NOCODB_BASE_ID}/fields/{field_id}"
        logger.info(f"PATCH request to {url} with payload: {field_data}")

        response = await client.patch(url, json=field_data)
        response.raise_for_status()

        result = response.json()
        logger.info(f"Successfully updated field '{field_id}' in base '{NOCODB_BASE_ID}'.")
        logger.debug(f"Field update result: {result}")

        return result

    except Exception as e:
        error_msg = f"Failed to update field '{field_id}' in base '{NOCODB_BASE_ID}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"error": True, "message": error_msg}


@mcp.tool()
async def list_tables(
    page: int = 1,
    page_size: int = 25,
    sort: Optional[str] = None,
    include_m2m: bool = False,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Retrieve the list of all tables in a specific NocoDB base.

    This tool fetches metadata for every table in the given base,
    with support for pagination, sorting, and optionally including M2M tables.

    Parameters:
    - page: Page number for pagination (default: 1).
    - page_size: Number of items per page (default: 25).
    - sort: Sort order for the table list (optional).
    - include_m2m: Whether to include many-to-many relationship tables (default: False).

    Returns:
    - Dictionary containing the list of tables (and pageInfo) or error information.

    Example:
    List tables in base "p_124hhlkbeasewh":
       list_tables(page=1, page_size=50, include_m2m=True)
    """
    logger.info(f"Request to list tables in base '{NOCODB_BASE_ID}' (page={page}, page_size={page_size}, sort={sort}, include_m2m={include_m2m})")

    try:
        client = await get_nocodb_client(ctx)

        url = f"/api/v2/meta/bases/{NOCODB_BASE_ID}/tables"
        params = {
            "page": page,
            "pageSize": page_size,
            "includeM2M": str(include_m2m).lower()
        }
        if sort:
            params["sort"] = sort

        logger.info(f"GET request to {url} with params: {params}")

        response = await client.get(url, params=params)
        response.raise_for_status()

        result = response.json()

        tables = result.get("list", [])
        page_info = result.get("pageInfo", {})
        logger.info(f"Retrieved {len(tables)} tables (page {page}/{page_info.get('totalRows', '?')}).")
        logger.debug(f"Full response: {result}")

        return result

    except Exception as e:
        error_msg = f"Failed to list tables in base '{NOCODB_BASE_ID}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"error": True, "message": error_msg}


# Run the server
if __name__ == "__main__":
    print("Starting Nocodb MCP Server initialization...", file=sys.stderr)
    sys.stderr.flush()  # Force output to display immediately
    
    # Check environment variables
    required_vars = ["NOCODB_URL", "NOCODB_API_TOKEN", "NOCODB_BASE_ID"]
    for var in required_vars:
        value = os.environ.get(var)
        print(f"Environment variable {var}: {'SET' if value else 'MISSING'}", file=sys.stderr)
    sys.stderr.flush()
    
    print("Initializing MCP server...", file=sys.stderr)
    sys.stderr.flush()
    
    try:
        mcp.run()
        print("MCP server run() completed - this line should not appear if run() blocks properly", file=sys.stderr)
    except Exception as e:
        print(f"ERROR starting MCP server: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
