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
print(f"Python version: {sys.version}")
print(f"Starting NocoDB MCP server")
print(f"Args: {sys.argv}")
print(f"Env vars: NOCODB_URL exists: {'NOCODB_URL' in os.environ}")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("nocodb-mcp")

# Create the MCP server
mcp = FastMCP("Nocodb MCP Server")

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
    
    # Validate data structure based on bulk flag
    if bulk and not isinstance(data, list):
        logger.warning(f"Bulk creation requested but data is not a list, converting single record to list")
        data = [data]
    elif not bulk and isinstance(data, list):
        logger.warning(f"Single record creation requested but data is a list, using first item only")
        data = data[0] if data else {}
        
    # Log operation details
    operation_type = "bulk" if bulk else "single record"
    record_count = len(data) if isinstance(data, list) else 1
    logger.debug(f"Creating {record_count} records ({operation_type})")
    
    try:
        client = await get_nocodb_client(ctx)
        
        # Get the table ID from the table name
        table_id = await get_table_id(client, table_name)
        
        # Determine the endpoint based on whether we're doing bulk creation or single record
        if bulk:
            # Bulk creation endpoint
            url = f"/api/v2/tables/{table_id}/records/bulk"
            # Ensure data is a list for bulk operations
            if not isinstance(data, list):
                data = [data]
            logger.info(f"Performing bulk creation of {len(data)} records")
        else:
            # Single record creation endpoint
            url = f"/api/v2/tables/{table_id}/records"
            logger.info(f"Creating single record")
        
        # Make the request
        response = await client.post(url, json=data)
        
        # Handle response
        response.raise_for_status()
        result = response.json()
        
        logger.info(f"Successfully created record(s) in table '{table_name}'")
        return result
        
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP error {e.response.status_code} creating records in '{table_name}'"
        logger.error(error_msg)
        logger.debug(f"Response body: {e.response.text}")
        return {
            "error": True,
            "status_code": e.response.status_code,
            "message": f"HTTP error: {e.response.text}"
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
            response = await client.delete(url, json={"ids": bulk_ids})
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
        
        # Delete operations may return empty response body
        try:
            result = response.json()
        except json.JSONDecodeError:
            logger.debug("Delete operation returned empty response body")
            result = {"success": True, "message": "Record(s) deleted successfully"}
        
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
