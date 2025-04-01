#!/usr/bin/env python3
"""
Nocodb MCP Server

This server provides tools to interact with a Nocodb database through the Model Context Protocol.
It offers CRUD operations (Create, Read, Update, Delete) for Nocodb tables.

Environment Variables:
- NOCODB_URL: The base URL of your Nocodb instance
- NOCODB_API_TOKEN: The API token for authentication

Usage:
1. Ensure the environment variables are set
2. Run this script directly or use the MCP CLI
"""

import os
import json
import httpx
from typing import Dict, List, Optional, Union, Any
from pydantic import BaseModel, Field
from mcp.server.fastmcp import FastMCP, Context

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
        raise ValueError("NOCODB_URL environment variable is not set")
    if not api_token:
        raise ValueError("NOCODB_API_TOKEN environment variable is not set")
    
    # Remove trailing slash from URL if present
    if nocodb_url.endswith("/"):
        nocodb_url = nocodb_url[:-1]
    
    # Create httpx client with authentication headers - using xc-token as required by Nocodb v2 API
    headers = {
        "xc-token": api_token,
        "Content-Type": "application/json"
    }
    
    return httpx.AsyncClient(base_url=nocodb_url, headers=headers)


async def get_table_id(client: httpx.AsyncClient, table_name: str) -> str:
    """Get the table ID from the table name using the hardcoded base ID"""
    # Get the list of tables in the base
    response = await client.get(f"/api/v2/meta/bases/{NOCODB_BASE_ID}/tables")
    response.raise_for_status()
    
    tables = response.json().get("list", [])
    
    # Find the table with the matching name
    for table in tables:
        if table.get("title") == table_name:
            return table.get("id")
    
    raise ValueError(f"Table '{table_name}' not found in base '{NOCODB_BASE_ID}'")


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
    try:
        client = await get_nocodb_client(ctx)
        
        # Get the table ID from the table name
        table_id = await get_table_id(client, table_name)
        
        # Determine the endpoint based on whether we're fetching a single record or multiple
        if row_id:
            # Single record endpoint
            url = f"/api/v2/tables/{table_id}/records/{row_id}"
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
                
            response = await client.get(url, params=params)
        
        # Handle response
        response.raise_for_status()
        return response.json()
        
    except httpx.HTTPStatusError as e:
        return {
            "error": True,
            "status_code": e.response.status_code,
            "message": f"HTTP error: {e.response.text}"
        }
    except Exception as e:
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
        else:
            # Single record creation endpoint
            url = f"/api/v2/tables/{table_id}/records"
        
        # Make the request
        response = await client.post(url, json=data)
        
        # Handle response
        response.raise_for_status()
        return response.json()
        
    except httpx.HTTPStatusError as e:
        return {
            "error": True,
            "status_code": e.response.status_code,
            "message": f"HTTP error: {e.response.text}"
        }
    except Exception as e:
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
    if not data:
        return {
            "error": True,
            "message": "Data parameter is required for updates"
        }
        
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
            response = await client.patch(url, json=payload)
        elif row_id:
            # Single record update endpoint
            url = f"/api/v2/tables/{table_id}/records/{row_id}"
            response = await client.patch(url, json=data)
        else:
            return {
                "error": True,
                "message": "Either row_id (for single update) or bulk=True with bulk_ids (for bulk update) must be provided"
            }
        
        # Handle response
        response.raise_for_status()
        return response.json()
        
    except httpx.HTTPStatusError as e:
        return {
            "error": True,
            "status_code": e.response.status_code,
            "message": f"HTTP error: {e.response.text}"
        }
    except Exception as e:
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
    try:
        client = await get_nocodb_client(ctx)
        
        # Get the table ID from the table name
        table_id = await get_table_id(client, table_name)
        
        # Determine the endpoint based on whether we're doing bulk deletion or single record
        if bulk and bulk_ids:
            # Bulk deletion endpoint
            url = f"/api/v2/tables/{table_id}/records/bulk"
            # For bulk deletions with IDs, we need to send the ids in the request body
            response = await client.delete(url, json={"ids": bulk_ids})
        elif row_id:
            # Single record deletion endpoint
            url = f"/api/v2/tables/{table_id}/records/{row_id}"
            response = await client.delete(url)
        else:
            return {
                "error": True,
                "message": "Either row_id (for single deletion) or bulk=True with bulk_ids (for bulk deletion) must be provided"
            }
        
        # Handle response
        response.raise_for_status()
        
        # Delete operations may return empty response body
        try:
            return response.json()
        except json.JSONDecodeError:
            return {"success": True, "message": "Record(s) deleted successfully"}
        
    except httpx.HTTPStatusError as e:
        return {
            "error": True,
            "status_code": e.response.status_code,
            "message": f"HTTP error: {e.response.text}"
        }
    except Exception as e:
        return {
            "error": True,
            "message": f"Error: {str(e)}"
        }
    finally:
        if 'client' in locals():
            await client.aclose()


# Run the server
if __name__ == "__main__":
    mcp.run()
