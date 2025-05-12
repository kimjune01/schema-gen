from mcp.server.fastmcp import FastMCP
from typing import Dict, List, Optional, Any
import sqlite3
import json
from pathlib import Path
import os

# Initialize FastMCP server
mcp = FastMCP("SchemaGen")


def get_db():
    """Get SQLite database connection"""
    db_path = Path("test_schema_gen.db" if os.getenv("TESTING") else "schema_gen.db")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = dict_factory
    return conn


def dict_factory(cursor, row):
    """Convert SQLite row to dictionary"""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


@mcp.tool()
async def ping() -> str:
    """Ping the SchemaGen server"""
    return "Pong!"


@mcp.tool()
async def create_table(table_name: str, columns: Dict[str, str]) -> Dict[str, Any]:
    """Create a new table with specified columns

    Args:
        table_name: Name of the table to create
        columns: Dictionary mapping column names to their SQLite types

    Returns:
        Dict containing operation status and table details
    """
    conn = get_db()
    try:
        # Create column definitions
        column_defs = [
            f"{col_name} {col_type}" for col_name, col_type in columns.items()
        ]
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"

        conn.execute(sql)
        conn.commit()

        return {
            "status": "success",
            "table": table_name,
            "columns": columns,
            "sql": sql,
        }
    finally:
        conn.close()


@mcp.tool()
async def insert_record(table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Insert a new record into the specified table

    Args:
        table_name: Name of the table to insert into
        data: Dictionary of column names and values to insert

    Returns:
        Dict containing operation status and inserted record details
    """
    conn = get_db()
    try:
        columns = list(data.keys())
        placeholders = [f":{col}" for col in columns]
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

        cursor = conn.execute(sql, data)
        conn.commit()

        return {
            "status": "success",
            "record_id": cursor.lastrowid,
            "data": data,
            "sql": sql,
        }
    finally:
        conn.close()


@mcp.tool()
async def get_records(
    table_name: str, filters: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """Retrieve records from the specified table

    Args:
        table_name: Name of the table to query
        filters: Optional dictionary of column filters

    Returns:
        List of records matching the criteria
    """
    conn = get_db()
    try:
        sql = f"SELECT * FROM {table_name}"
        params = {}

        if filters:
            conditions = []
            for col, val in filters.items():
                conditions.append(f"{col} = :{col}")
                params[col] = val
            sql += " WHERE " + " AND ".join(conditions)

        cursor = conn.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


@mcp.tool()
async def update_record(
    table_name: str, record_id: int, data: Dict[str, Any]
) -> Dict[str, Any]:
    """Update an existing record in the specified table

    Args:
        table_name: Name of the table containing the record
        record_id: ID of the record to update
        data: Dictionary of column names and new values

    Returns:
        Dict containing operation status and updated record details
    """
    conn = get_db()
    try:
        set_clauses = [f"{col} = :{col}" for col in data.keys()]
        sql = (
            f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE rowid = :record_id"
        )

        params = data.copy()
        params["record_id"] = record_id

        conn.execute(sql, params)
        conn.commit()

        return {"status": "success", "record_id": record_id, "data": data, "sql": sql}
    finally:
        conn.close()


@mcp.tool()
async def delete_record(table_name: str, record_id: int) -> Dict[str, Any]:
    """Delete a record from the specified table

    Args:
        table_name: Name of the table containing the record
        record_id: ID of the record to delete

    Returns:
        Dict containing operation status
    """
    conn = get_db()
    try:
        sql = f"DELETE FROM {table_name} WHERE rowid = :record_id"
        conn.execute(sql, {"record_id": record_id})
        conn.commit()

        return {"status": "success", "record_id": record_id, "sql": sql}
    finally:
        conn.close()


@mcp.tool()
async def drop_table(table_name: str) -> str:
    """Drop (delete) a table from the database

    Args:
        table_name: Name of the table to drop

    Returns:
        String containing the DDL statement that was executed
    """
    conn = get_db()
    try:
        sql = f"DROP TABLE IF EXISTS {table_name}"
        conn.execute(sql)
        conn.commit()
        return sql
    finally:
        conn.close()


@mcp.tool()
async def add_column(table_name: str, column_name: str, column_type: str) -> str:
    """Add a new column to an existing table

    Args:
        table_name: Name of the table to modify
        column_name: Name of the new column
        column_type: SQLite type of the new column

    Returns:
        String containing the DDL statement that was executed
    """
    conn = get_db()
    try:
        sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        conn.execute(sql)
        conn.commit()
        return sql
    finally:
        conn.close()


@mcp.tool()
async def drop_column(table_name: str, column_name: str) -> str:
    """Drop (delete) a column from a table

    Args:
        table_name: Name of the table to modify
        column_name: Name of the column to drop

    Returns:
        String containing the DDL statement that was executed
    """
    conn = get_db()
    try:
        # SQLite doesn't support DROP COLUMN directly, so we need to:
        # 1. Create a new table without the column
        # 2. Copy data from old table
        # 3. Drop old table
        # 4. Rename new table to old name

        # Get current schema
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        columns = [
            row["name"] for row in cursor.fetchall() if row["name"] != column_name
        ]

        # Create new table
        temp_table = f"{table_name}_new"
        create_sql = f"CREATE TABLE {temp_table} AS SELECT {', '.join(columns)} FROM {table_name}"
        conn.execute(create_sql)

        # Drop old table and rename new one
        conn.execute(f"DROP TABLE {table_name}")
        conn.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")

        conn.commit()
        return f"ALTER TABLE {table_name} DROP COLUMN {column_name}"
    finally:
        conn.close()


@mcp.tool()
async def rename_table(old_name: str, new_name: str) -> str:
    """Rename an existing table

    Args:
        old_name: Current name of the table
        new_name: New name for the table

    Returns:
        String containing the DDL statement that was executed
    """
    conn = get_db()
    try:
        sql = f"ALTER TABLE {old_name} RENAME TO {new_name}"
        conn.execute(sql)
        conn.commit()
        return sql
    finally:
        conn.close()


@mcp.tool()
async def rename_column(table_name: str, old_name: str, new_name: str) -> str:
    """Rename a column in a table

    Args:
        table_name: Name of the table containing the column
        old_name: Current name of the column
        new_name: New name for the column

    Returns:
        String containing the DDL statement that was executed
    """
    conn = get_db()
    try:
        # SQLite doesn't support RENAME COLUMN directly, so we need to:
        # 1. Create a new table with renamed column
        # 2. Copy data from old table
        # 3. Drop old table
        # 4. Rename new table to old name

        # Get current schema
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        columns = []
        for row in cursor.fetchall():
            if row["name"] == old_name:
                columns.append(f"{row['name']} AS {new_name}")
            else:
                columns.append(row["name"])

        # Create new table
        temp_table = f"{table_name}_new"
        create_sql = f"CREATE TABLE {temp_table} AS SELECT {', '.join(columns)} FROM {table_name}"
        conn.execute(create_sql)

        # Drop old table and rename new one
        conn.execute(f"DROP TABLE {table_name}")
        conn.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")

        conn.commit()
        return f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}"
    finally:
        conn.close()


@mcp.tool()
async def get_schema() -> str:
    """Get the current database schema as DDL (Data Definition Language)

    Returns:
        String containing SQL DDL statements that define the current database schema
    """
    conn = get_db()
    try:
        # Get all tables
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row["name"] for row in cursor.fetchall()]

        # Get schema for each table
        schemas = []
        for table in tables:
            cursor = conn.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'"
            )
            schemas.append(cursor.fetchone()["sql"])

        return ";\n".join(schemas)
    finally:
        conn.close()


if __name__ == "__main__":
    mcp.run()
