import pytest
import asyncio
from server import (
    create_table,
    insert_record,
    get_records,
    update_record,
    delete_record,
    drop_table,
    get_schema,
)
from pathlib import Path
import sqlite3
import os

# Test database path
TEST_DB_PATH = Path("test_schema_gen.db")


@pytest.fixture(autouse=True)
def setup_teardown():
    """Setup and teardown for each test"""
    # Set testing environment variable
    os.environ["TESTING"] = "1"

    # Setup: Remove test database if it exists
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()

    yield

    # Teardown: Remove test database and unset environment variable
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()
    os.environ.pop("TESTING", None)


def test_create_table():
    """Test creating a table"""
    # Arrange
    table_name = "test_table"
    columns = {"id": "INTEGER PRIMARY KEY", "name": "TEXT", "age": "INTEGER"}

    # Act
    result = asyncio.run(create_table(table_name, columns))

    # Assert
    assert result["status"] == "success"
    assert result["table"] == table_name
    assert result["columns"] == columns

    # Verify table exists in database
    conn = sqlite3.connect(str(TEST_DB_PATH))
    cursor = conn.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    )
    assert cursor.fetchone() is not None
    conn.close()


def test_insert_and_get_record():
    """Test inserting and retrieving a record"""
    # Arrange
    table_name = "test_table"
    columns = {"id": "INTEGER PRIMARY KEY", "name": "TEXT", "age": "INTEGER"}
    asyncio.run(create_table(table_name, columns))

    test_data = {"name": "John Doe", "age": 30}

    # Act
    insert_result = asyncio.run(insert_record(table_name, test_data))
    records = asyncio.run(get_records(table_name))

    # Assert
    assert insert_result["status"] == "success"
    assert insert_result["data"] == test_data
    assert len(records) == 1
    assert records[0]["name"] == test_data["name"]
    assert records[0]["age"] == test_data["age"]


def test_update_record():
    """Test updating a record"""
    # Arrange
    table_name = "test_table"
    columns = {"id": "INTEGER PRIMARY KEY", "name": "TEXT", "age": "INTEGER"}
    asyncio.run(create_table(table_name, columns))

    initial_data = {"name": "John Doe", "age": 30}
    insert_result = asyncio.run(insert_record(table_name, initial_data))
    record_id = insert_result["record_id"]

    update_data = {"name": "Jane Doe", "age": 31}

    # Act
    update_result = asyncio.run(update_record(table_name, record_id, update_data))
    records = asyncio.run(get_records(table_name))

    # Assert
    assert update_result["status"] == "success"
    assert update_result["record_id"] == record_id
    assert len(records) == 1
    assert records[0]["name"] == update_data["name"]
    assert records[0]["age"] == update_data["age"]


def test_delete_record():
    """Test deleting a record"""
    # Arrange
    table_name = "test_table"
    columns = {"id": "INTEGER PRIMARY KEY", "name": "TEXT", "age": "INTEGER"}
    asyncio.run(create_table(table_name, columns))

    test_data = {"name": "John Doe", "age": 30}
    insert_result = asyncio.run(insert_record(table_name, test_data))
    record_id = insert_result["record_id"]

    # Act
    delete_result = asyncio.run(delete_record(table_name, record_id))
    records = asyncio.run(get_records(table_name))

    # Assert
    assert delete_result["status"] == "success"
    assert delete_result["record_id"] == record_id
    assert len(records) == 0


def test_round_trip_crud():
    """Test complete CRUD round trip with multiple records"""
    # Arrange
    table_name = "test_table"
    columns = {
        "id": "INTEGER PRIMARY KEY",
        "name": "TEXT",
        "age": "INTEGER",
        "email": "TEXT",
    }

    # Create table
    create_result = asyncio.run(create_table(table_name, columns))
    assert create_result["status"] == "success"

    # Insert multiple records
    test_records = [
        {"name": "John Doe", "age": 30, "email": "john@example.com"},
        {"name": "Jane Smith", "age": 25, "email": "jane@example.com"},
        {"name": "Bob Johnson", "age": 35, "email": "bob@example.com"},
    ]

    inserted_ids = []
    for record in test_records:
        result = asyncio.run(insert_record(table_name, record))
        inserted_ids.append(result["record_id"])

    # Verify all records were inserted
    all_records = asyncio.run(get_records(table_name))
    assert len(all_records) == len(test_records)

    # Update a record
    update_data = {"name": "Jane Updated", "age": 26}
    update_result = asyncio.run(update_record(table_name, inserted_ids[1], update_data))
    assert update_result["status"] == "success"

    # Verify update
    updated_records = asyncio.run(get_records(table_name))
    assert updated_records[1]["name"] == "Jane Updated"
    assert updated_records[1]["age"] == 26

    # Delete a record
    delete_result = asyncio.run(delete_record(table_name, inserted_ids[0]))
    assert delete_result["status"] == "success"

    # Verify deletion
    remaining_records = asyncio.run(get_records(table_name))
    assert len(remaining_records) == len(test_records) - 1

    # Verify schema
    schema = asyncio.run(get_schema())
    assert table_name in schema
    for col_name in columns.keys():
        assert col_name in schema


def test_table_operations():
    """Test table creation and deletion"""
    # Arrange
    table_name = "test_table"
    columns = {"id": "INTEGER PRIMARY KEY", "name": "TEXT", "age": "INTEGER"}

    # Create table
    create_result = asyncio.run(create_table(table_name, columns))
    assert create_result["status"] == "success"

    # Verify table exists
    schema = asyncio.run(get_schema())
    assert table_name in schema

    # Drop table
    drop_result = asyncio.run(drop_table(table_name))
    assert f"DROP TABLE IF EXISTS {table_name}" in drop_result

    # Verify table is gone
    schema = asyncio.run(get_schema())
    assert table_name not in schema


def test_get_records_with_filters():
    """Test retrieving records with filters"""
    # Arrange
    table_name = "test_table"
    columns = {"id": "INTEGER PRIMARY KEY", "name": "TEXT", "age": "INTEGER"}
    asyncio.run(create_table(table_name, columns))

    test_records = [
        {"name": "John Doe", "age": 30},
        {"name": "Jane Smith", "age": 25},
        {"name": "Bob Johnson", "age": 30},
    ]

    for record in test_records:
        asyncio.run(insert_record(table_name, record))

    # Act & Assert
    # Filter by age
    age_filter = {"age": 30}
    age_records = asyncio.run(get_records(table_name, age_filter))
    assert len(age_records) == 2

    # Filter by name
    name_filter = {"name": "Jane Smith"}
    name_records = asyncio.run(get_records(table_name, name_filter))
    assert len(name_records) == 1
    assert name_records[0]["age"] == 25

    # Filter by both
    both_filter = {"name": "John Doe", "age": 30}
    both_records = asyncio.run(get_records(table_name, both_filter))
    assert len(both_records) == 1
