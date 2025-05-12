import sqlite3
from pathlib import Path

# Test database path
TEST_DB_PATH = Path("test_schema_gen.db")


def test_direct_sqlite():
    # Remove test database if it exists
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()

    # Connect to database
    conn = sqlite3.connect(str(TEST_DB_PATH))
    cursor = conn.cursor()

    # Create table
    table_name = "test_table"
    columns = {"id": "INTEGER PRIMARY KEY", "name": "TEXT", "age": "INTEGER"}

    # Create column definitions
    column_defs = [f"{col_name} {col_type}" for col_name, col_type in columns.items()]
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"

    print("Creating table...")
    cursor.execute(create_sql)
    conn.commit()

    # Verify table exists
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    )
    assert cursor.fetchone() is not None, "Table was not created"

    # Insert record
    test_data = {"name": "John Doe", "age": 30}
    columns = list(test_data.keys())
    placeholders = [f":{col}" for col in columns]
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

    print("Inserting record...")
    cursor.execute(insert_sql, test_data)
    conn.commit()

    # Verify record was inserted
    cursor.execute(f"SELECT * FROM {table_name}")
    records = cursor.fetchall()
    print(f"Records in table: {records}")
    assert len(records) == 1, f"Expected 1 record, got {len(records)}"

    # Update record
    update_data = {"name": "Jane Doe", "age": 31}
    set_clauses = [f"{col} = :{col}" for col in update_data.keys()]
    update_sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE rowid = 1"

    print("Updating record...")
    cursor.execute(update_sql, update_data)
    conn.commit()

    # Verify update
    cursor.execute(f"SELECT * FROM {table_name}")
    records = cursor.fetchall()
    print(f"Records after update: {records}")
    assert len(records) == 1, f"Expected 1 record, got {len(records)}"

    # Delete record
    print("Deleting record...")
    cursor.execute(f"DELETE FROM {table_name} WHERE rowid = 1")
    conn.commit()

    # Verify deletion
    cursor.execute(f"SELECT * FROM {table_name}")
    records = cursor.fetchall()
    print(f"Records after deletion: {records}")
    assert len(records) == 0, f"Expected 0 records, got {len(records)}"

    # Cleanup
    conn.close()
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()

    print("All tests passed!")


if __name__ == "__main__":
    test_direct_sqlite()
