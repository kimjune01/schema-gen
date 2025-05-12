SchemaGen: Dynamic CRUD API Schema Generator
SchemaGen is a lightweight Python tool for rapidly prototyping CRUD applications by dynamically generating database schemas based on API usage patterns. It maintains a SQLite database that evolves as you interact with the API, allowing you to quickly prototype applications without writing a schema upfront.

What SchemaGen Does:
Creates and maintains a SQLite database based on your API usage
Dynamically adds tables and columns as needed from your requests
Handles standard HTTP methods (GET, POST, PUT, DELETE) via MCP tools
Generates DDL schema on demand
Uses only Python standard library (no external dependencies)
Maintains state solely in the SQLite database

What SchemaGen Doesn't Do:
Provide user authentication or authorization
Enforce complex validation rules
Optimize for high-traffic production use
Manage relationships between tables
Support complex querying capabilities
Provide a web interface for database management
