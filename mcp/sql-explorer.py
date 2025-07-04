from pathlib import Path
import pyodbc
import os
from typing import List, Dict, Any, Optional
from fastmcp import FastMCP
import pandas as pd

# Initialize FastMCP server
mcp = FastMCP("SQL Server Explorer", log_level="CRITICAL")


CONN_STR = "DRIVER={ODBC Driver 17 for SQL Server};SERVER={server_name};DATABASE={db_name};UID=pyservice;PWD={password}" 


class SQLServerConnection:

    def __init__(self, database):
        self.database = database

    def __enter__(self):
        if self.database is None:
            self.database = "ants"  # Default database if not specified
        global CONN_STR

        conn = CONN_STR.replace("ants", self.database)
        self.conn = pyodbc.connect(conn)
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()



@mcp.tool()
def read_query(
        query: str,
        params: Optional[List[Any]] = None,
        fetch_all: bool = True,
        row_limit: int = 10,
        database: Optional[str] = "ants"
) -> List[Dict[str, Any]]:
    """
    Execute a query on the SQL Server database.

    Note : if you get error 'ODBC SQL type -155 is not yet supported.  column-index=32  type=-155', 'HY106')
           then it is due to datetimeoffset field which this tool cant handle that type , so get proper table schema and for those columns CAST datetimeoffset into datetime
    """
    query = query.strip()

    if query.endswith(';'):
        query = query[:-1].strip()

    def contains_multiple_statements(sql: str) -> bool:
        in_single_quote = False
        in_double_quote = False
        for char in sql:
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
            elif char == ';' and not in_single_quote and not in_double_quote:
                return True
        return False

    if contains_multiple_statements(query):
        raise ValueError("Multiple SQL statements are not allowed")

    query_lower = query.lower()
    if not any(query_lower.startswith(prefix) for prefix in ('select', 'with', 'sp_helptext')):
        raise ValueError("Only SELECT queries (including WITH clauses) are allowed for safety")

    if 'limit' not in query_lower and 'top' not in query_lower:
        query = f"SELECT TOP {row_limit} * FROM ({query}) AS limited_result"

    params = params or []

    with SQLServerConnection(database) as cursor:
        try:
            cursor.execute(query, params)
            columns = [column[0] for column in cursor.description]

            if fetch_all:
                rows = cursor.fetchall()
            else:
                rows = [cursor.fetchone()]

            results = []
            for row in rows:
                if row is not None:
                    results.append(dict(zip(columns, row)))
            return results

        except pyodbc.Error as e:
            raise ValueError(f"SQL Server error: {str(e)}")

@mcp.tool()
def list_tables(database: Optional[str] = "ants") -> List[str]:
    """List all user tables in the SQL Server database."""
    with SQLServerConnection(database) as cursor:
        try:
            cursor.execute("""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            return [row[0] for row in cursor.fetchall()]
        except pyodbc.Error as e:
            raise ValueError(f"SQL Server error: {str(e)}")


@mcp.tool()
def describe_table(table_name: str, database: Optional[str] = "ants") -> str:
    """Describe the schema of a table in SQL Server."""
    with SQLServerConnection(database) as cursor:
        try:
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, 
                       COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IsIdentity,
                       COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsPrimaryKey') AS IsPrimaryKey
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
            """, table_name)

            return str(
                [
                    {
                        "name": row.COLUMN_NAME,
                        "type": row.DATA_TYPE,
                        "notnull": "NO" if row.IS_NULLABLE == "YES" else "YES",
                        "dflt_value": row.COLUMN_DEFAULT,
                        "identity": "YES" if row.IsIdentity == 1 else "NO",
                        "pk": "YES" if row.IsPrimaryKey == 1 else "NO"
                    }
                    for row in cursor.fetchall()
                ]
            )
        except pyodbc.Error as e:
            raise ValueError(f"SQL Server error: {str(e)}")




@mcp.tool()
def get_table_linked_to_this_procedure(procedure_name: str, database: Optional[str] = "ants") -> str:
    """ Get all the tables referencing the stored procedure """
    with SQLServerConnection(database) as cursor:
        try:
            cursor.execute(
                f"""
                 WITH refObj AS (
            SELECT Distinct o.Object_Id,OBJECT_NAME(referencing_id) AS referencing_entity_name,
            o.type, o.type_desc AS referencing_desciption, o.create_date, o.modify_date,
            COALESCE(COL_NAME(referencing_id, referencing_minor_id), '(n/a)') AS referencing_minor_id, referencing_class_desc,
            referenced_server_name, referenced_schema_name, referenced_database_name ,referenced_database_name as [database_name],
            referenced_id ,referenced_entity_name
            ,(Select type From sys.objects Where Object_id = OBJECT_ID(referenced_entity_name)) entity_Type
            --,COALESCE(COL_NAME(referenced_id, referenced_minor_id), '(n/a)') AS referenced_column_name, is_caller_dependent, is_ambiguous
            FROM sys.sql_expression_dependencies AS sed
            INNER JOIN sys.objects AS o ON sed.referencing_id = o.object_id
            --And referenced_entity_name = 'TsAgentTiming'
            --AND (o.Name Not like 'sp_MS%') -- AND o.Type = 'P'
            )
            SELECT * FROM refObj Where referencing_entity_name = '{procedure_name}'
                """
            )

            rows = cursor.fetchall()
            rows = [tuple(row) for row in rows]

            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)

            return df.to_markdown()

        except pyodbc.Error as e:
            raise ValueError(f"SQL Server error: {str(e)}")

@mcp.tool()
def get_procedure_linked_to_this_table(table_name: str, database: Optional[str] = "ants") -> str:
    """ Get all the procs and functions referencing the table """
    with SQLServerConnection(database) as cursor:
        try:
            cursor.execute(
                f"""
                WITH refObj AS (
            SELECT Distinct o.Object_Id,OBJECT_NAME(referencing_id) AS referencing_entity_name,
            o.type, o.type_desc AS referencing_desciption, o.create_date, o.modify_date,
            COALESCE(COL_NAME(referencing_id, referencing_minor_id), '(n/a)') AS referencing_minor_id, referencing_class_desc,
            referenced_server_name, referenced_schema_name, referenced_database_name ,referenced_database_name as [database_name],
            referenced_id ,referenced_entity_name
            ,(Select type From sys.objects Where Object_id = OBJECT_ID(referenced_entity_name)) entity_Type
            --,COALESCE(COL_NAME(referenced_id, referenced_minor_id), '(n/a)') AS referenced_column_name, is_caller_dependent, is_ambiguous
            FROM sys.sql_expression_dependencies AS sed
            INNER JOIN sys.objects AS o ON sed.referencing_id = o.object_id
            And referenced_entity_name = '{table_name}'
            --AND (o.Name Not like 'sp_MS%') -- AND o.Type = 'P'
            )
            SELECT * FROM refObj
                """
            )

            rows = cursor.fetchall()
            rows = [tuple(row) for row in rows]

            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)

            return df.to_markdown()

        except pyodbc.Error as e:
            raise ValueError(f"SQL Server error: {str(e)}")


if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=8009,
        path="/mcp"  # Optional: defines where the MCP endpoint resides
    )
