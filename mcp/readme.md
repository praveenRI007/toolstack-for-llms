# 🧠 SQL Server MCP Server

This repository implements a **Model Context Protocol (MCP)** server that exposes read-only SQL Server tools over HTTP. It's designed for tool-calling LLMs, autonomous agents, and cognitive IDEs such as **Cursor**.

---

## 🔌 MCP Endpoint

http://localhost:8009/mcp


---

## 🔧 Cursor MCP Configuration

To connect this server to **Cursor IDE**, add the following to your `mcp.json`:

```json
"sql-explorer-readOnly": {
  "type": "http",
  "url": "http://localhost:8009/mcp",
  "timeout": 60,
  "disabled": false
}
```
## 🧰 Available Tools

All tools are exposed via **MCP-compatible endpoints**:

---

### 📋 `list_user_tables`

Lists all user-defined tables in the connected SQL Server database.

---

### 🧬 `describe_table`

Describes the schema of the specified table, including column names, data types, nullability, and constraints.

---

### 🔎 `run_query`

Executes a read-only `SELECT` query with optional row limiting and database override. Only `SELECT` statements are allowed.

---

### 🔁 `get_tables_in_procedure`

Returns all tables that a given stored procedure reads from or writes to.

---

### 🔂 `get_procedures_for_table`

Returns all stored procedures and functions that reference the specified table.


---


▶️ Running the Server

Make sure dependencies are installed:

Then start the MCP server:

```python sql-explorer.py```

---

🧠 Protocol: Model Context Protocol (MCP)
This server conforms to the Model Context Protocol (MCP) standard, exposing each function as a semantically typed tool callable by LLM agents.
