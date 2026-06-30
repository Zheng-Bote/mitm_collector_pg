# PostgreSQL Collector

The **PostgreSQL Collector** is an autonomous Go program designed to run as a scheduled job. It dynamically retrieves all records from a specified database table inside a Postgres database instance, encrypts it using Envelope Encryption (AES-GCM), and inserts the raw fragments into the central MitM database's `raw_ingestion` table.

For code details, refer to:

- [main.go](file:///home/zb_bamboo/DEV/__NEW__/Go/mitm-2/collector-layer/mitm_collector_pg/main.go) - Ingestion & encryption logic.
- [go.mod](file:///home/zb_bamboo/DEV/__NEW__/Go/mitm-2/collector-layer/mitm_collector_pg/go.mod) - Dependency definition.

---

## 🏗️ How It Works

1.  **Bootstrapping**: Expects the MitM database connection configuration passed via `MITM_DB_*` environment variables, an optional JSON arguments string (`os.Args[1]`) injected by the scheduler to override settings (like table name and source name), and other environment settings.
2.  **Envelope Decryption**:
    - Reads the Key Encryption Key (KEK) from the `MASTER_KEY` environment variable.
    - Retrieves the encrypted source DB config and wrapped Data Encryption Key (DEK) from the MitM database.
    - Decrypts the DEK using the KEK, then decrypts the source database credentials using the DEK.
3.  **Data Extraction**:
    - Connects to the source PostgreSQL database.
    - Retrieves the last processed cursor offset from `ingestion_cursors`.
    - Queries new records from the specified source table (defaults to `employees`, `id > lastCursor`).
4.  **Ingestion**:
    - Encrypts each employee record as a JSON fragment via AES-GCM using the DEK and a fresh random nonce.
    - Generates a deterministic `correlation_id` (UUIDv5) based on the specified `business_key_column`.
    - Inserts the encrypted records into `raw_ingestion` with `pending` status.
    - Updates the cursor offset to the highest processed ID.
5.  **IPC Event Reporting**: Sends startup status, audit events (e.g. key decryption confirmations), processing progress, and final run statistics back to the scheduler over a Unix Domain Socket.

---

## ⚙️ Configuration & Environment

### Environment Variables

- `MASTER_KEY` (Required): The base64-encoded 32-byte Master Key (KEK) used to unwrap DEKs.
- `MITM_DB_CONFIG_JSON` (**Preferred**): JSON-encoded credentials containing a nested `"db"` object for the MitM PostgreSQL database.
- `MITM_DB_HOST`, `MITM_DB_PORT`, `MITM_DB_USER`, `MITM_DB_PASSWORD`, `MITM_DB_NAME` (**Fallback**): The connection parameters for the central target MitM database.
- `RUN_ID` (Optional): Run ID injected by the scheduler to identify this execution instance.
- `SCHEDULER_SOCKET_PATH` (Optional): Path to the Unix socket for IPC event logging.

### JSON CLI Arguments

The collector accepts an optional JSON parameter as command-line argument:

#### 1. Optional Job Overrides (`os.Args[1]`)

An optional JSON string passed by the scheduler to override the default ingestion behaviour.

Example:

```json
{
  "source_name": "mirror-dev_employee",
  "table": "employee",
  "cursor_column": "id",
  "topic": "Employee",
  "business_key_column": "employee_id"
}
```

* `business_key_column`: (Optional) Specifies the column to be used for generating the deterministic `correlation_id`. Critical for joining distributed data in the Transformation Layer. Defaults to the `cursor_column` if omitted.

---

## 🛠️ Build Instructions

### Prerequisites

- Go 1.25.0 or later installed on the system.

### Compiling the Binary

To compile the collector into a standalone executable, navigate to the collector directory and build:

```bash
cd /home/zb_bamboo/DEV/__NEW__/Go/mitm-2/collector-layer/mitm_collector_pg
go build -o bin/mitm-collector-pg main.go
```

This compiles a static executable `mitm-collector-pg` inside the local `bin/` directory.

---

## 🚀 Execution Example

To test the binary manually from the command line:

```bash
# 1. Export the Master Key (must match the one used during DB initialization)
export MASTER_KEY="Y29uZmlkZW50aWFsX21hc3Rlcl9rZXlfMzJfYnl0ZXM="

# 2. Run the collector binary, passing the MitM connection details and optional overrides
export MITM_DB_CONFIG_JSON='{"db":{"host":"127.0.0.1","port":5432,"user":"postgres","password":"...","database":"mitm"}}'

# Or via Direct Environment Variables (Fallback)
export MITM_DB_HOST="127.0.0.1"
export MITM_DB_PORT="5432"
export MITM_DB_USER="postgres"
export MITM_DB_PASSWORD="yourpassword"
export MITM_DB_NAME="mitm"

./bin/mitm-collector-pg-employee '{"source_name": "PG_EMPLOYEE", "table": "employees"}'
```
