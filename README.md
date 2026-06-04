# PostgreSQL Employee Collector

The **PostgreSQL Employee Collector** is an autonomous Go program designed to run as a scheduled job. It retrieves employee data from a source PostgreSQL database, encrypts it using Envelope Encryption (AES-GCM), and inserts the raw fragments into the central MitM database's `raw_ingestion` table.

For code details, refer to:

- [main.go](file:///home/zb_bamboo/DEV/__NEW__/Go/mitm-2/collector-layer/mitm_collector_pg-employee/main.go) - Ingestion & encryption logic.
- [go.mod](file:///home/zb_bamboo/DEV/__NEW__/Go/mitm-2/collector-layer/mitm_collector_pg-employee/go.mod) - Dependency definition.

---

## 🏗️ How It Works

1.  **Bootstrapping**: Expects the MitM database connection configuration passed as a JSON string argument, and environment settings from the parent scheduler.
2.  **Envelope Decryption**:
    - Reads the Key Encryption Key (KEK) from the `MASTER_KEY` environment variable.
    - Retrieves the encrypted source DB config and wrapped Data Encryption Key (DEK) from the MitM database.
    - Decrypts the DEK using the KEK, then decrypts the source database credentials using the DEK.
3.  **Data Extraction**:
    - Connects to the source PostgreSQL database.
    - Retrieves the last processed cursor offset from `ingestion_cursors`.
    - Queries new records from the `employees` table (`id > lastCursor`).
4.  **Ingestion**:
    - Encrypts each employee record as a JSON fragment via AES-GCM using the DEK and a fresh random nonce.
    - Inserts the encrypted records into `raw_ingestion` with `pending` status.
    - Updates the cursor offset to the highest processed ID.
5.  **IPC Event Reporting**: Sends startup status, audit events (e.g. key decryption confirmations), processing progress, and final run statistics back to the scheduler over a Unix Domain Socket.

---

## ⚙️ Configuration & Environment

### Environment Variables

- `MASTER_KEY` (Required): The base64-encoded 32-byte Master Key (KEK) used to unwrap DEKs.
- `RUN_ID` (Optional): Run ID injected by the scheduler to identify this execution instance.
- `SCHEDULER_SOCKET_PATH` (Optional): Path to the Unix socket for IPC event logging.

### JSON CLI Argument

The collector requires a single JSON parameter as a command-line argument detailing the connection to the MitM target database.

#### Example JSON Config:

```json
{
  "host": "pghost",
  "port": 5432,
  "user": "pg_user",
  "password": "user_password",
  "database": "hr",
  "table": "employees"
  "source_name": "PG_EMPLOYEE"
}
```

_Note: Alternatively, you can pass a single `"dsn"` key containing the full connection string instead of host/port details._

---

## 🛠️ Build Instructions

### Prerequisites

- Go 1.25.0 or later installed on the system.

### Compiling the Binary

To compile the collector into a standalone executable, navigate to the collector directory and build:

```bash
cd /home/zb_bamboo/DEV/__NEW__/Go/mitm-2/collector-layer/mitm_collector_pg-employee
go build -o bin/mitm-collector-pg-employee main.go
```

This compiles a static executable `mitm-collector-pg-employee` inside the local `bin/` directory.

---

## 🚀 Execution Example

To test the binary manually from the command line:

```bash
# 1. Export the Master Key (must match the one used during DB initialization)
export MASTER_KEY="Y29uZmlkZW50aWFsX21hc3Rlcl9rZXlfMzJfYnl0ZXM="

# 2. Run the collector binary, passing the MitM connection details
./bin/mitm-collector-pg-employee '{
  "host": "127.0.0.1",
  "port": 5432,
  "user": "postgres",
  "password": "yourpassword",
  "database": "mitm",
  "source_name": "PG_EMPLOYEE"
}'
```
