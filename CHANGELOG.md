# Changelog

All notable changes to the PostgreSQL Collector will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.2.0] - 2026-06-05

### Added
- Fully table-independent and dynamic query engine using `rows.FieldDescriptions()` to fetch all columns dynamically.
- Dynamic row scanning using `rows.Values()` for compatibility with dynamic schemas.
- Sanitization for custom dynamic table and cursor column names to prevent SQL injection.
- Support for runtime configuration overrides (source name, table, cursor column, and destination topic) passed as a JSON string via `os.Args[2]`.

### Changed
- Replaced hardcoded `Employee` struct scan logic with generic map serialization.
- Updated database insertion query to route records to dynamic topics (defaults to `pg.<table_name>.data`).
- Updated cursor persistence to support generic string-based cursor values (`maxCursorValue`) instead of numeric IDs.

## [v0.1.0] - 2026-06-04

### Added
- Initial release of the PostgreSQL Employee Collector.
- Automated extraction of raw employee data using envelope encryption (AES-256-GCM).
- State-based pagination using the `ingestion_cursors` table.
- Nil-safe IPC reporting for status, audit, and progress logging to the scheduler.
