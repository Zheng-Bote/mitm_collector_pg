/**
 * SPDX-FileComment: PostgreSQL Employee Collector
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file main.go
 * @brief Autonomous collector retrieving employee data from a PostgreSQL source, encrypting it, and saving it to RAW tables.
 * @version 1.0.0
 * @date 2026-06-04
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @license Apache-2.0
 */

package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TargetDBConfig defines parameters for the MitM target database passed via JSON CLI argument
type TargetDBConfig struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	User       string `json:"user"`
	Password   string `json:"password"`
	Database   string `json:"database"`
	DSN        string `json:"dsn"`
	SourceName string `json:"source_name"` // Defaults to "PG_EMPLOYEE"
}

// SourceDBConfig defines decrypted credentials for the source database loaded from source_credentials
type SourceDBConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
	DSN      string `json:"dsn"`
}

// Employee defines the structure of data fetched from the source database
type Employee struct {
	ID         int       `json:"id"`
	FirstName  string    `json:"first_name"`
	LastName   string    `json:"last_name"`
	Email      string    `json:"email"`
	Department string    `json:"department"`
	Salary     float64   `json:"salary"`
	HireDate   time.Time `json:"hire_date"`
}

// CollectorArgs defines optional runtime arguments passed by the scheduler as JSON
type CollectorArgs struct {
	SourceName string `json:"source_name"`
	Table      string `json:"table"`
}

// StatusEvent is sent to the scheduler Unix socket
type StatusEvent struct {
	RunID    int    `json:"run_id"`
	Type     string `json:"type"` // "status" or "audit"
	Status   string `json:"status"`
	Message  string `json:"message"`
	Progress int    `json:"progress"`
}

// IPCClient handles communicating status updates to the parent scheduler
type IPCClient struct {
	SocketPath string
	RunID      int
}

func (c *IPCClient) SendEvent(status, message string, progress int) {
	if c == nil || c.SocketPath == "" {
		return
	}
	conn, err := net.Dial("unix", c.SocketPath)
	if err != nil {
		log.Printf("[IPC ERROR] Failed to connect to scheduler socket: %v", err)
		return
	}
	defer conn.Close()

	event := StatusEvent{
		RunID:    c.RunID,
		Type:     "status",
		Status:   status,
		Message:  message,
		Progress: progress,
	}
	data, _ := json.Marshal(event)
	_, _ = conn.Write(append(data, '\n'))
}

func (c *IPCClient) SendAudit(message string) {
	if c == nil || c.SocketPath == "" {
		return
	}
	conn, err := net.Dial("unix", c.SocketPath)
	if err != nil {
		log.Printf("[IPC ERROR] Failed to connect to scheduler socket: %v", err)
		return
	}
	defer conn.Close()

	event := StatusEvent{
		RunID:   c.RunID,
		Type:    "audit",
		Message: message,
	}
	data, _ := json.Marshal(event)
	_, _ = conn.Write(append(data, '\n'))
}

func main() {
	// 1. Check arguments
	if len(os.Args) < 2 {
		log.Fatal("Usage: collector <json_database_config>")
	}

	jsonConfig := os.Args[1]

	// 2. Load IPC Environment
	var ipc *IPCClient
	runIDStr := os.Getenv("RUN_ID")
	socketPath := os.Getenv("SCHEDULER_SOCKET_PATH")
	if runIDStr != "" && socketPath != "" {
		runID, err := strconv.Atoi(runIDStr)
		if err == nil {
			ipc = &IPCClient{
				SocketPath: socketPath,
				RunID:      runID,
			}
		}
	}

	ipc.SendEvent("started", "Employee collector program started", 0)

	// 3. Parse Target DB configuration
	var targetCfg TargetDBConfig
	if err := json.Unmarshal([]byte(jsonConfig), &targetCfg); err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to parse MitM database JSON config: %v", err), 0)
		log.Fatalf("Failed to parse MitM JSON configuration: %v", err)
	}

	// 3b. Parse optional collector arguments from scheduler (os.Args[2])
	tableName := "employees"
	if len(os.Args) >= 3 {
		var colArgs CollectorArgs
		if err := json.Unmarshal([]byte(os.Args[2]), &colArgs); err == nil {
			if colArgs.SourceName != "" {
				targetCfg.SourceName = colArgs.SourceName
			}
			if colArgs.Table != "" {
				tableName = colArgs.Table
			}
		} else {
			log.Printf("Warning: Failed to parse collector arguments from os.Args[2]: %v", err)
		}
	}

	// Sanitize tableName (prevent SQL injection)
	sanitizedTable := tableName
	for _, char := range sanitizedTable {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_' || char == '.') {
			log.Fatalf("Invalid table name: %s", tableName)
		}
	}

	if targetCfg.SourceName == "" {
		targetCfg.SourceName = "PG_EMPLOYEE"
	}

	var mitmDSN string
	if targetCfg.DSN != "" {
		mitmDSN = targetCfg.DSN
	} else {
		mitmDSN = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
			targetCfg.User, targetCfg.Password, targetCfg.Host, targetCfg.Port, targetCfg.Database)
	}

	ctx := context.Background()

	// 4. Connect to MitM target database
	mitmPool, err := pgxpool.New(ctx, mitmDSN)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to connect to MitM database: %v", err), 0)
		log.Fatalf("Failed to connect to MitM database: %v", err)
	}
	defer mitmPool.Close()

	ipc.SendEvent("processing", "Connected to MitM database", 20)



	// 5. Load KEK from environment
	masterKey := os.Getenv("MASTER_KEY")
	if masterKey == "" {
		ipc.SendEvent("failed", "Missing MASTER_KEY environment variable", 0)
		log.Fatal("Missing MASTER_KEY environment variable")
	}

	var kek []byte
	if decoded, err := base64.StdEncoding.DecodeString(masterKey); err == nil {
		kek = decoded
	} else {
		kek = []byte(masterKey)
	}

	// Adjust KEK to 32 bytes if necessary
	if len(kek) != 32 {
		adjusted := make([]byte, 32)
		copy(adjusted, kek)
		kek = adjusted
	}

	// 6. Query encrypted source credentials
	var configPayload []byte
	var credentialsNonce []byte
	var dekID string

	err = mitmPool.QueryRow(ctx, `
		SELECT config_payload, nonce, dek_id 
		FROM source_credentials 
		WHERE source_name = $1 AND is_active = true 
		LIMIT 1
	`, targetCfg.SourceName).Scan(&configPayload, &credentialsNonce, &dekID)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to load source credentials for '%s': %v", targetCfg.SourceName, err), 0)
		log.Fatalf("Failed to load source credentials: %v", err)
	}

	// 7. Query wrapped DEK
	var wrappedKey []byte
	err = mitmPool.QueryRow(ctx, `
		SELECT wrapped_key 
		FROM storage_keys 
		WHERE id = $1 AND is_active = true 
		LIMIT 1
	`, dekID).Scan(&wrappedKey)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to load wrapped DEK (ID: %s): %v", dekID, err), 0)
		log.Fatalf("Failed to load wrapped DEK: %v", err)
	}

	// 8. Decrypt wrapped DEK using KEK
	if len(wrappedKey) < 12 {
		ipc.SendEvent("failed", "Wrapped DEK is too short (must be at least 12 bytes nonce + cipher)", 0)
		log.Fatal("Wrapped DEK in database is invalid")
	}
	dekNonce := wrappedKey[:12]
	wrappedCipher := wrappedKey[12:]

	kekBlock, err := aes.NewCipher(kek)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to initialize AES cipher with KEK: %v", err), 0)
		log.Fatalf("Failed to initialize AES cipher: %v", err)
	}
	kekGCM, err := cipher.NewGCM(kekBlock)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to initialize GCM with KEK: %v", err), 0)
		log.Fatalf("Failed to initialize GCM: %v", err)
	}
	dek, err := kekGCM.Open(nil, dekNonce, wrappedCipher, nil)
	if err != nil {
		ipc.SendEvent("failed", "Failed to decrypt wrapped DEK (KEK mismatch or corrupted key data)", 0)
		log.Fatalf("Failed to decrypt DEK: %v", err)
	}

	ipc.SendAudit("Decrypted storage DEK using KEK successfully")

	// 9. Decrypt source connection credentials payload using DEK
	dekBlock, err := aes.NewCipher(dek)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to initialize AES cipher with DEK: %v", err), 0)
		log.Fatalf("Failed to initialize DEK AES cipher: %v", err)
	}
	dekGCM, err := cipher.NewGCM(dekBlock)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to initialize GCM with DEK: %v", err), 0)
		log.Fatalf("Failed to initialize DEK GCM: %v", err)
	}
	decryptedConfigBytes, err := dekGCM.Open(nil, credentialsNonce, configPayload, nil)
	if err != nil {
		ipc.SendEvent("failed", "Failed to decrypt source config payload using DEK", 0)
		log.Fatalf("Failed to decrypt source config: %v", err)
	}

	ipc.SendAudit("Decrypted source connection credentials payload successfully")

	// 10. Parse source database configuration
	var sourceCfg SourceDBConfig
	if err := json.Unmarshal(decryptedConfigBytes, &sourceCfg); err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to parse decrypted source database configuration: %v", err), 0)
		log.Fatalf("Failed to parse decrypted source config: %v", err)
	}

	var sourceDSN string
	if sourceCfg.DSN != "" {
		sourceDSN = sourceCfg.DSN
	} else {
		sourceDSN = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
			sourceCfg.User, sourceCfg.Password, sourceCfg.Host, sourceCfg.Port, sourceCfg.Database)
	}

	// 11. Connect to source database
	sourcePool, err := pgxpool.New(ctx, sourceDSN)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to connect to source database: %v", err), 0)
		log.Fatalf("Failed to connect to source database: %v", err)
	}
	defer sourcePool.Close()

	ipc.SendEvent("processing", "Connected to source database", 50)
	ipc.SendAudit("Connected to source database successfully")

	// Ensure source table exists in source for testing robustness
	_, _ = sourcePool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id          SERIAL PRIMARY KEY,
			first_name  VARCHAR(50),
			last_name   VARCHAR(50) NOT NULL,
			email       VARCHAR(100),
			department  VARCHAR(50),
			salary      NUMERIC,
			hire_date   DATE DEFAULT CURRENT_DATE
		);
	`, sanitizedTable))

	// 12. Retrieve cursor from MitM database
	var lastCursor string
	err = mitmPool.QueryRow(ctx, "SELECT last_cursor FROM ingestion_cursors WHERE source_name = $1", targetCfg.SourceName).Scan(&lastCursor)
	if err != nil && err != pgx.ErrNoRows {
		log.Printf("Warning: Failed to load cursor: %v", err)
	}

	// 13. Query source table
	var rows pgx.Rows
	if lastCursor != "" {
		lastID, _ := strconv.Atoi(lastCursor)
		rows, err = sourcePool.Query(ctx, fmt.Sprintf(`
			SELECT id, first_name, last_name, email, department, CAST(salary AS float8), hire_date 
			FROM %s 
			WHERE id > $1 
			ORDER BY id ASC
		`, sanitizedTable), lastID)
	} else {
		rows, err = sourcePool.Query(ctx, fmt.Sprintf(`
			SELECT id, first_name, last_name, email, department, CAST(salary AS float8), hire_date 
			FROM %s 
			ORDER BY id ASC
		`, sanitizedTable))
	}

	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to query source database: %v", err), 0)
		log.Fatalf("Failed to query source database: %v", err)
	}
	defer rows.Close()

	// 14. Iterate and ingest records
	recordsIngested := 0
	maxIDProcessed := 0

	ipc.SendEvent("processing", "Reading employee records and preparing ingestion", 70)

	for rows.Next() {
		var emp Employee
		err := rows.Scan(&emp.ID, &emp.FirstName, &emp.LastName, &emp.Email, &emp.Department, &emp.Salary, &emp.HireDate)
		if err != nil {
			log.Printf("Failed to scan employee record: %v", err)
			continue
		}

		// Convert employee to JSON
		empJSON, err := json.Marshal(emp)
		if err != nil {
			log.Printf("Failed to marshal employee record (ID: %d): %v", emp.ID, err)
			continue
		}

		// Generate random 12-byte nonce for AES-GCM
		nonce := make([]byte, 12)
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			log.Printf("Failed to generate nonce for employee (ID: %d): %v", emp.ID, err)
			continue
		}

		// Encrypt payload via GCM using storage DEK
		encryptedPayload := dekGCM.Seal(nil, nonce, empJSON, nil)

		topic := "employee.data"
		if strings.ToLower(sanitizedTable) != "employees" {
			topic = fmt.Sprintf("pg.%s.data", strings.ToLower(sanitizedTable))
		}

		// Insert into raw_ingestion
		_, err = mitmPool.Exec(ctx, `
			INSERT INTO raw_ingestion (topic, source_system, correlation_id, payload, nonce, dek_id, status)
			VALUES ($1, $2, gen_random_uuid(), $3, $4, $5, 'pending')
		`, topic, targetCfg.SourceName, encryptedPayload, nonce, dekID)
		if err != nil {
			log.Printf("Failed to insert raw fragment for employee (ID: %d): %v", emp.ID, err)
			continue
		}

		recordsIngested++
		if emp.ID > maxIDProcessed {
			maxIDProcessed = emp.ID
		}
	}

	// 15. Update cursor if records were ingested
	if recordsIngested > 0 {
		_, err = mitmPool.Exec(ctx, `
			INSERT INTO ingestion_cursors (source_name, last_cursor, updated_at)
			VALUES ($1, $2, NOW())
			ON CONFLICT (source_name) 
			DO UPDATE SET last_cursor = EXCLUDED.last_cursor, updated_at = NOW()
		`, targetCfg.SourceName, strconv.Itoa(maxIDProcessed))
		if err != nil {
			log.Printf("Failed to save current cursor state: %v", err)
		}
		ipc.SendAudit(fmt.Sprintf("Ingested %d records. Cursor updated to %d.", recordsIngested, maxIDProcessed))
	}

	// 16. Finish execution
	ipc.SendEvent("finished", fmt.Sprintf("Successfully processed and ingested %d employee records into RAW table", recordsIngested), 100)
	log.Printf("Collector finished. Ingested %d records.", recordsIngested)
}
