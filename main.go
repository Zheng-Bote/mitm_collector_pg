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

// CollectorArgs defines optional runtime arguments passed by the scheduler as JSON
type CollectorArgs struct {
	SourceName   string `json:"source_name"`
	Table        string `json:"table"`
	CursorColumn string `json:"cursor_column"`
	Topic        string `json:"topic"`
}

// StatusEvent is sent to the scheduler Unix socket
type StatusEvent struct {
	RunID     int    `json:"run_id"`
	Type      string `json:"type"` // "status" (default) or "audit"
	Component string `json:"component"`
	Status    string `json:"status"`
	Message   string `json:"message"`
	Progress  int    `json:"progress"`
}

// IPCClient is used to send events to the scheduler
type IPCClient struct {
	SocketPath string
	RunID      int
	Component  string
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
		RunID:     c.RunID,
		Type:      "audit",
		Component: c.Component,
		Message:   message,
	}
	data, _ := json.Marshal(event)
	_, _ = conn.Write(append(data, '\n'))
}

func main() {
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
				Component:  "mitm_collector_pg",
			}
		}
	}

	ipc.SendEvent("started", "Employee collector program started", 0)

	// 3. Parse Target DB configuration from ENV
	var targetCfg TargetDBConfig
	targetCfg.Host = os.Getenv("MITM_DB_HOST")
	if portStr := os.Getenv("MITM_DB_PORT"); portStr != "" {
		targetCfg.Port, _ = strconv.Atoi(portStr)
	}
	targetCfg.User = os.Getenv("MITM_DB_USER")
	targetCfg.Password = os.Getenv("MITM_DB_PASSWORD")
	targetCfg.Database = os.Getenv("MITM_DB_NAME")

	if targetCfg.Host == "" {
		// Fallback to JSON from ENV
		jsonConfig := os.Getenv("MITM_DB_CONFIG_JSON")
		if jsonConfig != "" {
			if err := json.Unmarshal([]byte(jsonConfig), &targetCfg); err != nil {
				ipc.SendEvent("failed", fmt.Sprintf("Failed to parse MitM database JSON config: %v", err), 0)
				log.Fatalf("Failed to parse MitM JSON configuration: %v", err)
			}
		} else {
			ipc.SendEvent("failed", "MitM database configuration missing in ENV", 0)
			log.Fatal("MitM database credentials not found in environment (MITM_DB_HOST or MITM_DB_CONFIG_JSON)")
		}
	}

	// 3b. Parse optional collector arguments from scheduler (now in os.Args[1])
	tableName := "employees"
	cursorColumn := "" // No default, to allow tables without 'id'
	topicName := "employee.data"

	if len(os.Args) >= 2 {
		var colArgs CollectorArgs
		if err := json.Unmarshal([]byte(os.Args[1]), &colArgs); err == nil {
			if colArgs.SourceName != "" {
				targetCfg.SourceName = colArgs.SourceName
			}
			if colArgs.Table != "" {
				tableName = colArgs.Table
				topicName = fmt.Sprintf("pg.%s.data", strings.ToLower(tableName))
			}
			if colArgs.CursorColumn != "" {
				cursorColumn = colArgs.CursorColumn
			}
			if colArgs.Topic != "" {
				topicName = colArgs.Topic
			}
		} else {
			log.Printf("Warning: Failed to parse collector arguments from os.Args[1]: %v", err)
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

	// 12. Retrieve cursor from MitM database
	var lastCursor string
	err = mitmPool.QueryRow(ctx, "SELECT last_cursor FROM ingestion_cursors WHERE source_name = $1", targetCfg.SourceName).Scan(&lastCursor)
	if err != nil && err != pgx.ErrNoRows {
		log.Printf("Warning: Failed to load cursor: %v", err)
	}

	// 13. Query source table
	var rows pgx.Rows
	var queryArgs []interface{}
	var query string

	if lastCursor != "" && cursorColumn != "" {
		var cursorVal interface{} = lastCursor
		if val, err := strconv.Atoi(lastCursor); err == nil {
			cursorVal = val
		} else if val, err := strconv.ParseFloat(lastCursor, 64); err == nil {
			cursorVal = val
		}
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s > $1 ORDER BY %s ASC",
			sanitizedTable, cursorColumn, cursorColumn)
		queryArgs = append(queryArgs, cursorVal)
	} else if cursorColumn != "" {
		query = fmt.Sprintf("SELECT * FROM %s ORDER BY %s ASC",
			sanitizedTable, cursorColumn)
	} else {
		query = fmt.Sprintf("SELECT * FROM %s", sanitizedTable)
	}

	rows, err = sourcePool.Query(ctx, query, queryArgs...)
	if err != nil {
		ipc.SendEvent("failed", fmt.Sprintf("Failed to query source database: %v", err), 0)
		log.Fatalf("Failed to query source database: %v", err)
	}
	defer rows.Close()

	// 14. Iterate and ingest records dynamically
	fields := rows.FieldDescriptions()
	cols := make([]string, len(fields))
	for i, f := range fields {
		cols[i] = f.Name
	}

	cursorColIdx := -1
	for idx, colName := range cols {
		if strings.EqualFold(colName, cursorColumn) {
			cursorColIdx = idx
			break
		}
	}

	recordsIngested := 0
	maxCursorValue := ""

	ipc.SendEvent("processing", "Preparing dynamic record ingestion", 70)

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			log.Printf("Failed to scan PostgreSQL row: %v", err)
			continue
		}

		// Map column names to values
		rowMap := make(map[string]interface{})
		var currentCursorVal string

		for i, colName := range cols {
			cleaned := cleanValue(values[i])
			rowMap[colName] = cleaned

			// Keep track of cursor value for this row
			if i == cursorColIdx && cleaned != nil {
				currentCursorVal = fmt.Sprintf("%v", cleaned)
			}
		}

		// Convert map to JSON
		rowJSON, err := json.Marshal(rowMap)
		if err != nil {
			log.Printf("Failed to marshal row to JSON: %v", err)
			continue
		}

		// Generate random 12-byte nonce
		nonce := make([]byte, 12)
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			log.Printf("Failed to generate random nonce: %v", err)
			continue
		}

		// Encrypt payload via AES-GCM using storage DEK
		encryptedPayload := dekGCM.Seal(nil, nonce, rowJSON, nil)

		// Insert into raw_ingestion in target database
		_, err = mitmPool.Exec(ctx, `
			INSERT INTO raw_ingestion (topic, source_system, correlation_id, payload, nonce, dek_id, status)
			VALUES ($1, $2, gen_random_uuid(), $3, $4, $5, 'pending')
		`, topicName, targetCfg.SourceName, encryptedPayload, nonce, dekID)
		if err != nil {
			log.Printf("Failed to insert raw fragment: %v", err)
			continue
		}

		recordsIngested++
		if currentCursorVal != "" {
			maxCursorValue = currentCursorVal
		}
	}

	// 15. Update cursor if records were ingested
	if recordsIngested > 0 && maxCursorValue != "" {
		_, err = mitmPool.Exec(ctx, `
			INSERT INTO ingestion_cursors (source_name, last_cursor, updated_at)
			VALUES ($1, $2, NOW())
			ON CONFLICT (source_name) 
			DO UPDATE SET last_cursor = EXCLUDED.last_cursor, updated_at = NOW()
		`, targetCfg.SourceName, maxCursorValue)
		if err != nil {
			log.Printf("Failed to save current cursor state: %v", err)
		}
		ipc.SendAudit(fmt.Sprintf("Ingested %d PostgreSQL records. Cursor updated to %s.", recordsIngested, maxCursorValue))
	}

	// 16. Finish execution
	ipc.SendEvent("finished", fmt.Sprintf("Successfully processed and ingested %d PostgreSQL records into RAW table", recordsIngested), 100)
	log.Printf("Collector finished. Ingested %d records.", recordsIngested)
}

func cleanValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case []byte:
		return string(v)
	case time.Time:
		return v.Format(time.RFC3339)
	default:
		return v
	}
}
