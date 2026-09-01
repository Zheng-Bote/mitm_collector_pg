/**
 * SPDX-FileComment: PostgreSQL Collector
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
	"bufio"
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
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	appName        = "PostgreSQL Collector"
	appDescription = "Extracts data from PostgreSQL databases"
	version        = "0.15.3"
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
	SourceName        string `json:"source_name"`
	Table             string `json:"table"`
	CursorColumn      string `json:"cursor_column"`
	Topic             string `json:"topic"`
	BusinessKeyColumn string `json:"business_key_column"`
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
	Topic      string
	SourceName string
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

	if c.Topic != "" && c.SourceName != "" {
		message = fmt.Sprintf("%s: %s: %s", c.Topic, c.SourceName, message)
	}

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

	if c.Topic != "" && c.SourceName != "" {
		message = fmt.Sprintf("%s: %s: %s", c.Topic, c.SourceName, message)
	}

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
	// Fetch credentials via IPC if running under scheduler
	if dbCfg, masterKey, err := fetchCredentialsFromScheduler(); err == nil {
		if dbCfg != "" {
			os.Setenv("MITM_DB_CONFIG_JSON", dbCfg)
		}
		if masterKey != "" {
			os.Setenv("MASTER_KEY", masterKey)
		}
	} else if os.Getenv("RUN_ID") != "" && os.Getenv("SCHEDULER_SOCKET_PATH") != "" {
		log.Printf("[IPC Warning] Failed to get credentials from scheduler: %v", err)
	}

	version = strings.Split(version, "-")[0]

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

	ipc.SendEvent("started", fmt.Sprintf("%s (%s) started", appName, version), 0)
	ipc.SendAudit(fmt.Sprintf("%s (%s) started", appName, version))

	// 3. Parse Target DB configuration
	var targetCfg TargetDBConfig
	configSource := "Environment Variables"
	jsonConfig := os.Getenv("MITM_DB_CONFIG_JSON")

	if jsonConfig != "" {
		var fullCfg struct {
			DB struct {
				Host     string `json:"host"`
				Port     int    `json:"port"`
				User     string `json:"user"`
				Password string `json:"password"`
				Database string `json:"database"`
				SSLMode  bool   `json:"sslmode"`
			} `json:"db"`
		}
		if err := json.Unmarshal([]byte(jsonConfig), &fullCfg); err != nil {
			if ipc != nil {
				ipc.SendEvent("failed", fmt.Sprintf("Failed to parse MitM database JSON config: %v", err), 0)
			}
			log.Fatalf("Failed to parse MitM JSON configuration: %v", err)
		}
		targetCfg.Host = fullCfg.DB.Host
		targetCfg.Port = fullCfg.DB.Port
		targetCfg.User = fullCfg.DB.User
		targetCfg.Password = fullCfg.DB.Password
		targetCfg.Database = fullCfg.DB.Database
		if fullCfg.DB.SSLMode {
			os.Setenv("MITM_DB_SSLMODE", "require")
		} else {
			os.Setenv("MITM_DB_SSLMODE", "disable")
		}
		configSource = "JSON Config (MITM_DB_CONFIG_JSON)"
	} else {
		targetCfg.Host = os.Getenv("MITM_DB_HOST")
		if portStr := os.Getenv("MITM_DB_PORT"); portStr != "" {
			targetCfg.Port, _ = strconv.Atoi(portStr)
		}
		targetCfg.User = os.Getenv("MITM_DB_USER")
		targetCfg.Password = os.Getenv("MITM_DB_PASSWORD")
		targetCfg.Database = os.Getenv("MITM_DB_NAME")
	}

	if targetCfg.Host == "" {
		if ipc != nil {
			ipc.SendEvent("failed", "MitM database configuration missing in ENV", 0)
		}
		log.Fatal("MitM database credentials not found in environment (MITM_DB_HOST or MITM_DB_CONFIG_JSON)")
	}

	if ipc != nil {
		ipc.SendAudit(fmt.Sprintf("Loaded database configuration from %s", configSource))
	}

	// 3b. Parse optional collector arguments from scheduler (now in os.Args[1])
	tableName := "employees"
	cursorColumn := "" // No default, to allow tables without 'id'
	topicName := "employee.data"
	businessKeyCol := "id" // Default fallback

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
			if colArgs.BusinessKeyColumn != "" {
				businessKeyCol = colArgs.BusinessKeyColumn
			}
		} else {
			log.Printf("Warning: Failed to parse collector arguments from os.Args[1]: %v", err)
		}
	}

	if strings.ToLower(cursorColumn) == "none" {
		cursorColumn = ""
	}

	// Sanitize identifiers (prevent SQL injection)
	if err := validateIdentifier(tableName); err != nil {
		if ipc != nil { ipc.SendEvent("failed", fmt.Sprintf("Invalid table name '%s': %v", tableName, err), 0) }
		log.Fatalf("Invalid table name '%s': %v", tableName, err)
	}
	if err := validateIdentifier(cursorColumn); err != nil {
		if ipc != nil { ipc.SendEvent("failed", fmt.Sprintf("Invalid cursor column '%s': %v", cursorColumn, err), 0) }
		log.Fatalf("Invalid cursor column '%s': %v", cursorColumn, err)
	}
	sanitizedTable := tableName

	if targetCfg.SourceName == "" {
		targetCfg.SourceName = "PG_EMPLOYEE"
	}

	if ipc != nil {
		ipc.Topic = topicName
		ipc.SourceName = targetCfg.SourceName
	}

	var mitmDSN string
	if targetCfg.DSN != "" {
		mitmDSN = targetCfg.DSN
	} else {
		sslMode := "disable"
	envSSLMode := os.Getenv("MITM_DB_SSLMODE")
	if envSSLMode != "" {
		if envSSLMode == "true" {
			sslMode = "require"
		} else if envSSLMode == "false" {
			sslMode = "disable"
		} else {
			sslMode = envSSLMode
		}
	}
		mitmDSN = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			targetCfg.User, targetCfg.Password, targetCfg.Host, targetCfg.Port, targetCfg.Database, sslMode)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 4. Connect to MitM target database
	config_mitmPool, err := pgxpool.ParseConfig(mitmDSN)
	if err == nil {
		config_mitmPool.MaxConns = 20
		config_mitmPool.MaxConnIdleTime = 5 * time.Minute
		config_mitmPool.MaxConnLifetime = 1 * time.Hour
	}
	var mitmPool *pgxpool.Pool
	if err == nil {
		mitmPool, err = pgxpool.NewWithConfig(ctx, config_mitmPool)
	}
	if err != nil {
		msg := fmt.Sprintf("Failed to connect to MitM database: %v", err)
		ipc.SendEvent("failed", msg, 0)
		ipc.SendAudit("ERROR: " + msg)
		log.Fatalf("%s", msg)
	}
	defer mitmPool.Close()

	ipc.SendEvent("processing", "Connected to MitM database", 20)

	// 5. Load KEK from environment
	masterKey := os.Getenv("MASTER_KEY")
	if masterKey == "" {
		ipc.SendEvent("failed", "Missing MASTER_KEY environment variable", 0)
		log.Fatal("Missing MASTER_KEY environment variable")
	}

	kek, err := validateKEK(masterKey)
	if err != nil {
		if ipc != nil {
			ipc.SendEvent("failed", err.Error(), 0)
		}
		log.Fatalf("%v", err)
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
		msg := fmt.Sprintf("Failed to parse decrypted source database configuration: %v", err)
		ipc.SendEvent("failed", msg, 0)
		ipc.SendAudit("ERROR: " + msg)
		log.Fatalf("%s", msg)
	}

	var sourceDSN string
	if sourceCfg.DSN != "" {
		sourceDSN = sourceCfg.DSN
	} else {
		sourceDSN = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
			sourceCfg.User, sourceCfg.Password, sourceCfg.Host, sourceCfg.Port, sourceCfg.Database)
	}

	ipc.SendAudit(fmt.Sprintf("DEBUG: Trying to connect to PostgreSQL at %s:%d with Database '%s' (User: %s)", sourceCfg.Host, sourceCfg.Port, sourceCfg.Database, sourceCfg.User))

	// 11. Connect to source database
	config_sourcePool, err := pgxpool.ParseConfig(sourceDSN)
	if err == nil {
		config_sourcePool.MaxConns = 20
		config_sourcePool.MaxConnIdleTime = 5 * time.Minute
		config_sourcePool.MaxConnLifetime = 1 * time.Hour
	}
	var sourcePool *pgxpool.Pool
	if err == nil {
		sourcePool, err = pgxpool.NewWithConfig(ctx, config_sourcePool)
	}
	if err != nil {
		msg := fmt.Sprintf("Failed to connect to PostgreSQL source database: %v", err)
		ipc.SendEvent("failed", msg, 0)
		ipc.SendAudit("ERROR: " + msg)
		log.Fatalf("%s", msg)
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
	recordsFailed := 0
	maxCursorValue := ""

	ipc.SendEvent("processing", "Preparing dynamic record ingestion", 70)

	batch := &pgx.Batch{}
	batchSize := 0
	const maxBatchSize = 1000

	executeBatch := func(cursorToSave string) {
		if batchSize == 0 {
			return
		}
		
		if cursorToSave != "" {
			batch.Queue(`
				INSERT INTO ingestion_cursors (source_name, last_cursor, updated_at)
				VALUES ($1, $2, NOW())
				ON CONFLICT (source_name) 
				DO UPDATE SET last_cursor = EXCLUDED.last_cursor, updated_at = NOW()
			`, targetCfg.SourceName, cursorToSave)
		}
		
		tx, err := mitmPool.Begin(ctx)
		if err != nil {
			log.Printf("Failed to begin transaction for batch: %v", err)
			recordsFailed += batchSize
			batch = &pgx.Batch{}
			batchSize = 0
			return
		}
		
		br := tx.SendBatch(ctx, batch)
		
		var batchError error
		for i := 0; i < batchSize; i++ {
			_, err := br.Exec()
			if err != nil {
				batchError = err
				break
			}
		}
		
		if cursorToSave != "" && batchError == nil {
			_, err := br.Exec()
			if err != nil {
				batchError = err
			}
		}
		
		br.Close()
		
		if batchError != nil {
			tx.Rollback(ctx)
			log.Printf("Batch exec error: %v", batchError)
			recordsFailed += batchSize
		} else {
			if err := tx.Commit(ctx); err != nil {
				log.Printf("Failed to commit batch tx: %v", err)
				recordsFailed += batchSize
			} else {
				recordsIngested += batchSize
			}
		}
		
		batch = &pgx.Batch{}
		batchSize = 0
	}


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

		// Determine Business Key
		var businessKey string
		if bkVal, ok := rowMap[businessKeyCol]; ok && bkVal != nil {
			businessKey = fmt.Sprintf("%v", bkVal)
		} else if currentCursorVal != "" {
			businessKey = currentCursorVal
		} else {
			log.Printf("Missing business key for row, skipping")
			continue
		}

		// Generate deterministic Correlation ID
		namespaceMitM := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
		correlationID := uuid.NewSHA1(namespaceMitM, []byte(businessKey))

		// Queue in batch
		batch.Queue(`
			INSERT INTO raw_ingestion (topic, source_system, correlation_id, payload, nonce, dek_id, status)
			VALUES ($1, $2, $3, $4, $5, $6, 'pending')
		`, topicName, targetCfg.SourceName, correlationID, encryptedPayload, nonce, dekID)
		batchSize++
		
		if currentCursorVal != "" {
			maxCursorValue = currentCursorVal
		}

		if batchSize >= maxBatchSize {
			executeBatch(maxCursorValue)
		}
	}

	executeBatch(maxCursorValue)

	if err := rows.Err(); err != nil {
		msg := fmt.Sprintf("Error reading rows: %v", err)
		if ipc != nil {
			ipc.SendEvent("failed", msg, 0)
		}
		log.Fatalf("%s", msg)
	}

	// 15. Cursor is updated atomically with batches
	if recordsIngested > 0 && maxCursorValue != "" {
		ipc.SendAudit(fmt.Sprintf("Ingested %d PostgreSQL records. Cursor updated to %s.", recordsIngested, maxCursorValue))
	}

	// 16. Finish execution
	ipc.SendAudit(fmt.Sprintf("Successfully processed and ingested %d PostgreSQL records into RAW table. Failed: %d", recordsIngested, recordsFailed))
	ipc.SendAudit(fmt.Sprintf("%s (%s) finished", appName, version))
	log.Printf("Collector finished. Ingested %d records. Failed %d.", recordsIngested, recordsFailed)
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

func validateKEK(masterKey string) ([]byte, error) {
	var kek []byte
	if decoded, err := base64.StdEncoding.DecodeString(masterKey); err == nil {
		kek = decoded
	} else {
		kek = []byte(masterKey)
	}

	if len(kek) != 32 {
		return nil, fmt.Errorf("Invalid MASTER_KEY length: expected 32 bytes, got %d", len(kek))
	}
	return kek, nil
}

func fetchCredentialsFromScheduler() (string, string, error) {
	runIDStr := os.Getenv("RUN_ID")
	socketPath := os.Getenv("SCHEDULER_SOCKET_PATH")
	if runIDStr == "" || socketPath == "" {
		return "", "", fmt.Errorf("not running under scheduler")
	}
	
	runID, err := strconv.Atoi(runIDStr)
	if err != nil {
		return "", "", err
	}
	
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return "", "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	req := map[string]interface{}{
		"type":   "get_credentials",
		"run_id": runID,
	}
	data, _ := json.Marshal(req)
	if _, err := conn.Write(append(data, '\n')); err != nil {
		return "", "", err
	}

	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		var resp struct {
			MasterKey    string `json:"master_key"`
			DBConfigJSON string `json:"db_config_json"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &resp); err == nil {
			return resp.DBConfigJSON, resp.MasterKey, nil
		}
	}
	return "", "", fmt.Errorf("no response or invalid JSON from scheduler")
}

// validateIdentifier ensures the string only contains safe characters for SQL identifiers
func validateIdentifier(id string) error {
	if id == "" {
		return nil
	}
	for _, char := range id {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_' || char == '.') {
			return fmt.Errorf("invalid character in identifier: %c", char)
		}
	}
	return nil
}
