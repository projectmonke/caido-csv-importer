package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// CSVRecord holds the data from a single row of the CSV file.
type CSVRecord struct {
	ID                  int64
	Host                string
	Method              string
	Path                string
	Length              int64
	Port                int
	Raw                 []byte
	IsTLS               bool
	Query               string
	FileExtensions      string // This field is not directly used in the provided schema mapping.
	Source              string
	Alteration          string
	Edited              bool
	ParentID            sql.NullInt64
	CreatedAt           int64
	ResponseID          sql.NullInt64
	ResponseStatusCode  int
	ResponseRaw         []byte
	ResponseLength      int64
	ResponseAlteration  string
	ResponseEdited      bool
	ResponseParentID    sql.NullInt64
	ResponseCreatedAt   int64
}

// Converter handles the database connection and data insertion.
type Converter struct {
	db *sql.DB
}

// NewConverter establishes a connection to the Caido project database.
func NewConverter(projectPath string) (*Converter, error) {
	db, err := openDB(projectPath)
	if err != nil {
		return nil, err
	}
	return &Converter{db: db}, nil
}

// Close terminates the database connection.
func (c *Converter) Close() error {
	return c.db.Close()
}

// ImportFromCSV reads the CSV file and imports its data.
func (c *Converter) ImportFromCSV(path string) error {
	csvFile, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("error opening CSV file: %v", err)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	// Skip header row
	if _, err := reader.Read(); err != nil {
		return fmt.Errorf("error reading header from CSV: %v", err)
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading record from CSV: %v", err)
			continue // Skip to the next record
		}

		csvRecord, err := parseCSVRecord(record)
		if err != nil {
			log.Printf("Error parsing CSV record: %v", err)
			continue
		}

		if err := c.insertData(csvRecord); err != nil {
			log.Printf("Error inserting data for host %s: %v", csvRecord.Host, err)
		}
	}
	return nil
}

// parseCSVRecord converts a string slice from the CSV into a structured CSVRecord.
func parseCSVRecord(record []string) (CSVRecord, error) {
    // Helper function to parse boolean values
	parseBool := func(s string) bool {
		val, _ := strconv.ParseBool(s)
		return val
	}
    
    // Helper function to parse integers
	parseInt := func(s string) int64 {
		val, _ := strconv.ParseInt(s, 10, 64)
		return val
	}
    
    // Helper function to parse nullable integers
	parseNullInt := func(s string) sql.NullInt64 {
		if s == "" {
			return sql.NullInt64{}
		}
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return sql.NullInt64{}
		}
		return sql.NullInt64{Int64: val, Valid: true}
	}

	return CSVRecord{
		ID:                 parseInt(record[0]),
		Host:               record[1],
		Method:             record[2],
		Path:               record[3],
		Length:             parseInt(record[4]),
		Port:               int(parseInt(record[5])),
		Raw:                []byte(record[6]),
		IsTLS:              parseBool(record[7]),
		Query:              record[8],
		FileExtensions:     record[9],
		Source:             record[10],
		Alteration:         record[11],
		Edited:             parseBool(record[12]),
		ParentID:           parseNullInt(record[13]),
		CreatedAt:          parseInt(record[14]),
		ResponseID:         parseNullInt(record[15]),
		ResponseStatusCode: int(parseInt(record[16])),
		ResponseRaw:        []byte(record[17]),
		ResponseLength:     parseInt(record[18]),
		ResponseAlteration: record[19],
		ResponseEdited:     parseBool(record[20]),
		ResponseParentID:   parseNullInt(record[21]),
		ResponseCreatedAt:  parseInt(record[22]),
	}, nil
}


// insertData orchestrates the insertion of response and request data.
func (c *Converter) insertData(record CSVRecord) error {
	responseID, err := c.insertResponse(record)
	if err != nil {
		return err
	}

	requestID, err := c.insertRequest(responseID, record)
	if err != nil {
		return err
	}

	_, err = c.insertIntercept(requestID)
	if err != nil {
		return err
	}

	fmt.Printf("Successfully inserted request for host: %s\n", record.Host)
	return nil
}

// insertResponse inserts the HTTP response data into the database.
func (c *Converter) insertResponse(record CSVRecord) (int64, error) {
	var rawResponseID int64
	err := c.db.QueryRow("INSERT INTO raw.responses_raw (data, source, alteration) VALUES (?, ?, ?) RETURNING id",
		record.ResponseRaw, record.Source, record.ResponseAlteration).Scan(&rawResponseID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert into raw.responses_raw: %w", err)
	}

	var responseID int64
	err = c.db.QueryRow(`
		INSERT INTO responses (status_code, raw_id, length, alteration, edited, parent_id, created_at, roundtrip_time)
		VALUES (?, ?, ?, ?, ?, ?, ?, 0) RETURNING id`,
		record.ResponseStatusCode, rawResponseID, record.ResponseLength, record.ResponseAlteration, record.ResponseEdited, record.ResponseParentID, record.ResponseCreatedAt,
	).Scan(&responseID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert into responses: %w", err)
	}

	return responseID, nil
}

// insertRequest inserts the HTTP request data into the database.
func (c *Converter) insertRequest(responseID int64, record CSVRecord) (int64, error) {
	var rawRequestID int64
	err := c.db.QueryRow("INSERT INTO raw.requests_raw (data, source, alteration) VALUES (?, ?, ?) RETURNING id",
		record.Raw, record.Source, record.Alteration).Scan(&rawRequestID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert into raw.requests_raw: %w", err)
	}

	var metadataID int64
	err = c.db.QueryRow("INSERT INTO requests_metadata DEFAULT VALUES RETURNING id").Scan(&metadataID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert into requests_metadata: %w", err)
	}

	var requestID int64
	err = c.db.QueryRow(`
		INSERT INTO requests (host, method, path, length, port, is_tls, raw_id, query, response_id, source, alteration, edited, parent_id, created_at, metadata_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`,
		record.Host, record.Method, record.Path, record.Length, record.Port, record.IsTLS, rawRequestID, record.Query, responseID, record.Source, record.Alteration, record.Edited, record.ParentID, record.CreatedAt, metadataID,
	).Scan(&requestID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert into requests: %w", err)
	}

	return requestID, nil
}

// insertIntercept adds the request to the intercept view.
func (c *Converter) insertIntercept(requestID int64) (int64, error) {
	var interceptID int64
	err := c.db.QueryRow("INSERT INTO intercept_entries (request_id) VALUES (?) RETURNING id", requestID).Scan(&interceptID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert into intercept_entries: %w", err)
	}
	return interceptID, nil
}

// openDB connects to the main and raw Caido databases.
func openDB(projectPath string) (*sql.DB, error) {
	dbPath := projectPath + "/database.caido"
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("caido main database does not exist at %s", dbPath)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("error opening database.caido: %v", err)
	}
	log.Println("[INFO] Opened database.caido")

	dbRawPath := projectPath + "/database_raw.caido"
	if _, err := os.Stat(dbRawPath); os.IsNotExist(err) {
		db.Close()
		return nil, fmt.Errorf("caido raw database does not exist at %s", dbRawPath)
	}

	_, err = db.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS raw", dbRawPath))
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error attaching database_raw.caido: %v", err)
	}
	log.Println("[INFO] Attached database_raw.caido")

	return db, nil
}

func main() {
	projectPath := flag.String("p", "", "Path to the Caido project directory")
	csvPath := flag.String("f", "", "Path to the CSV file to import")
	flag.Parse()

	if *projectPath == "" || *csvPath == "" {
		log.Fatal("Both project path (-p) and CSV file path (-f) are required.")
	}

	converter, err := NewConverter(*projectPath)
	if err != nil {
		log.Fatalf("Failed to initialize converter: %v", err)
	}
	defer converter.Close()

	log.Printf("[INFO] Starting import from %s", *csvPath)
	startTime := time.Now()

	if err := converter.ImportFromCSV(*csvPath); err != nil {
		log.Fatalf("Failed to import data: %v", err)
	}

	duration := time.Since(startTime)
	log.Printf("[INFO] Import completed successfully in %v.", duration)
}
