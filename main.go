package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

type ColumnConfig struct {
	Index      int    `yaml:"index"`
	Field      string `yaml:"field"`
	Label      string `yaml:"label"`
	Type       string `yaml:"type"`
	TypePolicy string `yaml:"type_policy"`
	Default    string `yaml:"default"`
}

type Config struct {
	Header           bool           `yaml:"header"`
	Columns          []ColumnConfig `yaml:"columns"`
	IgnoreDuplicates bool           `yaml:"ignore_duplicates"`
}

func loadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func parseDate(value, defaultValue string) time.Time {
	layout := "2006-01-02"
	if parsed, err := time.Parse(layout, value); err == nil {
		return parsed
	}
	parsed, _ := time.Parse(layout, defaultValue)
	return parsed
}

func parseDateTime(value, defaultValue string) time.Time {
	layout := "2006-01-02T15:04:05Z"
	if parsed, err := time.Parse(layout, value); err == nil {
		return parsed
	}
	parsed, _ := time.Parse(layout, defaultValue)
	return parsed
}

func castValue(value string, col ColumnConfig) interface{} {
	if value == "" {
		value = col.Default
	}

	switch col.Type {
	case "int":
		v, err := strconv.Atoi(value)
		if err != nil && col.TypePolicy == "strict" {
			log.Fatalf("Error casting value %s to int for column %s", value, col.Field)
		}
		if err != nil && col.TypePolicy == "nullable" {
			return nil
		}
		return v
	case "bool":
		v, err := strconv.ParseBool(value)
		if err != nil && col.TypePolicy == "strict" {
			log.Fatalf("Error casting value %s to bool for column %s", value, col.Field)
		}
		if err != nil && col.TypePolicy == "nullable" {
			return nil
		}
		return v
	case "date":
		return parseDate(value, col.Default)
	case "datetime":
		return parseDateTime(value, col.Default)
	case "string":
		return value
	default:
		return value
	}
}

func main() {
	startTime := time.Now()

	// Parse command-line flags
	inputFile := flag.String("input", "", "Input CSV file")
	configFile := flag.String("config", "", "YAML configuration file")
	outputFile := flag.String("output", "", "Output JSON file")
	flag.Parse()

	if *inputFile == "" || *configFile == "" || *outputFile == "" {
		log.Fatal("Input file, config file, and output file are required")
	}

	// Load YAML configuration
	config, err := loadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Open the CSV file
	file, err := os.Open(*inputFile)
	if err != nil {
		log.Fatal("Unable to open CSV file", err)
	}
	defer file.Close()

	fmt.Printf("Time to open file: %v\n", time.Since(startTime))

	// Read the CSV file
	reader := csv.NewReader(file)

	// Skip the header if config says so
	if config.Header {
		_, _ = reader.Read()
	}

	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal("Unable to read CSV file", err)
	}

	fmt.Printf("Time to read file: %v\n", time.Since(startTime))

	var jsonData []map[string]interface{}
	var wg sync.WaitGroup
	jsonDataMutex := &sync.Mutex{}
	seenMutex := &sync.Mutex{}

	// Track seen rows to avoid duplicates
	seen := make(map[string]struct{})
	var processedCount, ignoredCount int

	// Process rows concurrently
	for i, row := range records {
		wg.Add(1)
		go func(i int, row []string) {
			defer wg.Done()

			// Create a unique key for the current row based on relevant fields
			uniqueKey := ""
			for _, col := range config.Columns {
				if config.IgnoreDuplicates {
					if col.Index < len(row) {
						uniqueKey += row[col.Index] + "|"
					}
				}
			}

			// Check for duplicates
			if config.IgnoreDuplicates {
				seenMutex.Lock()
				if _, exists := seen[uniqueKey]; exists {
					ignoredCount++
					seenMutex.Unlock()
					return // Skip processing this row
				}
				seen[uniqueKey] = struct{}{} // Mark this row as seen
				seenMutex.Unlock()
			}

			entry := make(map[string]interface{})
			for _, col := range config.Columns {
				// Ensure the column index is within the bounds of the row
				if col.Index < len(row) {
					value := castValue(row[col.Index], col)
					entry[col.Label] = value
				} else {
					log.Printf("Warning: Column index %d out of range for row %d", col.Index, i)
				}
			}

			jsonDataMutex.Lock()
			jsonData = append(jsonData, entry)
			processedCount++
			jsonDataMutex.Unlock()
		}(i, row)
	}

	wg.Wait()

	// Convert to JSON
	jsonPayload, err := json.MarshalIndent(jsonData, "", "  ")
	if err != nil {
		log.Fatal("Unable to marshal to JSON", err)
	}

	// Write JSON to output file
	err = os.WriteFile(*outputFile, jsonPayload, 0644)
	if err != nil {
		log.Fatal("Unable to write JSON to file", err)
	}

	totalTime := time.Since(startTime)
	rowCount := len(records)
	avgSpeed := float64(processedCount) / totalTime.Seconds()

	fmt.Printf("Processed %d rows in %.2f seconds\n", rowCount, totalTime.Seconds())
	if config.IgnoreDuplicates {
		fmt.Printf("Ignored %d duplicate rows\n", ignoredCount)
		fmt.Printf("Found %d unique rows\n", processedCount)
	}
	fmt.Printf("Average processing speed: %.2f rows/second\n", avgSpeed)
}
