# CSV Processing Benchmark: Go vs Python

This repository contains two scripts—one in Go and one in Python—designed to read a CSV file, process the data, and output it in JSON format. Both scripts utilize concurrency (Go routines and Python threading) for parallel processing of rows and include features like duplicate row detection and strict or flexible data type handling.

## Table of Contents
1. [Features](#features)
2. [Configuration](#configuration)
3. [Usage](#usage)
4. [Benchmarking and Performance Comparison](#benchmarking-and-performance-comparison)
5. [Error Handling](#error-handling)
6. [Contributions](#contributions)

## Features

### Common Features:
- **CSV to JSON Conversion**: Both scripts read a CSV file, apply data type transformations, and output a JSON file.
- **Configurable via YAML**: Both scripts accept a configuration file to define CSV structure, column indices, data types, and row processing behavior.
- **Concurrency**: Row processing in both Go and Python scripts is performed in parallel (Go routines and Python threading) to improve performance for large datasets.
- **Duplicate Row Detection**: An optional configuration (`ignore_duplicates`) allows the scripts to skip processing of duplicated rows.
- **Flexible Data Type Handling**: Both scripts can handle various data types, such as `int`, `bool`, `string`, `date`, `datetime`, and come with strict, flexible, or nullable type policies.
- **Benchmarking**: Both scripts print telemetry data about the total processing time, the number of rows processed, duplicates ignored, and rows retained.

### Go-Specific Features:
- **Native Go concurrency** using goroutines and mutexes for parallelism and data safety.
- **Optimized performance** for large datasets due to the speed of Go's compiled nature.

### Python-Specific Features:
- **Thread-based parallelism** using Python’s threading module, suitable for I/O-bound tasks.
- **Versatile library support** for handling CSV, JSON, and YAML parsing, making the script easy to modify and extend.

## Configuration

The processing behavior for both scripts is defined through a YAML configuration file. The configuration file specifies the CSV structure, including the column indices, data types, and how to handle missing or invalid data.

Example configuration (`config.yaml`):

```yaml
header: true
ignore_duplicates: true
columns:
  - index: 0
    field: "employee_id"
    label: "Employee ID"
    type: "int"
    type_policy: "strict"
    default: "0"
  - index: 1
    field: "first_name"
    label: "First Name"
    type: "string"
    type_policy: "flexible"
    default: "Unknown"
  - index: 3
    field: "date_of_birth"
    label: "Date of Birth"
    type: "date"
    type_policy: "nullable"
    default: "1970-01-01"
  - index: 20
    field: "salary"
    label: "Salary"
    type: "int"
    type_policy: "nullable"
    default: "50000"
# Other columns...
```

### Key Configuration Fields:
- `header`: Boolean. Defines whether the CSV contains a header row.
- `ignore_duplicates`: Boolean. Defines whether the script should skip duplicate rows.
- `columns`: Array. Defines each column with the following:
  - `index`: The column index (0-based).
  - `field`: Internal field name for data processing.
  - `label`: User-friendly label for the column.
  - `type`: Data type (int, bool, string, date, datetime).
  - `type_policy`: Strict, flexible, or nullable policy for type conversion.
  - `default`: Default value for empty or invalid data.

## Usage

### Prerequisites
- **Go**: Version 1.17 or higher.
- **Python**: Version 3.7 or higher.
- **Dependencies**: Install necessary Python libraries by running `pip install -r requirements.txt`.

### Running the Go Script
```bash
go run main.go -input=input.csv -config=config.yaml -output=output.json
```

### Running the Python Script
```bash
python csv_processor.py --input input.csv --config config.yaml --output output.json
```

## Benchmarking and Performance Comparison

### Sample Telemetry (Go)
```
Processed 112,000 rows in 0.60 seconds
Ignored 111,000 duplicate rows
Found 1,000 unique rows
Average processing speed: 188,213.41 rows/second
```

### Sample Telemetry (Python)
```
Processed 112,000 rows in 1.20 seconds
Ignored 111,000 duplicate rows
Found 1,000 unique rows
Average processing speed: 93,410.70 rows/second
```

### Performance Overview

| Metric                 | Go Script          | Python Script      |
|------------------------|--------------------|--------------------|
| Processing Time         | 1.59 seconds       | 5.05               |
| Duplicate Detection     | No                 | No                 |
| Rows per Second         | 70597.36 rows/sec  | 22195.27 rows/sec  |
| Concurrency Model       | Goroutines         | Threading          |
| Ideal Use Case          | Large datasets     | I/O-bound workloads|

**Why Go is faster**:
- Go's compiled nature and goroutines provide a significant performance boost, especially with large datasets.
- Python's Global Interpreter Lock (GIL) can limit multi-threaded performance, making Go more suited for CPU-bound tasks.

## Error Handling

Both scripts implement error handling for:
- Missing configuration or input files.
- Incorrect data types based on the provided configuration.
- Duplicate rows, based on the `ignore_duplicates` setting.
