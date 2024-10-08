import csv
import json
import yaml
import os
import sys
import threading
import queue
from datetime import datetime
import time

# Function to infer the data type based on the YAML config
def cast_value(value, data_type, data_format, default, type_policy):
    if value == "" or value is None:
        if type_policy == "nullable":
            return None
        return default

    try:
        if data_type == "int":
            return int(value)
        elif data_type == "float":
            return float(value)
        elif data_type == "date":
            if data_format:
                return datetime.strptime(value, data_format).date()
            return datetime.strptime(value, '%Y-%m-%d').date()
        elif data_type == "datetime":
            if data_format:
                return datetime.strptime(value, data_format)
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        elif data_type == "string":
            return str(value)
    except ValueError as e:
        if type_policy == "flexible":
            return str(value)
        elif type_policy == "nullable":
            return None
        else:
            raise ValueError(f"Error parsing {value} as {data_type}: {e}")
    return value

# Worker function for processing CSV rows
def process_row(q, results, yaml_config):
    while True:
        index, row = q.get()
        if row is None:
            break
        row_data = {}
        for col in yaml_config['columns']:
            col_index = col['index']
            data_type = col['type'] if 'type' in col else 'string'
            data_format = col['format'] if 'format' in col else None
            default_value = col['default'] if 'default' in col else None
            type_policy = col.get('type_policy', 'strict')
            field = col['field']

            value = row[col_index] if col_index < len(row) else None
            row_data[field] = cast_value(value, data_type, data_format, default_value, type_policy)

        if index < len(results):
            results[index] = row_data
        else:
            results.append(row_data)
        q.task_done()

# Function to read the YAML config
def read_yaml_config(yaml_file):
    with open(yaml_file, 'r') as file:
        return yaml.safe_load(file)

# Main function to handle CSV to JSON processing
def process_csv_to_json(csv_file, yaml_file, output_json_file):
    start_time = time.time()
    yaml_config = read_yaml_config(yaml_file)

    # Initialize queues and threading
    q = queue.Queue()
    results = [None] * 100  # Adjust size based on expected row count
    threads = []

    # Set to store unique rows and avoid duplicates
    processed_rows = set()

    # Spawn worker threads
    for _ in range(4):  # Adjust number of threads based on CPU cores
        t = threading.Thread(target=process_row, args=(q, results, yaml_config))
        t.start()
        threads.append(t)

    row_count = 0
    ignored_rows = 0
    unique_row_count = 0
    total_rows = sum(1 for _ in open(csv_file)) - (1 if yaml_config['header'] else 0)
    last_progress_time = time.time()

    ignore_duplicates = yaml_config.get('ignore_duplicates', False)

    with open(csv_file, 'r') as f:
        reader = csv.reader(f)

        # Skip the header row if the config specifies it
        if yaml_config['header']:
            next(reader)

        # Enqueue rows for processing
        for index, row in enumerate(reader):
            # Convert row to a tuple (which is hashable) to check for duplicates
            row_tuple = tuple(row)
            if row_tuple not in processed_rows or ignore_duplicates is False:
                processed_rows.add(row_tuple)
                q.put((index, row))
                unique_row_count += 1
            else:
                ignored_rows += 1
            row_count += 1

            # Print progress every 20 seconds
            current_time = time.time()
            if current_time - last_progress_time >= 20:
                completion_percentage = (row_count / total_rows) * 100
                print(f"Processed {row_count} out of {total_rows} rows ({completion_percentage:.2f}% complete)")
                last_progress_time = current_time

    # Block until all rows have been processed
    q.join()

    # Stop workers
    for _ in threads:
        q.put((None, None))
    for t in threads:
        t.join()

    # For output_json_file create directory if it doesn't exist
    if not os.path.exists(os.path.dirname(output_json_file)):
        os.makedirs(os.path.dirname(output_json_file))

    # Write the results to a JSON file
    with open(output_json_file, 'w') as json_file:
        json.dump([row for row in results if row is not None], json_file, indent=4, default=str)

    end_time = time.time()
    processing_time = end_time - start_time
    rows_per_second = row_count / processing_time

    print(f"Processed {row_count} rows in {processing_time:.2f} seconds")
    if ignore_duplicates:
      print(f"Ignored {ignored_rows} duplicate rows")
      print(f"Found {unique_row_count} unique rows")
    print(f"Average processing speed: {rows_per_second:.2f} rows/second")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py <input_csv> <yaml_config> <output_json>")
        sys.exit(1)

    input_csv = sys.argv[1]
    yaml_config = sys.argv[2]
    output_json = sys.argv[3]

    process_csv_to_json(input_csv, yaml_config, output_json)
