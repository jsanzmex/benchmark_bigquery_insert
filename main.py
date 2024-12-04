import time
import uuid
import random
import datetime
import csv
from google.cloud import bigquery
from prettytable import PrettyTable

# Initialize BigQuery client
client = bigquery.Client()

# Function to generate a sample row of a given size in KB
def generate_large_sample_row(row_size_kb):
    large_string = "A" * int(row_size_kb * 1024 - 156)  # Convert to int for valid string multiplication
    return {
        "id": str(uuid.uuid4()),
        "description": large_string[:512],
        "details": large_string[512:],
        "tags": ["tag1", "tag2", "tag3"],
        "created_at": datetime.datetime.utcnow().isoformat(),
        "is_active": random.choice([True, False]),
        "views": random.randint(0, 10000)
    }

# Function to generate a sample row as a tuple
def generate_large_sample_row_tuple(row_size_kb):
    large_string = "A" * int(row_size_kb * 1024 - 156)  # Convert to int
    return (
        str(uuid.uuid4()),
        large_string[:512],
        large_string[512:],
        ["tag1", "tag2", "tag3"],
        datetime.datetime.utcnow().isoformat(),
        random.choice([True, False]),
        random.randint(0, 10000)
    )

# Function to insert rows using insert_rows_json
def insert_with_json(records, row_size_kb, table_id):
    rows = [generate_large_sample_row(row_size_kb) for _ in range(records)]
    start_time = time.time()
    errors = client.insert_rows_json(table_id, rows)
    elapsed_time = time.time() - start_time
    return elapsed_time, errors

# Function to insert rows using insert_rows
def insert_with_rows(records, row_size_kb, table_id):
    table = client.get_table(table_id)
    schema = table.schema
    rows = [generate_large_sample_row_tuple(row_size_kb) for _ in range(records)]
    start_time = time.time()
    errors = client.insert_rows(table, rows, selected_fields=schema)
    elapsed_time = time.time() - start_time
    return elapsed_time, errors

# Function to insert rows using a multi-line SQL INSERT
def insert_with_multi_line(records, row_size_kb, table_id):
    rows = [generate_large_sample_row(row_size_kb) for _ in range(records)]
    values = ", ".join(
        f"('{row['id']}', '{row['description']}', '{row['details']}', {row['tags']}, '{row['created_at']}', {row['is_active']}, {row['views']})"
        for row in rows
    )
    query = f"""
    INSERT INTO `{table_id}` (id, description, details, tags, created_at, is_active, views)
    VALUES {values}
    """
    start_time = time.time()
    try:
        query_job = client.query(query)
        query_job.result()  # Wait for completion
        elapsed_time = time.time() - start_time
        return elapsed_time
    except Exception as e:
        elapsed_time = time.time() - start_time
        return elapsed_time

# Main function to compare latencies and data sizes
def main():
    results_table = PrettyTable()
    results_table.field_names = ["Method", "Table", "Records", "Time Taken (s)", "Total Data Size (KB)", "Errors"]

    # CSV file setup
    csv_file = "performance_results.csv"
    csv_headers = ["Method", "Table", "Records", "Time Taken (s)", "Total Data Size (KB)", "Errors"]

    # Open the CSV file and write headers
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(csv_headers)

        # Define test cases for different tables and row sizes
        test_cases = [
            {"table": "uncard-qa.julio_dataset.julio_table", "row_size_kb": 0.156},  # Original table
            {"table": "uncard-qa.julio_dataset.julio_table_1kb", "row_size_kb": 1},  # New 1 KB table
            {"table": "uncard-qa.julio_dataset.julio_table_10kb", "row_size_kb": 10},  # New 10 KB table
        ]

        for test in test_cases:
            table_id = test["table"]
            row_size_kb = test["row_size_kb"]

            print(f"\nTesting {table_id} with {row_size_kb} KB rows...")

            # Test with 1000 records
            print("\nTesting with 1000 records...")
            json_time, json_errors = insert_with_json(1000, row_size_kb, table_id)
            json_data_size_kb = 1000 * row_size_kb
            results_table.add_row(["insert_rows_json", table_id, 1000, json_time, json_data_size_kb, json_errors])
            writer.writerow(["insert_rows_json", table_id, 1000, json_time, json_data_size_kb, json_errors])

            rows_time, rows_errors = insert_with_rows(1000, row_size_kb, table_id)
            rows_data_size_kb = 1000 * row_size_kb
            results_table.add_row(["insert_rows", table_id, 1000, rows_time, rows_data_size_kb, rows_errors])
            writer.writerow(["insert_rows", table_id, 1000, rows_time, rows_data_size_kb, rows_errors])

            multi_time = insert_with_multi_line(1000, row_size_kb, table_id)
            multi_data_size_kb = 1000 * row_size_kb
            results_table.add_row(["multi-line SQL", table_id, 1000, multi_time, multi_data_size_kb, "None"])
            writer.writerow(["multi-line SQL", table_id, 1000, multi_time, multi_data_size_kb, "None"])

            # Test with single record
            print("\nTesting with a single record...")
            json_time, json_errors = insert_with_json(1, row_size_kb, table_id)
            json_data_size_kb = 1 * row_size_kb
            results_table.add_row(["insert_rows_json", table_id, 1, json_time, json_data_size_kb, json_errors])
            writer.writerow(["insert_rows_json", table_id, 1, json_time, json_data_size_kb, json_errors])

            rows_time, rows_errors = insert_with_rows(1, row_size_kb, table_id)
            rows_data_size_kb = 1 * row_size_kb
            results_table.add_row(["insert_rows", table_id, 1, rows_time, rows_data_size_kb, rows_errors])
            writer.writerow(["insert_rows", table_id, 1, rows_time, rows_data_size_kb, rows_errors])

            multi_time = insert_with_multi_line(1, row_size_kb, table_id)
            multi_data_size_kb = 1 * row_size_kb
            results_table.add_row(["multi-line SQL", table_id, 1, multi_time, multi_data_size_kb, "None"])
            writer.writerow(["multi-line SQL", table_id, 1, multi_time, multi_data_size_kb, "None"])

    # Print results
    print(results_table)
    print(f"\nResults have been saved to {csv_file}")

if __name__ == "__main__":
    main()
