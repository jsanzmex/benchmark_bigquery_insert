import time
import uuid
import random
import datetime
from google.cloud import bigquery
from prettytable import PrettyTable

# Initialize BigQuery client
client = bigquery.Client()

# Replace with your table ID
table_id = "uncard-qa.julio_dataset.julio_table"

# Function to generate sample row
def generate_sample_row():
    return {
        "id": str(uuid.uuid4()),
        "description": "Sample description " + str(random.randint(1, 1000)),
        "details": "Sample details " + str(random.randint(1, 1000)),
        "tags": ["tag1", "tag2", f"tag{random.randint(3, 10)}"],
        "created_at": datetime.datetime.utcnow().isoformat(),
        "is_active": random.choice([True, False]),
        "views": random.randint(0, 10000)
    }

# Function to generate a sample row as tuple (for insert_rows)
def generate_sample_row_tuple():
    return (
        str(uuid.uuid4()),
        "Sample description " + str(random.randint(1, 1000)),
        "Sample details " + str(random.randint(1, 1000)),
        ["tag1", "tag2", f"tag{random.randint(3, 10)}"],
        datetime.datetime.utcnow().isoformat(),
        random.choice([True, False]),
        random.randint(0, 10000)
    )

# Function to insert rows using insert_rows_json
def insert_with_json():
    rows = [generate_sample_row() for _ in range(1000)]
    start_time = time.time()
    errors = client.insert_rows_json(table_id, rows)
    elapsed_time = time.time() - start_time
    return elapsed_time, errors

# Function to insert rows using insert_rows
def insert_with_rows():
    # Retrieve table schema
    table = client.get_table(table_id)  # Fetches table metadata
    schema = table.schema  # Table schema
    
    # Generate rows matching the schema
    rows = [generate_sample_row_tuple() for _ in range(1000)]
    
    # Insert rows
    start_time = time.time()
    errors = client.insert_rows(table, rows, selected_fields=schema)
    elapsed_time = time.time() - start_time
    
    return elapsed_time, errors


# Function to insert rows using a multi-line SQL INSERT
def insert_with_multi_line():
    rows = [generate_sample_row() for _ in range(1000)]
    values = ", ".join(
        f"('{row['id']}', '{row['description']}', '{row['details']}', {row['tags']}, '{row['created_at']}', {row['is_active']}, {row['views']})"
        for row in rows
    )
    query = f"""
    INSERT INTO `{table_id}` (id, description, details, tags, created_at, is_active, views)
    VALUES {values}
    """
    start_time = time.time()
    query_job = client.query(query)
    query_job.result()  # Wait for completion
    elapsed_time = time.time() - start_time
    return elapsed_time

# Main function to compare latencies
def main():
    results_table = PrettyTable()
    results_table.field_names = ["Method", "Time Taken (s)", "Errors"]
    
    # Insert with insert_rows_json
    json_time, json_errors = insert_with_json()
    results_table.add_row(["insert_rows_json", json_time, json_errors])
    
    # Insert with insert_rows
    rows_time, rows_errors = insert_with_rows()
    results_table.add_row(["insert_rows", rows_time, rows_errors])
    
    # Insert with multi-line SQL INSERT
    try:
        multi_time = insert_with_multi_line()
        results_table.add_row(["multi-line SQL", multi_time, "None"])
    except Exception as e:
        results_table.add_row(["multi-line SQL", "Failed", str(e)])
    
    # Print results
    print(results_table)

if __name__ == "__main__":
    main()

