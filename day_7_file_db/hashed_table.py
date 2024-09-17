"""Select with Hashed Table"""

import os
import time

from faker import Faker

# ==================================================
# Configs
# ==================================================

FILE_PATH = "hashed_db.txt"
SEPARATOR = ";"
hash_table_index = {}

# ==================================================
# Helper Functions
# ==================================================

def _write_file_db(content: str, file_path: str = FILE_PATH):
    """Write to file db."""
    with open(file_path, "a", encoding="utf-8") as f_db:
        position = f_db.tell()  # Get the file position before writing
        f_db.write(content + "\n")  # Add newline after each record
    return position  # Return the file position for indexing

def _read_file_db(file_path: str = FILE_PATH):
    """Read from file db."""
    if not os.path.exists(file_path):
        print(f"Error: Database `{file_path}` does not exist.")
        return False
    with open(file_path, "r", encoding="utf-8") as f_db:
        return f_db.readlines()

def _format_student_record(full_name, enrollment_date, mark, comment, separator=SEPARATOR):
    """Format the record for operation."""
    key_size = len(full_name)
    data = f"{enrollment_date}{separator}{mark}{separator}{comment}"
    data_size = len(data)
    return f"{key_size}{separator}{data_size}{separator}{full_name}{separator}{data}"

def _build_hash_index():
    """Build the hash table index from file."""
    hash_table_index.clear()
    if os.path.exists(FILE_PATH):
        with open(FILE_PATH, "r", encoding="utf-8") as f_db:
            while True:
                pos = f_db.tell()
                line = f_db.readline()
                if not line:
                    break
                _, _, key, *_ = line.strip().split(SEPARATOR)
                hash_table_index[key] = pos

# ==================================================
# Insert and Select with Hash Table Index
# ==================================================

def insert_student_hash(full_name, enrollment_date, mark, comment):
    """Insert student and update hash table index."""
    if full_name in hash_table_index:
        # print(f"Error: Student {full_name} already exists in index.")
        return None
    record = _format_student_record(full_name, enrollment_date, mark, comment)
    position = _write_file_db(record)  # Write to file and get position
    hash_table_index[full_name] = position  # Update the hash index
    # print(f"Student {full_name} inserted successfully at position {position}.")

def select_student_hash(full_name):
    """Select student by full_name using hash table index."""
    if full_name not in hash_table_index:
        # print(f"Error: Student {full_name} not found in index.")
        return None
    pos = hash_table_index[full_name]
    with open(FILE_PATH, "r", encoding="utf-8") as f_db:
        f_db.seek(pos)  # Move to the position in the file
        return f_db.readline().strip()

# ==================================================
# Performance Measurement
# ==================================================

fake = Faker()

_build_hash_index()

def measure_performance_hash():
    """Measure performance of insert and select with hash table index."""

    operations_count = 10_000
    insert_time_avg = 0
    read_time_avg = 0

    for _ in range(operations_count):
        name = fake.name()
        date = fake.date()
        mark = fake.random_int()
        comment = fake.sentence()

        # Insert students
        i_time = time.time()
        insert_student_hash(name, date, mark, comment)
        insert_time_avg += time.time() - i_time

        # Rebuild the sorted array index
        _build_hash_index()

        # Select students
        r_start = time.time()
        select_student_hash(name)
        read_time_avg += time.time() - r_start

    insert_time_avg = round(insert_time_avg/operations_count, 6)
    insert_time_taken = f"{insert_time_avg:.10f}"
    print(f"insert time taken for {operations_count} operations: {insert_time_taken} s")

    read_time_avg = round(read_time_avg/operations_count, 6)
    read_time_taken = f"{read_time_avg:.10f}"
    print(f"select time taken for {operations_count} operations: {read_time_taken} s")

# ==================================================
# Testing Hash Table Indexing
# ==================================================
measure_performance_hash()
