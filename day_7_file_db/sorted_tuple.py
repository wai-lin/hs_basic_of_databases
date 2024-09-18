"""Select with sorted tuple"""

import os
import time
import bisect

from faker import Faker

FILE_PATH = "sorted_db.txt"
SEPARATOR = ";"

# ==================================================
# Configs for Sorted List Indexing
# ==================================================

sorted_array_index = []

# ==================================================
# Helper Functions
# ==================================================

def _delete_file_db(file_path: str = FILE_PATH):
    """Delete file db."""
    if not os.path.exists(file_path):
        print(f"Error: Database `{file_path}` does not exists.")
        return False
    os.remove(file_path)
    print(f"Database `{file_path}` deleted successfully.")
    return True

def _write_file_db(content: str, file_path: str = FILE_PATH):
    """Write to file db."""
    with open(file_path, "a", encoding="utf-8") as f_db:
        f_db.write(content + "\n")
        return True

def _read_file_db(file_path: str = FILE_PATH):
    """Read from file db."""
    if not os.path.exists(file_path):
        print(f"Error: Database `{file_path}` does not exists.")
        return False
    with open(file_path, "r", encoding="utf-8") as f_db:
        return f_db.readlines()

def _format_student_record(
        full_name: str,
        enrollment_date: str,
        mark: int,
        comment: str,
        separator: str = SEPARATOR
    ):
    """Format the record for operation"""
    key = full_name
    key_size = len(key)

    data = f"{enrollment_date}{separator}{mark}{separator}{comment}"
    data_size = len(data)
    return f"{key_size}{separator}{data_size}{separator}{key}{separator}{data}"

def _build_sorted_array_index():
    """Build the sorted array index from file."""
    sorted_array_index.clear()
    if os.path.exists(FILE_PATH):
        with open(FILE_PATH, "r", encoding="utf-8") as f_db:
            while True:
                pos = f_db.tell()
                line = f_db.readline()
                if not line:
                    break
                _, _, key, *_ = line.strip().split(SEPARATOR)
                bisect.insort(sorted_array_index, (key, pos))  # Keep the array sorted

# ==================================================
# Insert and Select with Sorted Array Index
# ==================================================

def insert_student_sorted(full_name, enrollment_date, mark, comment):
    """Insert student and update sorted array index."""
    idx = [name for name, _ in sorted_array_index]
    if full_name in idx:
        # print(f"Error: Student {full_name} already exists in sorted index.")
        return None
    record = _format_student_record(full_name, enrollment_date, mark, comment)
    position = _write_file_db(record)  # Write to file and get position
    bisect.insort(sorted_array_index, (full_name, position))  # Insert in sorted order
    # print(f"Student {full_name} inserted successfully at position {position}.")

def select_student_sorted(full_name):
    """Select student by full_name using binary search over sorted array."""
    idx = [name for name, _ in sorted_array_index]
    pos = bisect.bisect_left(idx, full_name)
    if pos != len(sorted_array_index) and sorted_array_index[pos][0] == full_name:
        with open(FILE_PATH, "r", encoding="utf-8") as f_db:
            f_db.seek(sorted_array_index[pos][1])  # Move to position in file
            return f_db.readline().strip()
    else:
        # print(f"Error: Student {full_name} not found in sorted index.")
        return None

# ==================================================
# Performance Measurement
# ==================================================

fake = Faker()

_build_sorted_array_index()

def measure_performance_sorted():
    """Measure performance of insert and select with sorted array index."""

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
        insert_student_sorted(name, date, mark, comment)
        insert_time_avg += time.time() - i_time

        # Rebuild the sorted array index
        _build_sorted_array_index()

        # Select students
        r_start = time.time()
        select_student_sorted(name)
        read_time_avg += time.time() - r_start

    insert_time_avg = round(insert_time_avg/operations_count, 6)
    insert_time_taken = f"{insert_time_avg:.10f}"
    print(f"insert time taken for {operations_count} operations: {insert_time_taken} s")

    read_time_avg = round(read_time_avg/operations_count, 6)
    read_time_taken = f"{read_time_avg:.10f}"
    print(f"select time taken for {operations_count} operations: {read_time_taken} s")

# ==================================================
# Testing Sorted Array Indexing
# ==================================================
measure_performance_sorted()
