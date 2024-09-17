"""File DB"""

import os
import time

from faker import Faker

# ==================================================
# Configs
# ==================================================

FILE_PATH = "file_db.txt"
SEPARATOR = ";"

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

def _if_student_exists(full_name: str, separator: str = SEPARATOR):
    """Fine if the student already exists or not."""
    content = _read_file_db()
    if content is False:
        return False
    for line in content:
        _, _, key, *_ = line.split(separator)
        if key == full_name:
            return True
        else:
            return False

# ==================================================
# Create, Update, Read operations
# ==================================================

def insert_student(
        full_name: str,
        enrollment_date: str,
        mark: int,
        comment: str,
    ):
    """Insert student if not exists."""
    already_exists = _if_student_exists(full_name)
    if already_exists:
        print(f"Student {full_name} already exists.")
        return None
    record = _format_student_record(full_name, enrollment_date, mark, comment)
    _write_file_db(record)

def update_student(
        full_name: str,
        enrollment_date: str,
        mark: int,
        comment: str,
    ):
    """Update student if exists. This will add new row at the end."""
    already_exists = _if_student_exists(full_name)
    if already_exists:
        record = _format_student_record(full_name, enrollment_date, mark, comment)
        _write_file_db(record)

def read_student(full_name: str, separator: str = SEPARATOR):
    """Read the latest student by full_name."""
    content = _read_file_db()
    if content is False:
        return False

    latest_record = None
    for line in content:
        _, _, key, *_ = line.split(separator)
        if key == full_name:
            latest_record = line.strip()
    if latest_record:
        # print(f"Latest record for {full_name}: {latest_record}")
        return latest_record
    else:
        # print(f"Error: Student {full_name} not found.")
        return False

# ==================================================
# Performance Measurements
# ==================================================

fake = Faker()

print("File DB:")

def measure_performance():
    """Measure performance of file db"""

    operations_count = 10_000
    insert_time_avg = 0
    read_time_avg = 0

    for _ in range(operations_count):
        name = fake.name()
        date = fake.date()
        mark = fake.random_int()
        comment = fake.sentence()

        i_start = time.time()
        insert_student(name, date, mark, comment)
        insert_time_avg += time.time() - i_start

        r_start = time.time()
        read_student(name)
        read_time_avg += time.time() - r_start

    insert_time_avg = round(insert_time_avg/operations_count, 6)
    insert_time_taken = f"{insert_time_avg:.10f}"
    print(f"insert time taken for {operations_count} operations: {insert_time_taken} s")

    read_time_avg = round(read_time_avg/operations_count, 6)
    read_time_taken = f"{read_time_avg:.10f}"
    print(f"select time taken for {operations_count} operations: {read_time_taken} s")

measure_performance()
