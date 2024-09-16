"""File DB implimentation"""

import os
import time

FILE_PATH = "students_db.txt"
SEPARATOR = ";"

def _delete_db():
    """Delete db file."""
    if os.path.exists(FILE_PATH):
        os.remove(FILE_PATH)
        print(f"{FILE_PATH} has been deleted.")
    else:
        print(f"{FILE_PATH} does not exist.")

def _read_records():
    """Read all records from the file."""
    if not os.path.exists(FILE_PATH):
        return []
    with open(FILE_PATH, "r", encoding="utf-8") as file:
        return file.readlines()

def _write_records(records):
    """Write all records to the file."""
    with open(FILE_PATH, "w", encoding="utf-8") as file:
        file.writelines(records)

def create_student(full_name, start_date, average_grade, comment):
    """Add a new student record to the database."""
    records = _read_records()
    key_size = len(full_name)
    data = f"{start_date}{SEPARATOR}{average_grade}{SEPARATOR}{comment}"
    rest_size = len(data)
    record = f"{key_size}{SEPARATOR}{rest_size}{SEPARATOR}{full_name}{SEPARATOR}{data}\n"

    for rec in records:
        if rec.startswith(f"{key_size}{SEPARATOR}{full_name}{SEPARATOR}"):
            raise ValueError("Student already exists.")

    records.append(record)
    _write_records(records)

def read_student(full_name):
    """Retrieve the most recent record for a student."""
    records = _read_records()
    key_size = len(full_name)

    for rec in reversed(records):
        parts = rec.split(SEPARATOR)
        if len(parts) < 4:
            continue
        if int(parts[0]) == key_size and parts[2] == full_name:
            return {
                'full_name': full_name,
                'start_date': parts[3],
                'average_grade': float(parts[4]),
                'comment': SEPARATOR.join(parts[5:]).strip()
            }

    raise ValueError("Student not found.")

def update_student(full_name, start_date, average_grade, comment):
    """Update or add a new student record."""
    records = _read_records()
    key_size = len(full_name)
    data = f"{start_date}{SEPARATOR}{average_grade}{SEPARATOR}{comment}"
    rest_size = len(data)
    record = f"{key_size}{SEPARATOR}{rest_size}{SEPARATOR}{full_name}{SEPARATOR}{data}\n"

    updated = False
    new_records = []
    for rec in records:
        if rec.startswith(f"{key_size}{SEPARATOR}{full_name}{SEPARATOR}"):
            updated = True
            new_records.append(record)
        else:
            new_records.append(rec)

    if not updated:
        new_records.append(record)

    _write_records(new_records)

# ===========================~~~===============================

perf_record = []
perf_repeat = [1, 10, 100, 1000, 10000, 100000]

def measure_perf(func, measure_times = 5):
    """Mearsure operation in repeat"""
    for turn in range(measure_times):
        print(f"\nTurn: {turn+1}\n")
        for repeat in perf_repeat:
            start_time = time.time()
            func(repeat)
            end_time = time.time()
            time_taken = round(end_time - start_time, 6)
            print(f"turn: {turn+1}\trepeat: {repeat}\ttime_taken: {time_taken} seconds\n")

# ===========================~~~===============================

def create_operation(repeat):
    """Create Operation Measurement"""
    _delete_db()
    print(f"create_operation_repeat: {repeat}")
    for i in range(repeat):
        create_student("Full Name","2022-07-30", 92.3, f"Comment {i}")

print("\nCREATE")
measure_perf(create_operation)

# ===========================~~~===============================

def read_operation(repeat):
    """Create Operation Measurement"""
    print(f"create_operation_repeat: {repeat}")
    for i in range(repeat):
        read_student("Full Name")

print("\nREAD")
measure_perf(read_operation)

# ===========================~~~===============================

def update_operation(repeat):
    """Create Operation Measurement"""
    _delete_db()
    print(f"create_operation_repeat: {repeat}")
    for i in range(repeat):
        update_student("Full Name 2","2022-07-30", 92.3, f"Comment {i}")

print("\nUPDATE")
measure_perf(update_operation)

# ===========================~~~===============================
