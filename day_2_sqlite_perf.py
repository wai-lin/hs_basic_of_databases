"""SQLite Performance"""

import sqlite3
import time

# Init Database
db = sqlite3.connect("perf_measure.db")
cursor = db.cursor()

# ===========================~~~===============================

# Migration
def migrate():
    """Create students table"""
    cursor.execute("DROP TABLE IF EXISTS students")
    cursor.execute("""
        CREATE TABLE students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            start_date TEXT,
            average_grade REAL,
            comment TEXT
        )
    """)

# ===========================~~~===============================

# Operations
def create_student():
    """Create a student"""
    cursor.execute(
        """
            INSERT INTO students (full_name, start_date, average_grade, comment) VALUES (?,?,?,?)
        """,
        ("Alex Roy", "2024-06-30", 90, "Comment")
    )

def read_student():
    """Update a student"""
    cursor.execute(
        "SELECT * FROM students WHERE id = 1",
        ("Comment OK")
    )

def update_student():
    """Update a student"""
    cursor.execute(
        "UPDATE students SET comment = ? WHERE id = 1",
        ("Comment OK")
    )

# ===========================~~~===============================

# Performance Measure
perf_record = []
perf_repeat = [1, 10, 100, 1000, 10000]

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
    """Measure SQLite insert operation."""
    migrate()
    print(f"create_operation_repeat: {repeat}")
    for _ in range(repeat):
        create_student()

print("\nCREATE")
measure_perf(create_operation)

# ===========================~~~===============================

def read_operation(repeat):
    """Measure SQLite read operation."""
    migrate()
    print(f"read_operation_repeat: {repeat}")
    for _ in range(repeat):
        create_student()

print("\nREAD")
measure_perf(read_operation)

# ===========================~~~===============================

def update_operation(repeat):
    """Measure SQLite read operation."""
    migrate()
    print(f"update_operation_repeat: {repeat}")
    for _ in range(repeat):
        update_student()

print("\nUPDATE")
measure_perf(update_operation)

# ===========================~~~===============================

# Commit DB changes
db.commit()

# Close DB connection
db.close()
