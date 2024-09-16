"""SQLite Performance Testing"""

import time
import sqlite3

from faker import Faker

fake = Faker()

print("SQLite\n\n")

# Configs
conn = sqlite3.connect("sqlite_perf.db")
cursor = conn.cursor()

# Migration if table NOT EXISTS
cursor.execute("""
    CREATE TABLE IF NOT EXISTS students (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        date_of_birth TEXT,
        nationality TEXT
    )
""")
cursor.execute("DELETE FROM students")
cursor.execute('DELETE FROM sqlite_sequence WHERE name = "students"')

### Insert Perf
#
RECORDS_COUNT = 100_000
i_start = time.time()
for _ in range(RECORDS_COUNT):
    student = (
        f"{fake.first_name()} {fake.last_name()}",
        fake.date_of_birth(minimum_age=15, maximum_age=28).isoformat(),
        fake.country()
    )
    cursor.execute("""
        INSERT INTO students (name, date_of_birth, nationality) VALUES (?, ?, ?)
    """, student)
i_end = time.time()
time_taken = round(i_end - i_start, 6)
print(
    f"INSERT operation of {RECORDS_COUNT} records => time taken: {time_taken} seconds"
)

### Update Perf
#
RECORDS_COUNT = 100_000
u_start = time.time()
for i in range(RECORDS_COUNT):
    student = (
        fake.date_of_birth(minimum_age=15, maximum_age=28).isoformat(),
        i
    )
    cursor.execute("""
        UPDATE students SET date_of_birth = ? WHERE id = ?
    """, student)
u_end = time.time()
time_taken = round(u_end - u_start, 6)
print(
    f"UPDATE operation of {RECORDS_COUNT} records => time taken: {time_taken} seconds"
)

### Read Perf
#
RECORDS_COUNT = 100_000
r_start = time.time()
for i in range(RECORDS_COUNT):
    cursor.execute("""
        SELECT * FROM students WHERE id = ?
    """, [i])
r_end = time.time()
time_taken = round(r_end - r_start, 6)
print(
    f"SELECT operation of {RECORDS_COUNT} records => time taken: {time_taken} seconds"
)

# Before Exit
conn.commit()
cursor.close()
conn.close()
