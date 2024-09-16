"""Postgres Performance Testing"""

import time
import psycopg2

from faker import Faker

fake = Faker()

print("PostgreSQL\n\n")

# Configs

DB_HOST="localhost"
DB_PORT=5432
DB_USER="postgres"
DB_PASSWORD="root"
DB_NAME="postgres"

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    dbname=DB_NAME
)
cursor = conn.cursor()

# Migration if table NOT EXISTS
cursor.execute("""
    CREATE TABLE IF NOT EXISTS students (
        id SERIAL NOT NULL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        date_of_birth DATE,
        nationality VARCHAR(80)
    );
    TRUNCATE TABLE students RESTART IDENTITY CASCADE;
""")

### Insert Perf
#
RECORDS_COUNT = 100_000
i_start = time.time()
for _ in range(RECORDS_COUNT):
    student = (
        f"{fake.first_name()} {fake.last_name()}",
        fake.date_of_birth(minimum_age=15, maximum_age=28),
        fake.country()
    )
    cursor.execute("""
        INSERT INTO students (name, date_of_birth, nationality) VALUES (%s, %s, %s)
    """, student)
i_end = time.time()
print(
    f"INSERT operation of {RECORDS_COUNT} records => time taken: {round(i_end - i_start, 6)} seconds"
)

### Update Perf
#
RECORDS_COUNT = 100_000
u_start = time.time()
for i in range(RECORDS_COUNT):
    student = (
        fake.date_of_birth(minimum_age=15, maximum_age=28),
        i
    )
    cursor.execute("""
        UPDATE students SET date_of_birth = %s WHERE id = %s
    """, student)
u_end = time.time()
print(
    f"UPDATE operation of {RECORDS_COUNT} records => time taken: {round(u_end - u_start, 6)} seconds"
)

### Read Perf
#
RECORDS_COUNT = 100_000
r_start = time.time()
for i in range(RECORDS_COUNT):
    cursor.execute("""
        SELECT * FROM students WHERE id = %s
    """, [i])
r_end = time.time()
print(
    f"SELECT operation of {RECORDS_COUNT} records => time taken: {round(r_end - r_start, 6)} seconds"
)

# Before Exit
conn.commit()
cursor.close()
conn.close()
