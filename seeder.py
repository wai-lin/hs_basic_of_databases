"""Connecting with SQLite DB"""

import sqlite3
# import time

# use Faker to generate data
from faker import Faker

fake = Faker()

conn = sqlite3.connect("class_material.db")

def truncate_table(table_name: str):
    """Truncate the given table."""
    clear_table = f"DELETE FROM {table_name}"
    conn.execute(clear_table)

# seeding
def seed(fresh_seed = True):
    """
        Seed the whole database.
        If fresh_seed is True, the seeding will delete existing data and re-seed again.
    """
    # majors
    if fresh_seed:
        truncate_table("majors")
    gen_count = 10
    for _i in range(gen_count):
        name = fake.name()
        years = fake.random_int(1, 3)
        cred_per_class = 90
        sql_str = f"INSERT INTO students (name, years, cred_per_class) VALUES ({name}, {years}, {cred_per_class})"
        conn.execute(sql_str)

    # teachers
    if fresh_seed:
        truncate_table("teachers")
    gen_count = 50
    for _i in range(gen_count):
        name = fake.name()
        experience = fake.random_int(10, 50)
        sql_str = f"INSERT INTO teachers (name, experience) VALUES ({name}, {experience})"
        conn.execute(sql_str)

    # subjects
    if fresh_seed:
        truncate_table("subjects")
    cursor = conn.execute("SELECT * FROM majors")
    majors = cursor.fetchall()
    gen_count = 100
    for _i in range(gen_count):
        name = fake.mountain_name()
        major_id = fake.random_int(1, len(majors))
        sql_str = f"INSERT INTO subjects (name, major_id) VALUES ({name}, {major_id})"
        conn.execute(sql_str)


    # classes
    if fresh_seed:
        truncate_table("classes")
    s_cursor = conn.execute("SELECT * FROM subjects")
    subjects = s_cursor.fetchall()
    t_cursor = conn.execute("SELECT * FROM teachers")
    teachers = t_cursor.fetchall()
    gen_count = 200
    for _i in range(gen_count):
        year = fake.random_int(2010, 2024)
        subject_id = fake.random_int(1, len(subjects))
        teacher_id = fake.random_int(1, len(teachers))
        sql_str = f"INSERT INTO classes (year, subject_id, teacher_id) VALUES ({year}, {subject_id}, {teacher_id})"

    students_count = 1_000_000

    # student ids
    if fresh_seed:
        truncate_table("student_ids")
    for _i in range(students_count):
        number = fake.identity_card_number()
        sql_str = f"INSERT INTO student_ids (number) VALUES ({number})"
        conn.execute(sql_str)

    # students
    if fresh_seed:
        truncate_table("students")
    m_cursor = conn.execute("SELECT * FROM majors")
    majors = m_cursor.fetchall()
    i_cursor = conn.execute("SELECT * FROM student_ids")
    student_ids = i_cursor.fetchall()
    for _i in range(students_count):
        name = fake.name()
        entrance_year = fake.random_int(2010, 2024)
        major_id = fake.random_int(1, len(majors))
        student_id_id = fake.random_int(1, len(student_ids))
        sql_str = f"INSERT INTO students (name, entrance_year, major_id, student_id_id) VALUES ({name}, {entrance_year}, {major_id}, {student_id_id})"
        conn.execute(sql_str)

    # enrollments
    if fresh_seed:
        truncate_table("enrollments")
    c_cursor = conn.execute("SELECT * FROM classes")
    classes = c_cursor.fetchall()
    s_cursor = conn.execute("SELECT * FROM students")
    students = s_cursor.fetchall()
    for _i in range(students_count):
        class_id = fake.random_int(1, len(classes))
        student_id = fake.random_int(1, len(students))
        sql_str = f"INSERT INTO enrollments (class_id, student_id) VALUES ({class_id}, {student_id})"

seed()

# close the connection
conn.close()
