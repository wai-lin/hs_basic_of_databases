import sqlite3

DB_FILE = "class_material.db"

def print_section(section_name: str, sql: str, result):
    print("\n##", section_name, "\n")
    print(sql, "\n")
    print(result, "\n")
    print("##############################~~~##############################")

def exec_sql(section_name: str, sql: str):
    cursor = db.execute(sql)
    result = cursor.fetchall()
    print_section(section_name, sql, result)

# NOTE: added LIMIT 10 so that result other results are print out nicely

db = sqlite3.connect(DB_FILE)

print("##############################~~~##############################")

# 1. Get all information (from students table) about students with the name Lisa Lyons
exec_sql(
        "1. Get all information (from students table) about students with the name Lisa Lyons",
        'SELECT * FROM students WHERE name = "Lisa Lyons"',
    )

# 2. Get all information (from students table) about students with the entrance year after 2020
exec_sql(
        "2. Get all information (from students table) about students with the entrance year after 2020",
        "SELECT * FROM students WHERE entrance_year = 2020 LIMIT 10",
    )

# 3. Get names of students from the CS major who entered before 2020
exec_sql(
        "3. Get names of students from the CS major who entered before 2020",
        'SELECT s.name FROM students s JOIN majors m ON s.major_id = m.id WHERE m.name = "CS" AND s.entrance_year < 2020 LIMIT 10'
    )

# 4. Get names and entrance years of all students who entered between 2017 and 2019 (inclusive)
exec_sql(
        "4. Get names and entrance years of all students who entered between 2017 and 2019 (inclusive)",
        "SELECT name, entrance_year FROM students WHERE entrance_year BETWEEN 2017 AND 2019 LIMIT 10"
    )

# 5. Get names and entrance years of all students who entered in a leap year (year number is divisible by four)
exec_sql(
        "5. Get names and entrance years of all students who entered in a leap year (year number is divisible by four)",
        "SELECT name, entrance_year FROM students WHERE (entrance_year%4) = 0 LIMIT 10"
    )

# 6. Select names of teachers with 3 or more years of experience
exec_sql(
        "6. Select names of teachers with 3 or more years of experience",
        "SELECT name FROM teachers WHERE experience_years >= 3 LIMIT 10"
    )

# 7. Select names of student and names of their majors for students entered in 2020
exec_sql(
        "7. Select names of student and names of their majors for students entered in 2020",
        "SELECT s.name, m.name FROM students s JOIN majors m WHERE s.entrance_year = 2020 LIMIT 10"
    )

# 8. Select names of student and names of their majors where a student first name is Thomas
exec_sql(
        "8. Select names of student and names of their majors where a student first name is Thomas",
        'SELECT s.name, m.name FROM students s JOIN majors m WHERE s.name like "Thomas%" LIMIT 10'
    )

# 9. Select name and entrance year of student where the full name is shorter than 10 characters (e.g. John Doe).
exec_sql(
        "9. Select name and entrance year of student where the full name is shorter than 10 characters (e.g. John Doe).",
        "SELECT name, entrance_year FROM students WHERE LENGTH(name) < 10 LIMIT 10"
    )

# commit db changes
db.commit()

# exit
db.close()

