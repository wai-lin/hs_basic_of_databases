"""JOIN Tables"""

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

db = sqlite3.connect(DB_FILE)



# commit changes
db.commit()

# close db
db.close()

