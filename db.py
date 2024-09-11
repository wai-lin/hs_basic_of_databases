"""Module ok"""

import os

DB_FILE = ""
CONTENT = ""

def connect():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as file:
            return file
    return None

# timeit

def __read_from_file(file_path: str):
    with open(file_path, "r", encoding="utf-8") as file:
        content = file.read()

def __save_to_file(file_path: str, content: str):
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(content)

def insert():
    return ""

def select():
    return ""

def update():
    return ""

def delete():
    return ""
