"""Relational Modeling and JOINs"""

import sqlite3

DB_FILE = "gallery.db"

db = sqlite3.connect(DB_FILE)

def print_section(section_name: str, sql: str, result):
    """Print the section in divisions."""

    print("\n##", section_name, "\n")
    print(sql, "\n")
    print(result, "\n")
    print("##############################~~~##############################")

def exec_sql(section_name: str, sql: str):
    """Execute the SQL Query and print the result in sections."""

    cursor = db.execute(sql)
    result = cursor.fetchall()
    print_section(section_name, sql, result)

print("##############################~~~##############################")

exec_sql(
    "Query 1",
    """
        SELECT artists.name, artworks.title, artworks.price
        FROM artworks
        JOIN artists ON artists.id = artworks.artist_id
        WHERE artworks.price > 500
        LIMIT 10
    """
)

exec_sql(
    "Query 2",
    """
        SELECT artists.name, artworks.title, sales.amount
        FROM artworks
        JOIN artists ON artists.id = artworks.artist_id
        JOIN sales ON sales.artwork_id = artworks.id
        LIMIT 10
    """
)
