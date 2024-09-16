"""Select Performance between SQLite and Postgres"""

import time
import sqlite3
import psycopg2

# Configs
postgres_config = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "root",
    "dbname": "postgres"
}
sqlite_db = "gallery.db"

# Queries
queries = {
    "get_by_primary_key_pg": """
        SELECT * FROM sales WHERE id = %s
    """,
    "get_by_primary_key_sqlite": """
        SELECT * FROM sales WHERE id = ?
    """,

    "get_by_non_primary_field_pg": """
        SELECT * FROM sales WHERE amount = %s
    """,
    "get_by_non_primary_field_sqlite": """
        SELECT * FROM sales WHERE amount = ?
    """,

    "select_with_joins_pg": """
        SELECT s.id, s.sale_date, v.name, a.title
        FROM sales s
        JOIN visitors v ON s.visitor_id = v.id
        JOIN artworks a ON s.artwork_id = a.id
        WHERE s.amount > %s
    """,
    "select_with_joins_sqlite": """
        SELECT s.id, s.sale_date, v.name, a.title
        FROM sales s
        JOIN visitors v ON s.visitor_id = v.id
        JOIN artworks a ON s.artwork_id = a.id
        WHERE s.amount > ?
    """,

    "aggregate_function_pg": """
        SELECT
            exhibitions.id AS exhibition_id,
            exhibitions.name AS exhibition_name,
            artists.name AS artist_name,
            TO_CHAR(sales.sale_date, 'YYYY-MM') AS sale_month,
            MAX(sales.amount) AS highest_sale_amount
        FROM artworks
        INNER JOIN exhibitions ON artworks.exhibition_id = exhibitions.id
        INNER JOIN artists ON artworks.artist_id = artists.id
        INNER JOIN sales ON artworks.id = sales.artwork_id
        GROUP BY exhibitions.id, exhibitions.name, artists.name, TO_CHAR(sales.sale_date, 'YYYY-MM');
    """,
    "aggregate_function_sqlite": """
        SELECT
            exhibitions.id as exhibition_id,
            exhibitions.name as exhibition_name,
            artists.name as artist_name,
            sales.id as sale_id,
            strftime('%Y-%m', sales.sale_date) AS sale_month,
            MAX(sales.amount) as highest_sale_amount
        FROM artworks
        INNER JOIN exhibitions ON artworks.exhibition_id = exhibitions.id
        INNER JOIN artists ON artworks.artist_id = artists.id
        INNER JOIN sales ON artworks.id = sales.artwork_id
        GROUP BY sale_month
    """
}

# Helpers
def tabular(cursor, rows):
    """Print database cursor and rows result in tabular format."""

    # Get column names
    column_names = [description[0] for description in cursor.description]

    # Determine the width of each column
    # Start by using the length of the column names as the minimum width
    column_widths = [len(column) for column in column_names]

    # Adjust column widths based on the data in each column
    for row in rows:
        for i, value in enumerate(row):
            column_widths[i] = max(column_widths[i], len(str(value)))

    # Function to print a separator line
    def print_separator():
        print('+' + '+'.join('-' * (width + 2) for width in column_widths) + '+')

    # Function to print a row of data (either headers or data rows)
    def print_row(row):
        print('| ' + ' | '.join(f'{str(value).ljust(width)}' for value, width in zip(row, column_widths)) + ' |')

    # Print the formatted output
    print_separator()
    print_row(column_names)  # Print headers
    print_separator()
    for row in rows:
        print_row(row)  # Print each data row
    print_separator()

def measure_query_performance(conn, query, params=()):
    """Helper function to measure execution time"""
    cursor = conn.cursor()
    start_time = time.time()
    cursor.execute(query, params)
    results = cursor.fetchall()
    end_time = time.time()
    cursor.close()
    return end_time - start_time, results

# Test Functions
def test_postgres_queries():
    """PostgreSQL Performance"""
    print("\nPostgreSQL Performance:\n")
    conn = psycopg2.connect(**postgres_config)

    # Get row by primary key
    primary_key_query_time, _ = measure_query_performance(conn, queries["get_by_primary_key_pg"], (1,))
    print(f"Primary key query time: {primary_key_query_time:.6f} seconds")

    # Get rows by non-primary field value (using amount as example)
    non_primary_query_time, _ = measure_query_performance(conn, queries["get_by_non_primary_field_pg"], (500,))
    print(f"Non-primary field query time: {non_primary_query_time:.6f} seconds")

    # Select with joins
    join_query_time, _ = measure_query_performance(conn, queries["select_with_joins_pg"], (500,))
    print(f"Join query time: {join_query_time:.6f} seconds")

    # Aggregate function query
    aggregate_query_time, _ = measure_query_performance(conn, queries["aggregate_function_pg"])
    print(f"Aggregate query time: {aggregate_query_time:.6f} seconds")

    conn.close()

def test_sqlite_queries():
    """SQLite Performance"""
    print("\nSQLite Performance:\n")
    conn = sqlite3.connect(sqlite_db)

    # Get row by primary key
    primary_key_query_time, _ = measure_query_performance(conn, queries["get_by_primary_key_sqlite"], (1,))
    print(f"Primary key query time: {primary_key_query_time:.6f} seconds")

    # Get rows by non-primary field value (using amount as example)
    non_primary_query_time, _ = measure_query_performance(conn, queries["get_by_non_primary_field_sqlite"], (500,))
    print(f"Non-primary field query time: {non_primary_query_time:.6f} seconds")

    # Select with joins
    join_query_time, _ = measure_query_performance(conn, queries["select_with_joins_sqlite"], (500,))
    print(f"Join query time: {join_query_time:.6f} seconds")

    # Aggregate function query
    aggregate_query_time, _ = measure_query_performance(conn, queries["aggregate_function_sqlite"])
    print(f"Aggregate query time: {aggregate_query_time:.6f} seconds")

    conn.close()

# Run the test
test_postgres_queries()
print("\n")
test_sqlite_queries()
