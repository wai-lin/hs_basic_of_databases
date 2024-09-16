"""Triggers and Store Procedures"""

import random
import psycopg2

from faker import Faker


# Configs

DB_HOST="localhost"
DB_PORT=5432
DB_USER="postgres"
DB_PASSWORD="root"
DB_NAME="postgres"

# Initializations

fake = Faker()

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    dbname=DB_NAME
)

cursor = conn.cursor()

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

# Migration

cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales_log (
        id SERIAL PRIMARY KEY,
        sale_id INTEGER,
        action TEXT,
        log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

# Triggers

# cursor.execute("""
#     CREATE OR REPLACE FUNCTION log_sales_entry()
#     RETURNS TRIGGER AS $$
#     BEGIN
#         INSERT INTO sales_log (sale_id, action)
#         VALUES (NEW.id, 'Sale recorded');
#         RETURN NEW;
#     END;
#     $$ LANGUAGE plpgsql;

#     CREATE TRIGGER trg_log_sales_entry
#     AFTER INSERT ON sales
#     FOR EACH ROW
#     EXECUTE FUNCTION log_sales_entry();
# """)

# Trigger Test

cursor.execute("SELECT id FROM artworks")
artwork_ids = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT id FROM visitors")
visitor_ids = [row[0] for row in cursor.fetchall()]

sale = (
    random.choice(artwork_ids),
    random.choice(visitor_ids),
    fake.date_this_year(before_today=True, after_today=False).isoformat(),
    round(random.uniform(100, 5000), 2)
)
cursor.execute("""
    INSERT INTO sales (artwork_id, visitor_id, sale_date, amount)
    VALUES (%s, %s, %s, %s)
""", sale)

# Test Trigger

cursor.execute("SELECT * FROM sales_log")
sales_log = cursor.fetchall()
print(tabular(cursor, sales_log))

# Commiting

conn.commit()

# Closing

cursor.close()
conn.close()
