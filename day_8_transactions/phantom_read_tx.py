"""
PHANTOM READ TRANSACTION
A user is selecting artworks that are priced below a certain amount.
During the transaction, new artworks are inserted that meet the condition.
"""

import time
import threading
import psycopg2

DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'root',
    'host': 'localhost',
    'port': '5432'
}

# ==============================~~~~~==============================

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

# ==============================~~~~~==============================

def phantom_read_transaction_1():
    """TX 1"""

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    conn.autocommit = False

    # Step 1: Begin transaction and select artworks priced under 500
    print("Transaction 1: BEGIN")
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("SELECT id, title, price FROM artworks WHERE price < 500;")
    rows = cursor.fetchall()
    print("Transaction 1: Initial Rows")
    tabular(cursor, rows)

    # Pause to allow Transaction 2 to insert a new row
    time.sleep(5)

    # Step 3: Select again
    cursor.execute("SELECT id, title, price FROM artworks WHERE price < 500;")
    new_rows = cursor.fetchall()
    print("Transaction 1: Rows after Transaction 2 insert")
    tabular(cursor, new_rows)

    conn.commit()
    print("Transaction 1: COMMIT")
    cursor.close()
    conn.close()

def phantom_read_transaction_2():
    """TX 2"""

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    conn.autocommit = False

    time.sleep(2)  # To ensure Transaction 1 selects first

    # Step 2: Begin transaction and insert a new artwork
    print("Transaction 2: BEGIN")
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("""
        INSERT INTO artworks (title, price, date_created, medium, artist_id, exhibition_id)
        VALUES ('New Artwork', 400, CURRENT_DATE, 'oil', 1, 1);
    """)
    conn.commit()
    print("Transaction 2: New artwork inserted and COMMIT")

    cursor.close()
    conn.close()

# ==============================~~~~~==============================

def run_phantom_read_test():
    """Runner"""

    t1 = threading.Thread(target=phantom_read_transaction_1)
    t2 = threading.Thread(target=phantom_read_transaction_2)

    # Start both threads
    t1.start()
    t2.start()

    # Wait for both threads to complete
    t1.join()
    t2.join()

    print("Phantom Read Test Completed")

run_phantom_read_test()
