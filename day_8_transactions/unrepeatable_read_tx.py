"""
UNREPEATABLE READ TRANSACTION
A customer is viewing an artwork, but before they complete the purchase, the price changes.
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

def unrepeatable_read_transaction_1():
    """TX 1"""

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    conn.autocommit = False

    # Step 1: Begin transaction and read the artwork price
    print("Transaction 1: BEGIN")
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("SELECT price FROM artworks WHERE id = 1;")
    price = cursor.fetchone()[0]
    print(f"Transaction 1: Initial Price = {price}")

    # Pause to allow Transaction 2 to update
    time.sleep(5)

    # Step 3: Read the price again
    cursor.execute("SELECT price FROM artworks WHERE id = 1;")
    new_price = cursor.fetchone()[0]
    print(f"Transaction 1: New Price after Transaction 2 update = {new_price}")

    conn.commit()
    print("Transaction 1: COMMIT")
    cursor.close()
    conn.close()

def unrepeatable_read_transaction_2():
    """TX 2"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    conn.autocommit = False

    time.sleep(2)  # To ensure Transaction 1 reads first

    # Step 2: Begin transaction and update the artwork price
    print("Transaction 2: BEGIN")
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("UPDATE artworks SET price = price + 100 WHERE id = 1;")
    conn.commit()
    print("Transaction 2: Price updated and COMMIT")

    cursor.close()
    conn.close()

# ==============================~~~~~==============================

def run_unrepeatable_read_test():
    """Runner"""

    t1 = threading.Thread(target=unrepeatable_read_transaction_1)
    t2 = threading.Thread(target=unrepeatable_read_transaction_2)

    # Start both threads
    t1.start()
    t2.start()

    # Wait for both threads to complete
    t1.join()
    t2.join()

    print("Unrepeatable Read Test Completed")

run_unrepeatable_read_test()
