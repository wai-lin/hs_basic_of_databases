"""
DEADLOCK TRANSACTION
Two users try to update stock levels on different artworks,
but in reverse order, causing a deadlock.
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

def deadlock_transaction_1():
    """TX 1"""

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        conn.autocommit = False

        # Set isolation level to SERIALIZABLE
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)

        # Step 1: Update stock of Artwork 1
        print("Transaction 1: BEGIN")
        cursor.execute("BEGIN TRANSACTION;")
        cursor.execute("UPDATE artworks SET stock = stock - 1 WHERE id = 1;")
        print("Transaction 1: Artwork 1 updated")

        # Pause to simulate delay
        time.sleep(5)

        # Step 3: Try to update Artwork 2
        cursor.execute("UPDATE artworks SET stock = stock - 1 WHERE id = 2;")
        print("Transaction 1: Attempting to update Artwork 2")

        conn.commit()
        cursor.close()
        conn.close()
    except psycopg2.errors.DeadlockDetected as _:
        print("Deadlock detected in transaction 1, rolling back")
        conn.rollback()
    except Exception as e:
        print(f"An error occurred {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def deadlock_transaction_2():
    """TX 2"""

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        conn.autocommit = False

        # Set isolation level to SERIALIZABLE
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)

        time.sleep(2)  # To ensure Transaction 1 locks Artwork 1 first

        # Step 2: Update stock of Artwork 2
        print("Transaction 2: BEGIN")
        cursor.execute("BEGIN TRANSACTION;")
        cursor.execute("UPDATE artworks SET stock = stock - 1 WHERE id = 2;")
        print("Transaction 2: Artwork 2 updated")

        # Step 4: Try to update Artwork 1 (this will cause a deadlock)
        cursor.execute("UPDATE artworks SET stock = stock - 1 WHERE id = 1;")
        print("Transaction 2: Attempting to update Artwork 1")

        conn.commit()
        cursor.close()
        conn.close()
    except psycopg2.errors.DeadlockDetected as _:
        print("Deadlock detected in transaction 2, rolling back")
        conn.rollback()
    except Exception as e:
        print(f"An error occurred {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# ==============================~~~~~==============================

def run_deadlock_test():
    """Runner"""

    t1 = threading.Thread(target=deadlock_transaction_1)
    t2 = threading.Thread(target=deadlock_transaction_2)

    # Start both threads
    t1.start()
    t2.start()

    # Wait for both threads to complete
    t1.join()
    t2.join()

    print("Deadlock Test Completed")

run_deadlock_test()
