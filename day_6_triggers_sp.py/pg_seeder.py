"""Postgres Gallery DB Seeder"""

import random
import psycopg2
from faker import Faker

# Configs

DB_HOST="localhost"
DB_PORT=5432
DB_USER="postgres"
DB_PASSWORD="root"
DB_NAME="postgres"

gen_count = {
    "artists": 50,
    "exhibitions": 30,
    "artworks": 400,
    "visitors": 200,
    "sales": 100_000
}

# Helpers

def truncate_table_if_exists(table):
    """Truncate the table if it exists."""
    try:
        cursor.execute(f"""
            DO $$
            BEGIN
                IF EXISTS (
                       SELECT 1 FROM information_schema.tables WHERE table_name = '{table}'
                ) THEN
                    TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;
                END IF;
            END
            $$;
        """)
    except psycopg2.Error as e:
        print(f"Error truncating table {table}: {e}")

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

# Define Generations

def generate_artists(count = gen_count.get("artists")):
    """Generate Artists"""
    truncate_table_if_exists("artists")
    artists = [
        (
            fake.name(),
            fake.date_of_birth(minimum_age=20, maximum_age=80).isoformat(),
            fake.country(),
            fake.text(max_nb_chars=200)
        )
        for _ in range(count)
    ]
    cursor.executemany("""
        INSERT INTO artists (name, birth_date, nationality, bio)
        VALUES (%s, %s, %s, %s)
    """, artists)

def generate_exhibitions(count = gen_count.get("exhibitions")):
    """Generate Exhibitions"""
    truncate_table_if_exists("exhibitions")
    exhibitions = [
        (
            fake.word(),
            fake.date_this_decade(before_today=True, after_today=False).isoformat(),
            fake.date_this_decade(before_today=False, after_today=True).isoformat()
        )
        for _ in range(count)
    ]
    cursor.executemany("""
        INSERT INTO exhibitions (name, start_date, end_date) 
        VALUES (%s, %s, %s)
    """, exhibitions)

def generate_artworks(num = gen_count.get("artworks")):
    """Generate Artworks"""
    truncate_table_if_exists("artworks")

    cursor.execute("SELECT id FROM artists")
    artist_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT id FROM exhibitions")
    exhibition_ids = [row[0] for row in cursor.fetchall()]

    artworks = [
        (
            fake.sentence(nb_words=4),
            round(random.uniform(100, 5000), 2),
            fake.date_this_century(before_today=True, after_today=False).isoformat(),
            fake.word(),
            random.choice(artist_ids),
            random.choice(exhibition_ids)
        )
        for _ in range(num)
    ]
    cursor.executemany("""
        INSERT INTO artworks (title, price, date_created, medium, artist_id, exhibition_id) 
        VALUES (%s, %s, %s, %s, %s, %s)
    """, artworks)

def generate_visitors(count = gen_count.get("visitors")):
    """Generate Visitors"""
    truncate_table_if_exists("visitors")

    visitors = [
        (
            fake.name(),
            fake.email(),
            fake.date_this_year(before_today=True, after_today=False).isoformat()
        )
        for _ in range(count)
    ]
    cursor.executemany("""
        INSERT INTO visitors (name, email, date_visited)
        VALUES (%s, %s, %s)
    """, visitors)

def generate_sales(count = gen_count.get("sales")):
    """Generate Sales"""
    truncate_table_if_exists("sales")

    cursor.execute("SELECT id FROM artworks")
    artwork_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT id FROM visitors")
    visitor_ids = [row[0] for row in cursor.fetchall()]

    sales = [
        (
            random.choice(artwork_ids),
            random.choice(visitor_ids),
            fake.date_this_year(before_today=True, after_today=False).isoformat(),
            round(random.uniform(100, 5000), 2)
        )
        for _ in range(count)
    ]
    cursor.executemany("""
        INSERT INTO sales (artwork_id, visitor_id, sale_date, amount)
        VALUES (%s, %s, %s, %s)
    """, sales)


# Generation

generate_artists()
generate_exhibitions()
generate_artworks()
generate_visitors()
generate_sales()

# Commiting

conn.commit()

# Closing

cursor.close()
conn.close()
