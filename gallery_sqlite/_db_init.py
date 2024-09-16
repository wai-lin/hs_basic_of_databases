"""Gallery DB Seeder"""

import time
import sqlite3
import random

from faker import Faker

# Database Init
conn = sqlite3.connect('gallery.db')
cursor = conn.cursor()

# Migrations
def migrate():
    """Migrate tables"""
    cursor.execute("DROP TABLE artists")
    cursor.execute("""
        CREATE TABLE artists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            birth_date TEXT,
            nationality TEXT,
            bio TEXT
        );
    """)

    cursor.execute("DROP TABLE exhibitions")
    cursor.execute("""
        CREATE TABLE exhibitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            start_date TEXT,
            end_date TEXT
        );
    """)

    cursor.execute("DROP TABLE artworks")
    cursor.execute("""
        CREATE TABLE artworks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            price REAL,               -- Using REAL for price to accommodate decimal values
            date_created TEXT,       -- Storing date_created as TEXT in 'YYYY-MM-DD' format
            medium TEXT,             -- Medium used in the artwork (e.g., oil painting, sculpture)
            artist_id INTEGER,       -- Foreign key to reference artists table
            exhibition_id INTEGER,   -- Foreign key to reference exhibitions table
            FOREIGN KEY (artist_id) REFERENCES artists(id),
            FOREIGN KEY (exhibition_id) REFERENCES exhibitions(id)
        );
    """)

    cursor.execute("DROP TABLE visitors")
    cursor.execute("""
        CREATE TABLE visitors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,   -- Email is unique and cannot be NULL
            date_visited TEXT            -- Storing date_visited as TEXT in 'YYYY-MM-DD' format
        );
    """)

    cursor.execute("DROP TABLE sales")
    cursor.execute("""
        CREATE TABLE sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artwork_id INTEGER,         -- Foreign key to reference artworks table
            visitor_id INTEGER,         -- Foreign key to reference visitors table
            sale_date TEXT,             -- Storing sale_date as TEXT in 'YYYY-MM-DD' format
            amount REAL,                -- Using REAL for amount to accommodate decimal values
            FOREIGN KEY (artwork_id) REFERENCES artworks(id),
            FOREIGN KEY (visitor_id) REFERENCES visitors(id)
        );
    """)

migrate()

# Seeding
gen_count = {
    "artists": 50,
    "exhibitions": 30,
    "artworks": 400,
    "visitors": 200,
    "sales": 100_000
}

# Initialize Faker
fake = Faker()

def generate_artists(num = gen_count.get("artists")):
    """Function to generate random artists data"""
    cursor.execute('DELETE FROM artists')
    artists = [
        (
            fake.name(),
            fake.date_of_birth(minimum_age=20, maximum_age=80).isoformat(),
            fake.country(),
            fake.text(max_nb_chars=200)  # A short biography
        )
        for _ in range(num)
    ]
    cursor.executemany("""
        INSERT INTO artists (name, birth_date, nationality, bio) VALUES (?, ?, ?, ?)
    """, artists)

def generate_exhibitions(num = gen_count.get("exhibitions")):
    """Function to generate random exhibitions data"""
    cursor.execute('DELETE FROM exhibitions')
    exhibitions = [
        (
            fake.word(),
            fake.date_this_decade(before_today=True, after_today=False).isoformat(), 
            fake.date_this_decade(before_today=False, after_today=True).isoformat()
        )
        for _ in range(num)
    ]
    cursor.executemany("""
        INSERT INTO exhibitions (name, start_date, end_date) VALUES (?, ?, ?)
    """, exhibitions)

def generate_artworks(num = gen_count.get("artworks")):
    """Function to generate random artworks data"""
    cursor.execute('DELETE FROM artworks')
    artist_ids = [i for i in range(1, gen_count.get("artists") + 1)]  # Assuming there are 10 artists
    exhibition_ids = [i for i in range(1, gen_count.get("exhibitions") + 1)]  # Assuming there are 10 exhibitions
    artworks = [
        (
            fake.sentence(nb_words=3),  # Title of the artwork
            round(random.uniform(100, 5000), 2),  # Price between 100 and 5000
            fake.date_this_century(before_today=True, after_today=False).isoformat(), 
            fake.word(),  # Medium used in the artwork
            random.choice(artist_ids), 
            random.choice(exhibition_ids)
        )
        for _ in range(num)
    ]
    cursor.executemany("""
        INSERT INTO artworks (title, price, date_created, medium, artist_id, exhibition_id) VALUES (?, ?, ?, ?, ?, ?)
    """, artworks)

def generate_visitors(num = gen_count.get("visitors")):
    """Function to generate random visitors data"""
    cursor.execute('DELETE FROM visitors')
    visitors = [
        (
            fake.name(),
            fake.safe_email(),
            fake.date_this_decade(before_today=True, after_today=False).isoformat()
        )
        for _ in range(num)
    ]
    cursor.executemany("""
        INSERT INTO visitors (name, email, date_visited) VALUES (?, ?, ?)
    """, visitors)

def generate_sales(num = gen_count.get("sales")):
    """Function to generate random sales data"""
    cursor.execute('DELETE FROM sales')
    artwork_ids = [i for i in range(1, gen_count.get("artworks") + 1)]  # Assuming there are 20 artworks
    visitor_ids = [i for i in range(1, gen_count.get("visitors") + 1)]  # Assuming there are 20 visitors
    sales = [
        (
            random.choice(artwork_ids),
            random.choice(visitor_ids),
            fake.date_this_year().isoformat(),
            round(random.uniform(100, 5000), 2)  # Amount between 100 and 5000
        )
        for _ in range(num)
    ]
    cursor.executemany("""
        INSERT INTO sales (artwork_id, visitor_id, sale_date, amount) VALUES (?, ?, ?, ?)
    """, sales)

# Generate data
generate_artists()
generate_exhibitions()
generate_artworks()
generate_visitors()
generate_sales()

# i_start = time.time()
# i_end = time.time()
# time_taken = round(i_end - i_start, 6)
# print(
#     f"INSERT operations take {time_taken} seconds."
# )

# Commit and close
conn.commit()
conn.close()
