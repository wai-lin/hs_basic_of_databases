import sqlite3
from faker import Faker
import random

# Initialize Faker and SQLite connection
fake = Faker()
conn = sqlite3.connect("gallery.db")
cursor = conn.cursor()

# Helper function to generate random data
def generate_artists(num):
    artists = [(fake.name(), fake.year()) for _ in range(num)]
    cursor.executemany("INSERT INTO artists (name, birth_year) VALUES (?, ?)", artists)

def generate_exhibitions(num):
    exhibitions = [(fake.sentence(), fake.word()) for _ in range(num)]
    cursor.executemany("INSERT INTO exhibitions (name, gallery_name) VALUES (?, ?)", exhibitions)

def generate_artworks(num):
    artist_ids = [i for i in range(1, 11)]  # Assuming there are 10 artists
    exhibition_ids = [i for i in range(1, 11)]  # Assuming there are 10 exhibitions
    artworks = [
        (fake.sentence(), fake.year(), fake.random_number(digits=3), random.choice(artist_ids), random.choice(exhibition_ids))
        for _ in range(num)
    ]
    cursor.executemany("INSERT INTO artworks (title, year_created, price, artist_id, exhibition_id) VALUES (?, ?, ?, ?, ?)", artworks)

def generate_sales(num):
    artwork_ids = [i for i in range(1, 21)]  # Assuming there are 20 artworks
    visitor_ids = [i for i in range(1, 21)]  # Assuming there are 20 visitors
    sales = [
        (random.choice(artwork_ids), random.choice(visitor_ids), fake.year(), fake.random_number(digits=3))
        for _ in range(num)
    ]
    cursor.executemany("INSERT INTO sales (artwork_id, visitor_id, sale_year, amount) VALUES (?, ?, ?, ?)", sales)

def generate_visitors(num):
    visitors = [(fake.name(), fake.year()) for _ in range(num)]
    cursor.executemany("INSERT INTO visitors (name, year_visited) VALUES (?, ?)", visitors)

# Generate data
generate_artists(10)
generate_exhibitions(10)
generate_artworks(20)
generate_sales(30)
generate_visitors(20)

# Commit and close
conn.commit()
conn.close()

