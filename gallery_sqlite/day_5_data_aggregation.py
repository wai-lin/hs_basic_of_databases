"""Data Aggregation"""

import sqlite3

DB_FILE = "gallery.db"

# Init db connection
db = sqlite3.connect(DB_FILE)

print("##############################~~~##############################")

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

def print_section(task_no: int, section_name: str, sql: str, cursor, result):
    """Print out the Task, SQL Query and Result in sections."""

    print(f"\nTask {task_no}:")
    print(section_name, "\n")
    print("Query:")
    print(sql, "\n")
    print("Result:\n")
    tabular(cursor, result)
    print("\n")
    print("##############################~~~##############################")

def exec_sql(task_no: int, section_name: str, sql: str):
    """Execute the given SQL Query and Print out in sections."""

    cursor = db.execute(sql)
    result = cursor.fetchall()
    print_section(task_no, section_name, sql, cursor, result)

exec_sql(
    1,
    """
        Get (artists name, sales amount)
        artists who don't have any sales or sales amount lower than 2000
    """,
    """
        SELECT
            artists.id as artist_id,
            artists.name as artist_name,
            SUM(sales.amount) as total_sale_amount
        FROM artworks
        INNER JOIN artists ON artworks.artist_id = artists.id
        LEFT JOIN sales ON artworks.id = sales.artwork_id
        GROUP BY artists.id
        HAVING total_sale_amount IS NULL
            OR total_sale_amount < 2000
    """
)

exec_sql(
    2,
    """
        Get (exhibitions name, artist name, sales amount)
        sales amount which is the highest sales in each a month
    """,
    """
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
)

exec_sql(
    3,
    """
        Get (visitor name, artwork title, artwork medium)
        artwork which sales price is lower than 1000
    """,
    """
        SELECT
            visitors.id as visitor_id,
            visitors.name as visitor_name,
            artworks.title as artwork_title,
            artworks.medium as artwork_medium,
            sales.id as sale_id,
            sales.amount as sale_amount
        FROM artworks
        INNER JOIN sales ON artworks.id = sales.artwork_id
        INNER JOIN visitors ON sales.visitor_id = visitors.id
        WHERE sales.amount < 1000
    """
)

# close db connection
db.close()
