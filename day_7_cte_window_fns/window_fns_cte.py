"""Window Functions & CTE"""

import psycopg2


# Configs

DB_HOST="localhost"
DB_PORT=5432
DB_USER="postgres"
DB_PASSWORD="root"
DB_NAME="postgres"

# Iniatialize

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    dbname=DB_NAME
)

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

def execute_query(desc: str, query: str):
    """Execute the SQL query and print the result in tabular format"""
    cursor.execute(query)
    rows = cursor.fetchall()
    print(f"\n{desc}\n")
    print(f"{query}\n")
    tabular(cursor, rows)
    print("\n==============================~~~==============================\n")

cursor = conn.cursor()

conn.commit()

execute_query(
    desc="Rank visitors by their spendings",
    query="""
        WITH sales_of_visitors AS (
            SELECT
                visitors.id AS visitor_id,
                visitors.name AS visitor_name,
                sales.sale_date AS sale_date,
                sales.amount AS sale_amount
            FROM sales
            JOIN visitors ON sales.visitor_id = visitors.id
        )
        SELECT * FROM (
            SELECT
                visitor_id,
                visitor_name,
                sale_date,
                sale_amount,
                RANK() OVER (PARTITION BY visitor_id ORDER BY sale_amount DESC) AS sale_amount_rank
            FROM sales_of_visitors
        ) ranked_sales
        WHERE sale_amount_rank <= 3 LIMIT 10;
    """
)

execute_query(
    desc="Sales above average for unique visitors",
    query="""
        WITH visitor_avg AS (
            SELECT
                visitor_id,
                visitors.name AS visitor_name,
                AVG(amount) OVER (PARTITION BY visitor_id) AS avg_sale_amount
            FROM sales
            JOIN visitors ON sales.visitor_id = visitors.id
        )
        SELECT DISTINCT ON (sales.visitor_id)
            sales.id,
            sales.visitor_id,
            visitor_avg.visitor_name,
            visitor_avg.avg_sale_amount,
            sales.amount as sale_amount
        FROM sales
        JOIN visitor_avg ON visitor_avg.visitor_id = sales.visitor_id
        WHERE sales.amount > visitor_avg.avg_sale_amount
        ORDER BY sales.visitor_id, sales.amount
        LIMIT 10
    """
)

execute_query(
    desc="Avegrage Sale amount per Artwork",
    query="""
        WITH avg_sales AS (
            SELECT
                artwork_id,
                AVG(amount) AS avg_amount
            FROM sales
            GROUP BY artwork_id
        )
        SELECT
            artwork_id,
            avg_amount
        FROM avg_sales
        ORDER BY avg_amount DESC LIMIT 10;
    """
)

cursor.close()
conn.close()
