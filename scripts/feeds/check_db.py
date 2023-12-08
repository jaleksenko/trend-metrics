import sqlite3


# Select feeds in the database
def check_feeds(conn):
    """
    Select all the entries in the feeds table.
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM feeds")
    rows = cur.fetchall()
    # for row in rows:
    #     print(row)

# Count feeds in the database
def count_feeds(conn):
    """
    Count the number of entries in the feeds table.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM feeds")
    total = cur.fetchone()[0]
    print(f'Total number of feeds: {total}')
    return total

# Connect to the database
db_path = 'sources.db'
conn = sqlite3.connect(db_path)


# Select all the data in the feeds database and count them
check_feeds(conn)
count_feeds(conn)

# Close the connection to the database
conn.close()