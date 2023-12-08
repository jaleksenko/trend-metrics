import sqlite3

# Define the database file location
database = "sources.db"

# Create a database connection
conn = sqlite3.connect(database)
cursor = conn.cursor()

# Delete all rows in the "feeds" table
# cursor.execute("DELETE FROM feeds")

# Drop the "feeds" table if it exists
cursor.execute("DROP TABLE IF EXISTS feeds")

# Create the "feeds" table again
cursor.execute("""
    CREATE TABLE feeds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        url TEXT UNIQUE NOT NULL,
        last_updated INTEGER NOT NULL
    )
""")

# Commit the changes
conn.commit()

# Close the connection
conn.close()