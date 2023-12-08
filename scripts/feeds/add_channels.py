import csv
import sqlite3
import time
from metrics import current_value

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        print("Table created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")

def insert_feeds_from_csv(conn, csv_file_path):
    inserted_rows = 0
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
        feed_reader = csv.DictReader(csvfile)
        for row in feed_reader:
            if row:
                feed_title = row['title']
                feed_url = row['url']
                last_updated = int(row['last_updated']) if row['last_updated'] else 0
                added = add_feed_if_not_exists(conn, feed_title, feed_url, last_updated)
                if added:
                    inserted_rows += 1
    return inserted_rows

def add_feed_if_not_exists(conn, feed_title, feed_url, last_updated=0):
    sql = '''
    INSERT INTO feeds(title, url, last_updated)
    SELECT ?, ?, ?
    WHERE NOT EXISTS(SELECT 1 FROM feeds WHERE url = ?)
    '''
    cur = conn.cursor()
    cur.execute(sql, (feed_title, feed_url, last_updated, feed_url))
    conn.commit()
    return cur.rowcount


def count_feeds(conn):
    """
    Count the number of entries in the feeds table.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM feeds")
    total = cur.fetchone()[0]
    # print(f'Total number of feeds: {total}')
    return total
    
    
def run():
    db_path = '/opt/airflow/feeds/sources.db'
    csv_file_path = '/opt/airflow/feeds/feeds.csv'

    # SQL query to create a new table
    sql_create_feeds_table = ''' CREATE TABLE IF NOT EXISTS feeds (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    title TEXT NOT NULL,
                                    url TEXT NOT NULL,
                                    last_updated INTEGER
                                ); '''

    conn = sqlite3.connect(db_path)
    create_table(conn, sql_create_feeds_table)
    
    # Start script execution time
    # start_time = time.time()  

    inserted_rows = insert_feeds_from_csv(conn, csv_file_path)
    print(f"Inserted {inserted_rows} rows.")

    current_value('channels_new', inserted_rows)        # Send inserted rows to metrics
    channels_total = count_feeds(conn)
    current_value('channels_total', channels_total)     # Send inserted rows to metrics
    conn.close()
    

if __name__ == "__main__":
    run()

