import feedparser
from datetime import datetime
from database import create_connection, create_table  # Importing functions from database.py
from confluent_kafka import Producer
from metrics import current_value
from kafka_count import count_messages
import json
import uuid
 

# Helper functions
def to_iso8601(pub_time):
    # ISO 8601 format adjustment
    pub_time = pub_time.replace('Z', '+00:00')  # Replace 'Z' with '+00:00' to denote UTC
    try:
        # Try to parse the time in ISO 8601 format
        pub_date = datetime.fromisoformat(pub_time)
    except ValueError:
        # Fallback for other formats
        pub_time = pub_time.replace('GMT', '+0000')
        pub_date = datetime.strptime(pub_time, '%a, %d %b %Y %H:%M:%S %z')
    return pub_date.replace(tzinfo=None).isoformat() + 'Z'  # Return ISO 8601 format

 
def to_timestamp(pub_time):
    # ISO 8601 format adjustment
    pub_time = pub_time.replace('Z', '+00:00')  # Replace 'Z' with '+00:00' to denote UTC
    try:
        # Try to parse the time in ISO 8601 format
        pub_date = datetime.fromisoformat(pub_time)
    except ValueError:
        # Fallback for other formats
        pub_time = pub_time.replace('GMT', '+0000')
        pub_date = datetime.strptime(pub_time, '%a, %d %b %Y %H:%M:%S %z')
    return int(pub_date.timestamp() * 1000)


# SQL query to create a new table 
sql_create_feeds_table = """ CREATE TABLE IF NOT EXISTS feeds (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                title TEXT NOT NULL,
                                url TEXT NOT NULL,
                                last_updated INTEGER
                            ); """

# Define the database file location
database = '/opt/airflow/feeds/sources.db'


# Create a database connection and table
conn = create_connection(database)
if conn is not None:
    create_table(conn, sql_create_feeds_table)
else:
    print("Error! Cannot establish a connection to the database.")

# Function to retrieve and process all feeds
# Kafka producer
kafka_producer = Producer({'bootstrap.servers': 'broker:29092'})


# Function to send in Kafka
def send_to_kafka(feeds_data):
    try:
        kafka_producer.produce('feeds', value=json.dumps(feeds_data))
        kafka_producer.flush(30)
        return True
    except Exception as e:
        print(f"Failed to send to Kafka: {str(e)}")
        return False


# Process all channels
def process_all_channels(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id, title, url, last_updated FROM feeds")
    channels = cursor.fetchall()
    # print('Channels', channels)

    # Counter for new messages sent to Kafka
    new_messages_count = 0  

    for id, title, url, last_updated in channels:
        # print(id, title, url, last_updated)
        feed = feedparser.parse(url)

        new_entries = []
        max_last_updated = last_updated

        for entry in feed.entries:
            if hasattr(entry, 'published'):
                entry_published_ts = to_timestamp(entry.published)

                if entry_published_ts > (last_updated or 0):
                    max_last_updated = max(max_last_updated, entry_published_ts)
                    entry_id = str(uuid.uuid4())
                    new_entries.append({
                        'id': entry_id,
                        'title': entry.title,
                        'published': to_iso8601(entry.published),
                        'ChannelTitle': title
                    })
                    
        if new_entries:
            # Send to Kafka und update
            for new_entry in new_entries:
                send_to_kafka(new_entry)
                # print(f"New entry send: {new_entry}")
                new_messages_count += 1
                
            print(f"Updating last_updated for channel {id} to {max_last_updated}")
            cursor.execute("UPDATE feeds SET last_updated = ? WHERE id = ?", (max_last_updated, id))

    # Update the count of new messages sent to Kafka after all messages have been sent
    print(f'New {new_messages_count} messages sent to Kafka')
    
    # Send the value to Prometheus
    current_value('kafka_messages_new', new_messages_count)

    # Commit the changes and close the connection
    conn.commit()


# print('Conn:', conn)
# Process all feeds
try:
    process_all_channels(conn)
    # Count Kafka topic messages
    kafka_messages_total = count_messages('feeds')

    # Send the value to Prometheus
    current_value('kafka_messages_total', kafka_messages_total)
except Exception as e:
    print(f"An error occurred during feed processing: {str(e)}")
finally:
    conn.close()

 