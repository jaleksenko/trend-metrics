from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

def create_cassandra_table():
    cluster = Cluster(['cassandra'])
    session = cluster.connect('keywords')

    # Define tables
    keywords_hour = """
    CREATE TABLE IF NOT EXISTS keywords_hour (
        date text,
        hour int,
        keyword text,
        count int,
        PRIMARY KEY ((date, hour), keyword)
    ) WITH CLUSTERING ORDER BY (keyword ASC) AND default_time_to_live = 86400;
    """

    keywords_day = """
    CREATE TABLE IF NOT EXISTS keywords_day (
        date text,
        keyword text,
        count int,
        PRIMARY KEY (date, keyword)
    ) WITH CLUSTERING ORDER BY (keyword ASC) AND default_time_to_live = 86400;
    """

    # Create tables
    session.execute(SimpleStatement(keywords_hour))
    session.execute(SimpleStatement(keywords_day))

    session.shutdown()
    cluster.shutdown()

