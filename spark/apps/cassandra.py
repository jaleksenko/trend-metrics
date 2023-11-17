from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

def create_cassandra_table():
    cluster = Cluster(['cassandra_host'])  # Замените на ваш хост Cassandra
    session = cluster.connect('your_keyspace')  # Замените на ваше ключевое пространство

    # Define tables
    tokens_hour = """
    CREATE TABLE IF NOT EXISTS tokens_hour (
        date text,
        hour int,
        token text,
        count int,
        PRIMARY KEY ((date, hour), token)
    ) WITH CLUSTERING ORDER BY (token ASC) AND default_time_to_live = 86400;
    """

    tokens_day = """
    CREATE TABLE IF NOT EXISTS tokens_day (
        date text,
        token text,
        count int,
        PRIMARY KEY (date, token)
    ) WITH CLUSTERING ORDER BY (token ASC) AND default_time_to_live = 86400;
    """

    # Create tables
    session.execute(SimpleStatement(tokens_hour))
    session.execute(SimpleStatement(tokens_day))

    session.shutdown()
    cluster.shutdown()

