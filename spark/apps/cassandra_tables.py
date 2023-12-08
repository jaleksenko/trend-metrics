from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


def create_cassandra_table():
    cluster = Cluster(['cassandra'])
    session = cluster.connect('keywords')

    # Define tables
    keywords_hour = """
	CREATE TABLE IF NOT EXISTS keywords.keywords_hour (
	    hour text,
	    keyword text,
	    count int,
	    PRIMARY KEY (hour, count, keyword)
	) WITH CLUSTERING ORDER BY (count DESC, keyword ASC);
    """

    keywords_day = """
    CREATE TABLE IF NOT EXISTS keywords.keywords_day (
	    day text,
	    keyword text,
	    count int,
	    PRIMARY KEY (day, count, keyword)
	) WITH CLUSTERING ORDER BY (count DESC, keyword ASC);
    """

    keywords_month = """
    CREATE TABLE IF NOT EXISTS keywords.keywords_month (
	    month text,
	    keyword text,
	    count int,
	    PRIMARY KEY (month, count, keyword)
	) WITH CLUSTERING ORDER BY (count DESC, keyword ASC);
    """

    # Create tables
    session.execute(SimpleStatement(keywords_hour))
    session.execute(SimpleStatement(keywords_day))
    session.execute(SimpleStatement(keywords_month))

    session.shutdown()
    cluster.shutdown()

