from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from datetime import datetime, timedelta
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)

def aggregate_data():
    """
    Connects to Cassandra, performs daily and monthly data aggregation,
    and inserts aggregated data into 'keywords_day' and 'keywords_month' tables.
    """
    try:
        # Connect to Cassandra
        cluster = Cluster(
            contact_points=['cassandra'], 
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
            port=9042
        )
        session = cluster.connect('keywords')
        logging.info('Successfully connected to Cassandra.')

        # Prepare date strings for queries
        yesterday = datetime.now() - timedelta(days=1)
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        month_str = today.strftime('%Y-%m')
        yesterday_str = yesterday.strftime('%Y-%m-%d %H:%M:%S')
        today_str = today.strftime('%Y-%m-%d %H:%M:%S')

        # Clear existing data for today in the daily and monthly tables
        clear_table(session, 'keywords_day', today_str)
        clear_table(session, 'keywords_month', month_str)

        # Aggregate and save daily data from hourly data
        aggregate_daily_data(session, yesterday_str, today_str)

        # If it's the first day of the month, aggregate monthly data
        if datetime.now().day == 1:
            previous_month_str = (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
            aggregate_monthly_data(session, previous_month_str)
            logging.info('Monthly data aggregated.')

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        # Shutdown the session and cluster connection
        session.shutdown()
        cluster.shutdown()
        logging.info('Connection closed.')

def clear_table(session, table_name, date):
    """
    Clears data for a specific date or month from a specified table.
    """
    column_name = 'day' if table_name == 'keywords_day' else 'month'
    delete_query = f"DELETE FROM {table_name} WHERE {column_name} = %s"
    session.execute(delete_query, [date])
    logging.info(f'{table_name} table was successfully cleaned for the {column_name} {date}.')

def aggregate_daily_data(session, start_date_str, end_date_str):
    """
    Aggregates data from 'keywords_hour' for a given date range and inserts into 'keywords_day'.
    """
    # Fetch data from keywords_hour
    select_query = """
        SELECT keyword, count
        FROM keywords_hour
        WHERE hour >= %s AND hour < %s
        ALLOW FILTERING
    """
    rows = session.execute(select_query, [start_date_str, end_date_str])

    # Aggregate and sort data, limiting to top 20 keywords
    aggregated_data = {}
    for row in rows:
        if row.keyword in aggregated_data:
            aggregated_data[row.keyword] += row.count
        else:
            aggregated_data[row.keyword] = row.count

    top_keywords = sorted(aggregated_data.items(), key=lambda item: item[1], reverse=True)[:20]

    # Insert top 20 aggregated keywords into keywords_day
    for keyword, total_count in top_keywords:
        insert_query = """
            INSERT INTO keywords_day (day, keyword, count)
            VALUES (%s, %s, %s)
        """
        session.execute(insert_query, [end_date_str, keyword, total_count])
        logging.info(f'Daily data aggregated for keyword: {keyword}')

def aggregate_monthly_data(session, month_str):
    """
    Aggregates data from 'keywords_day' for the given month and inserts into 'keywords_month'.
    """
    # Fetch data from keywords_day
    select_query = """
        SELECT keyword, count
        FROM keywords_day
        WHERE day >= %s AND day < %s
        ALLOW FILTERING
    """
    next_month = datetime.strptime(month_str, "%Y-%m") + timedelta(days=32)
    next_month_str = next_month.strftime('%Y-%m')
    rows = session.execute(select_query, [month_str, next_month_str])

    # Aggregate and sort data, limiting to top 20 keywords for the month
    aggregated_data = {}
    for row in rows:
        if row.keyword in aggregated_data:
            aggregated_data[row.keyword] += row.count
        else:
            aggregated_data[row.keyword] = row.count

    top_keywords = sorted(aggregated_data.items(), key=lambda item: item[1], reverse=True)[:20]

    # Insert top 20 aggregated keywords into keywords_month
    for keyword, total_count in top_keywords:
        insert_query = """
            INSERT INTO keywords_month (month, keyword, count)
            VALUES (%s, %s, %s)
        """
        session.execute(insert_query, [month_str, keyword, total_count])
        logging.info(f'Monthly data aggregated for keyword: {keyword}')

if __name__ == "__main__":
    aggregate_data()

