import statsd
import time

# Create a StatsD client object to send metrics to statsd-exporter
statsd_client = statsd.StatsClient('statsd-exporter', 8125)


def execution_time(metric_name, start_time):
    # Measure the script execution time and send it as a metric
    end_time = time.time()
    execution_time = end_time - start_time
    statsd_client.timing(metric_name, execution_time)


def increment_counter(metric_name, value=1):
    # Increment the metric counter by the specified value (default is 1)
    statsd_client.incr(metric_name, value)


def current_value(metric_name, value):
    # Read the current value and send it as a metric
    statsd_client.gauge(metric_name, value)
   
   
def track_unique_values(metric_name, value):
    # Track the number of unique values for a given metric
    statsd_client.set(metric_name, value)

# track_unique_values('unique_visitors', user_id)

