from confluent_kafka import Consumer, TopicPartition

def count_messages(topic, broker='broker:29092'):
    consumer_config = {
        'bootstrap.servers': broker,
        'group.id': 'counting-group',
        'auto.offset.reset': 'earliest'
    }

    # Create an instance of Consumer
    consumer = Consumer(consumer_config)

    # Get the list of partitions for the topic
    partitions = consumer.list_topics(topic).topics[topic].partitions.keys()
    total_count = 0

    try:
        # For each partition, get the first and last offsets
        for partition in partitions:
            # First offset
            beginning_offset = consumer.get_watermark_offsets(TopicPartition(topic, partition, -2))[0]
            # Last offset
            end_offset = consumer.get_watermark_offsets(TopicPartition(topic, partition, -1))[1]
            # Calculate the number of messages in the partition and add to the total count
            total_count += (end_offset - beginning_offset)

    finally:
        # Close the connection with the consumer
        consumer.close()

    return total_count
