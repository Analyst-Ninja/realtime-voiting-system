from confluent_kafka import Consumer

# Kafka configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker(s)
    'group.id': 'my_consumer_group',        # Consumer group ID
    'auto.offset.reset': 'earliest'         # Start reading from the beginning
}

consumer = Consumer(consumer_config)

consumer.subscribe(['voteTopic'])

try:
    while True:
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            print(f'Consumer Error: {msg.error()}')
        print(f"Received Message : {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print('Stopping Consumer')
finally:
    consumer.close()