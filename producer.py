from confluent_kafka import Producer

producer_configs = {
    'bootstrap.servers' : 'localhost:9092'
}

producer = Producer(producer_configs)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery Failed : {err}")
    else:
        print(f"Message delivered to {msg.topic()}")

topic = 'voteTopic'

def sendVote(candidate):
    producer.produce(
        topic,
        value=str(candidate),
        callback = delivery_report
    )

    producer.flush()

while True:
    candidate = input('Select Candidate from A, B, & C only: ').lower().strip()
    if candidate in ['a','b','c']:
        sendVote(candidate=candidate)
