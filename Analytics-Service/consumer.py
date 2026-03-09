from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "resume-topic",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="analytics-consumer",
    value_deserializer=lambda x: x.decode("utf-8")
)

print("Python analytics consumer started...")

for message in consumer:
    print("Received:", message.value)