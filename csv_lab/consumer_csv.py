from kafka import KafkaConsumer

TOPIC = "csv-transactions"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="csv-consumer-group",
    value_deserializer=lambda v: v.decode("utf-8")
)

print("Starting CLEAN CSV Consumer...")

for msg in consumer:
    print(f"Consumed: {msg.value}")
