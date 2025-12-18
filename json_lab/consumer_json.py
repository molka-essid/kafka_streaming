from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "json-transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="json-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

total_amount = 0.0
repaired_events = 0

for msg in consumer:
    event = msg.value

    print("\nConsumed JSON:", event)

    total_amount += event["amount"]

    if event["is_repaired"]:
        repaired_events += 1

    print("💰 Total amount:", total_amount)
    print("🛠 Repaired events:", repaired_events)
