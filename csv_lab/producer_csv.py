from kafka import KafkaProducer
import time

TOPIC = "csv-transactions"

producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092'
,  
    value_serializer=lambda v: str(v).encode('utf-8')
)

print("Starting CLEAN CSV Producer...")

with open("data/transactions.csv", "r") as f:
    header = f.readline()  # skip header
    for line in f:
        line = line.strip()
        print(f"Producing: {line}")
        producer.send(TOPIC, value=line)
        time.sleep(1)  # simulate streaming

producer.flush()
producer.close()

print("Producer finished.")
