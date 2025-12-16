import csv
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: v.encode('utf-8'))

with open('data/transactions_dirty.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # skip header
    for row in reader:
        line = ','.join(row)
        print(f"Producing: {line}")
        producer.send('csv-transactions', value=line)
        time.sleep(0.5)

producer.flush()
print("Finished sending dirty CSV")
