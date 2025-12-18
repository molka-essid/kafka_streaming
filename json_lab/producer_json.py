import csv
import json
from collections import Counter
from kafka import KafkaProducer

# ---- STEP 1: Read the CSV once to compute statistics ----
file_path = '../csv_lab/data/transactions_dirty.csv'

valid_amounts = []
valid_user_ids = []
valid_timestamps = []

with open(file_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Amount
        try:
            amount = float(row['amount'])
            valid_amounts.append(amount)
        except (ValueError, TypeError):
            pass
        
        # User ID
        if row['user_id'].isdigit():
            valid_user_ids.append(int(row['user_id']))
        
        # Timestamp
        ts = row['timestamp']
        if ts and len(ts) >= 10:  # simple validation
            valid_timestamps.append(ts)


# Compute replacement values
import statistics
mean_amount = statistics.mean(valid_amounts)
most_common_user = Counter(valid_user_ids).most_common(1)[0][0]
most_common_ts = Counter(valid_timestamps).most_common(1)[0][0]


print("  mean_amount =", mean_amount)
print("  most_common_user =", most_common_user)
print("  most_common_ts =", most_common_ts)

# ---- STEP 2: Create Kafka producer ----
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---- STEP 3: Stream CSV as JSON with smart repairs ----
with open(file_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    repaired_count = 0
    total_amount = 0.0
    
    for row in reader:
        errors = []
        # transaction_id
        try:
            transaction_id = int(row['transaction_id'])
        except:
            transaction_id = -1
            errors.append('transaction_id_invalid')
        
        # user_id
        try:
            user_id = int(row['user_id'])
        except:
            user_id = most_common_user
            errors.append('user_id_repaired')
        
        # amount
        try:
            amount = float(row['amount'])
        except:
            amount = mean_amount
            errors.append('amount_repaired')
        
        # timestamp
        ts = row['timestamp']
        if not ts or len(ts) < 10:
            ts = most_common_ts
            errors.append('timestamp_repaired')
        
        if errors:
            repaired_count += 1
        
        total_amount += amount
        
        json_event = {
            "transaction_id": transaction_id,
            "user_id": user_id,
            "amount": amount,
            "timestamp": ts,
            "is_repaired": bool(errors),
            "errors": errors
        }
        
        # send to Kafka
        producer.send('json-transactions', value=json_event)
        
        print(f"Consumed JSON: {json_event}")
        print(f"💰 Total amount: {total_amount}")
    
    print(f"🛠 Total repaired events: {repaired_count}")
producer.flush(timeout=10)  # ensures all messages are sent
producer.close()