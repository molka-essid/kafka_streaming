from kafka import KafkaConsumer
from datetime import datetime
from collections import Counter

# -------------------------------
# Helpers
# -------------------------------

def is_valid_timestamp(ts):
    try:
        datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        return False

def most_frequent(counter, default=None):
    return counter.most_common(1)[0][0] if counter else default


# -------------------------------
# Kafka Consumer
# -------------------------------

consumer = KafkaConsumer(
    "csv-transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="dirty-csv-group",
    value_deserializer=lambda v: v.decode("utf-8")
)

running_total = 0.0

user_id_counter = Counter()
amount_counter = Counter()
transaction_id_counter = Counter()
timestamp_counter = Counter()

# -------------------------------
# Stream processing
# -------------------------------

for msg in consumer:
    line = msg.value
    print(f"\nConsumed raw: {line}")

    fields = line.split(",")

    # ---------- transaction_id ----------
    try:
        transaction_id = int(fields[0])
    except:
        transaction_id=most_frequent(transaction_id_counter)
        print(f"⚠ missing/invalid transaction_id replaced → {transaction_id}")
        continue

    # ---------- user_id ----------
    try:
        user_id = int(fields[1])
    except:
        user_id = most_frequent(user_id_counter)
        print(f"⚠ missing/invalid user_id replaced → {user_id}")

    # ---------- amount ----------
    try:
        amount = float(fields[2])
    except:
        amount=most_frequent(amount_counter)
        print(f"⚠ missing/invalid amount replaced → {amount}")
        continue

    # ---------- timestamp ----------
    timestamp_raw = fields[3].strip() if len(fields) > 3 else ""

    if not is_valid_timestamp(timestamp_raw):
        timestamp = most_frequent(timestamp_counter)
        print(f"⚠ invalid timestamp replaced → {timestamp}")
    else:
        timestamp = timestamp_raw

    # ---------- update state ----------
    running_total += amount
    user_id_counter[user_id] += 1
    timestamp_counter[timestamp] += 1

    print("✅ Repaired / Valid row")
    print(f"   transaction_id = {transaction_id}")
    print(f"   user_id        = {user_id}")
    print(f"   amount         = {amount}")
    print(f"   timestamp      = {timestamp}")
    print(f"   💰 Running total = {running_total}")
