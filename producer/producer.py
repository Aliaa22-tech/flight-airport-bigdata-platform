import csv
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "flight-events")
RATE = float(os.getenv("RATE", "10"))
SOURCE = os.getenv("SOURCE_FILE", "/data/2025/2025_12.csv")
STEP = 20  # ناخد صف من كل 20 صف عشان العينة تكون متنوعة


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


print("Loading sample rows from", SOURCE, flush=True)
rows = []
with open(SOURCE, newline="", encoding="utf-8-sig", errors="replace") as f:
    for i, r in enumerate(csv.DictReader(f)):
        if i % STEP == 0:
            rows.append(r)
print("Loaded", len(rows), "rows", flush=True)

producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except Exception as e:
        print("Kafka not ready, retrying...", e, flush=True)
        time.sleep(3)

count = 0
while True:
    r = random.choice(rows)
    event = {
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "flight_date": r["FlightDate"],
        "airline": r["Reporting_Airline"],
        "origin": r["Origin"],
        "dest": r["Dest"],
        "dep_time_blk": r["DepTimeBlk"],
        "dep_delay": num(r["DepDelay"]),
        "arr_delay": num(r["ArrDelay"]),
        "cancelled": int(num(r["Cancelled"]) or 0),
        "cancellation_code": r["CancellationCode"],
        "distance": num(r["Distance"]),
    }
    producer.send(TOPIC, event)
    count += 1
    if count % 100 == 0:
        print("sent", count, flush=True)
    time.sleep(1.0 / RATE)