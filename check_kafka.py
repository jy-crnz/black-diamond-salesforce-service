from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
import json

print("🔌 Forcing direct partition connection...")

consumer = Consumer(
    {"bootstrap.servers": "localhost:9092", "group.id": "instant-read-group-2"}
)

tp = TopicPartition("sf.contacts.dev", 0, OFFSET_BEGINNING)
consumer.assign([tp])

print("🎧 Scanning for NEW masked records (scan-verification-004)...\n" + "-" * 50)

records_found = 0
while records_found < 3:
    msg = consumer.poll(2.0)

    if msg is None:
        print("Broker responded, but no more messages found.")
        break
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    record = json.loads(msg.value().decode("utf-8"))

    # 🚨 ONLY print if it is from our brand new run
    if record.get("meta", {}).get("scan_id") == "scan-verification-004":
        print(json.dumps(record, indent=2))
        records_found += 1

consumer.close()
print("-" * 50 + "\n✅ Event stream verification complete!")
