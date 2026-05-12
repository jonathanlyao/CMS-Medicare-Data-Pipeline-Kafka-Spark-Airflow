from kafka import KafkaConsumer

c = KafkaConsumer(
    "cms-inpatient-stream",
    bootstrap_servers="kafka:29092",
    auto_offset_reset="earliest",
    consumer_timeout_ms=5000,
)

for i, msg in enumerate(c):
    if i >= 2:
        break
    print(msg.value[:100])

print("Spark container can reach Kafka!")