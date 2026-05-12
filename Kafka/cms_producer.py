"""
CMS Medicare Inpatient Data -> Kafka Producer 
Pull data from CMS API, import to Kafka topic by rows, stimulate streaming scenario
"""
import os
import requests 
import json
import time
from kafka import KafkaProducer

# - Setting Up - 
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = "cms-inpatient-stream"
CMS_API_URL = "https://data.cms.gov/data-api/v1/dataset/690ddc6c-2767-4618-b277-420ffb2bf27c/data"
BATCH_SIZE = 1000 # How many rows importing from API 
TOTAL_ROWS = 5000 # Verify after first 5000 rows, full importing 145000 later on

# - Initiating Producer - 
producer = KafkaProducer(
    bootstrap_servers = KAFKA_BROKER,
    value_serializer = lambda v: json.dumps(v).encode("utf-8"),
    key_serializer = lambda k: k.encode("utf-8") if k else None, 
)

def fetch_cms_data(offset, size): 
    """import some data from CMS API"""
    resp = requests.get(CMS_API_URL, params={"size": size, "offset": offset})
    resp.raise_for_status()
    return resp.json()

def produce_to_kafka(rows): 
    """transfer every single row to Kafka as a batch"""
    for row in rows: 
        # Use DRG_Cd as key -> same DRG data into same partition
        key = row.get("DRG_Cd", "UNKNOWN")
        producer.send(TOPIC, key=key, value=row)
    producer.flush()

def main(): 
    print(f"Starting CMS → Kafka Producer")
    print(f"Topic: {TOPIC} | Batch size: {BATCH_SIZE} | Target: {TOTAL_ROWS} rows")
    print("-" * 60)

    total_sent = 0
    offset = 0

    while total_sent < TOTAL_ROWS: 
        size = min(BATCH_SIZE, TOTAL_ROWS - total_sent)
        rows = fetch_cms_data(offset, size)

        if not rows: 
            print("No more data from CMS API.")
            break

        produce_to_kafka(rows)
        total_sent += len(rows)
        offset += len(rows)

        # print progress + sample data 
        sample = rows[0]
        print(
            f"[Batch {offset // BATCH_SIZE}] Sent {total_sent:,} rows | "
            f"Last: {sample['Rndrng_Prvdr_Org_Name'][:30]} | "
            f"DRG {sample['DRG_Cd']} | "
            f"Charge ${float(sample['Avg_Submtd_Cvrd_Chrg']):,.0f} vs "
            f"Medicare ${float(sample['Avg_Mdcr_Pymt_Amt']):,.0f}"
        )

        # Simulate streaming gap (in real scene where the data arrive consecutively)
        time.sleep(0.5)

    print("-" * 60)
    print(f"Done. Total {total_sent:,} messages sent to '{TOPIC}'")

if __name__ == "__main__":
    main()