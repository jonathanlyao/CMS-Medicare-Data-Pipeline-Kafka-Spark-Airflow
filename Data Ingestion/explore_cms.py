import requests
import json

# verify the available GUID (from CMS catalog)
DATASETS = {
    "inpatient_by_provider_service": "690ddc6c-2767-4618-b277-420ffb2bf27c",  # 145,879 行
    "inpatient_by_provider":         "ee6fb1a5-39b9-46b3-a980-a7284551a732",
    "part_d_spending_by_drug":       "7e0b4365-fd63-4a29-8f5e-e0ac9f66a81b",
    "part_d_quarterly":              "4ff7c618-4e40-483a-b390-c8a58c94fa15",
}

BASE_URL = "https://data.cms.gov/data-api/v1/dataset"

def explore_dataset(name, guid, sample_size=3):
    print(f"\n{'='*60}")
    print(f"Dataset: {name}")
    print(f"GUID: {guid}")

    # total number of rows
    stats = requests.get(f"{BASE_URL}/{guid}/data/stats").json()
    print(f"Total rows: {stats.get('total_rows', 'N/A'):,}")
    
    resp = requests.get(f"{BASE_URL}/{guid}/data", params={"size": sample_size})
    rows = resp.json()

    if rows: 
        print(f"Columns ({len(rows[0])}): {list(rows[0].keys())}")
        print(f"\nSample row:")
        print(json.dumps(rows[0], indent=2))

if __name__ == "__main__":
    for name, guid in DATASETS.items(): 
        explore_dataset(name, guid)


