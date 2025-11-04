import os
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv

# ------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------
load_dotenv()

CLOUD_URL = os.getenv("CLOUD_URL")
API_KEY = os.getenv("API_KEY")

INDEX_NAME = "incident_data_index"
CSV_FILE = "dummy_incident_tickets.csv"

# ------------------------------------------------------------
# Connect to Elasticsearch
# ------------------------------------------------------------
es = Elasticsearch(
    CLOUD_URL,
    api_key=API_KEY
)

if not es.ping():
    print("Connection failed! Check CLOUD_URL or API_KEY.")
    exit()
else:
    print("Connected to Elasticsearch successfully.")

# ------------------------------------------------------------
# Load and clean CSV data
# ------------------------------------------------------------
try:
    df = pd.read_csv(CSV_FILE)
    print(f"Loaded {len(df)} records from {CSV_FILE}")
except FileNotFoundError:
    print(f"File not found: {CSV_FILE}. Please ensure it exists in this folder.")
    exit()
except Exception as e:
    print("Error reading CSV:", e)
    exit()

# Drop completely empty rows
df = df.dropna(how="all")

# Clean column names (remove spaces, lowercase, replace with underscores)
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# Fill NaN values with empty strings
df = df.fillna("")

# Convert all data to strings to avoid type conflicts during indexing
df = df.astype(str)

print(f"Cleaned dataset: {len(df)} records ready for upload.")

# ------------------------------------------------------------
# Prepare bulk indexing actions
# ------------------------------------------------------------
actions = [
    {
        "_index": INDEX_NAME,
        "_source": row.to_dict()
    }
    for _, row in df.iterrows()
]

# ------------------------------------------------------------
# Perform bulk upload to Elasticsearch
# ------------------------------------------------------------
try:
    success, _ = helpers.bulk(es, actions, raise_on_error=False)
    print(f"Successfully indexed {success} documents into '{INDEX_NAME}'.")
except Exception as e:
    print(f"Error during bulk upload: {e}")
    exit()

# ------------------------------------------------------------
# Verify count of documents in the index
# ------------------------------------------------------------
try:
    count = es.count(index=INDEX_NAME)['count']
    print(f"Total documents currently in index '{INDEX_NAME}': {count}")
except Exception as e:
    print("Could not verify document count:", e)
