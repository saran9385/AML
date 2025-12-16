import requests
import json
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# SPARQL endpoint
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# Custom headers
HEADERS = {
    "User-Agent": "IndiaPEPFetcher/1.0 (your-email@example.com)",  # Replace with your email
    "Accept": "application/json"
}

# Output
OUTPUT_FOLDER = "PEP"
OUTPUT_FILE = "India_PEPs.json"

# SPARQL Query Template with OFFSET for pagination
QUERY_TEMPLATE = """
SELECT DISTINCT ?person ?personLabel ?positionLabel ?startDate ?endDate ?countryLabel
WHERE {{
  ?person wdt:P31 wd:Q5;
          p:P39 ?statement.
  ?statement ps:P39 ?position.
  ?position wdt:P279* wd:Q82955.
  OPTIONAL {{ ?person wdt:P27 wd:Q668. }}
  OPTIONAL {{ ?person wdt:P27 ?country. }}
  FILTER(?country = wd:Q668)

  OPTIONAL {{ ?statement pq:P580 ?startDate. }}
  OPTIONAL {{ ?statement pq:P582 ?endDate. }}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
ORDER BY ?personLabel
LIMIT 1000 OFFSET {offset}
"""

def fetch_peps(offset=0):
    """Fetches one batch of PEP data from Wikidata."""
    query = QUERY_TEMPLATE.format(offset=offset)
    try:
        response = requests.get(
            SPARQL_ENDPOINT,
            headers=HEADERS,
            params={'query': query, 'format': 'json'}
        )
        response.raise_for_status()
        data = response.json()
        return data["results"]["bindings"]
    except Exception as e:
        logging.error(f"Failed to fetch at offset {offset}: {e}")
        return []

def save_peps(data):
    """Appends the fetched data to the JSON file with today's date."""
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    filepath = os.path.join(OUTPUT_FOLDER, OUTPUT_FILE)
    today = datetime.now().strftime("%Y-%m-%d")

    # Load existing
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            existing = json.load(f)
    else:
        existing = []

    existing.append({"dateFetched": today, "records": data})

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=4)
    logging.info(f"Saved {len(data)} records for {today}.")

def main():
    offset = 0
    all_records = []

    while True:
        logging.info(f"Fetching records with offset {offset}...")
        batch = fetch_peps(offset)
        if not batch:
            break
        all_records.extend(batch)
        offset += 1000

    if all_records:
        save_peps(all_records)
        logging.info(f"Total records fetched: {len(all_records)}")
    else:
        logging.warning("No PEP records were fetched.")

if __name__ == "__main__":
    main()
