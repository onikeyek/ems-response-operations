"""
EMS Incident Dispatch Data — Bronze Layer Ingestion Script
============================================================
Pulls a filtered date range of NYC EMS Incident Dispatch Data from the
Socrata API (SODA), paginating through results, and lands the raw data
as a CSV file — no cleaning, no type casting. This is the Bronze layer:
exactly what the source gave us, preserved as-is.

Usage:
    1. Set your Socrata app token as an environment variable:
         export NYC_APP_TOKEN="your_token_here"      (Mac/Linux)
         setx NYC_APP_TOKEN "your_token_here"         (Windows)

    2. Run:
         python ingest_ems_data.py

    3. Adjust START_DATE / END_DATE below to change the date range pulled.
"""

import os
import sys
import time
import csv
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATASET_ID = "76xm-jjuj"  # NYC EMS Incident Dispatch Data
BASE_URL = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"

# Date range to pull (inclusive). Keep this to one calendar year or less —
# this dataset is large, and Databricks Community Edition has storage/compute
# limits worth respecting.
START_DATE = "2025-01-01T00:00:00"
END_DATE = "2025-12-31T23:59:59"

PAGE_SIZE = 50000  # Socrata's max page size per request
OUTPUT_PATH = "ems_incident_dispatch_2025_raw.csv"

APP_TOKEN = os.environ.get("NYC_APP_TOKEN")


def fetch_page(offset: int) -> list[dict]:
    """Fetch a single page of results starting at `offset`."""
    params = {
        "$where": f"incident_datetime between '{START_DATE}' and '{END_DATE}'",
        "$order": "incident_datetime",
        "$limit": PAGE_SIZE,
        "$offset": offset,
    }
    headers = {}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN

    response = requests.get(BASE_URL, params=params, headers=headers, timeout=60)

    if response.status_code == 429:
        print("Rate limited — waiting 30s before retrying...")
        time.sleep(30)
        return fetch_page(offset)

    response.raise_for_status()
    return response.json()


def main():
    if not APP_TOKEN:
        print(
            "WARNING: NYC_APP_TOKEN environment variable not set. "
            "You can still run this, but you'll be throttled quickly.\n"
            "Set it with: export NYC_APP_TOKEN='your_token_here'\n"
        )

    print(f"Pulling EMS incidents from {START_DATE} to {END_DATE}...")

    offset = 0
    total_rows = 0
    fieldnames = None
    writer = None
    out_file = open(OUTPUT_PATH, "w", newline="", encoding="utf-8")

    try:
        while True:
            print(f"  Fetching rows {offset} to {offset + PAGE_SIZE}...")
            page = fetch_page(offset)

            if not page:
                break

            if writer is None:
                fieldnames = list(page[0].keys())
                writer = csv.DictWriter(out_file, fieldnames=fieldnames)
                writer.writeheader()

            for row in page:
                # Ensure every row has all columns (some rows may have
                # missing keys if a field is null/absent for that incident)
                writer.writerow({k: row.get(k, "") for k in fieldnames})

            total_rows += len(page)
            offset += PAGE_SIZE

            if len(page) < PAGE_SIZE:
                # Last page was partial — we've reached the end
                break

            time.sleep(0.5)  # be a polite API citizen

    finally:
        out_file.close()

    print(f"\nDone. {total_rows} rows written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
