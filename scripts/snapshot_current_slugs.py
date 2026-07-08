"""
snapshot_current_slugs.py — one-off, run BEFORE any wipe-and-rebuild

Produces the authoritative "Airtable Record ID -> current live Webflow
slug" mapping needed to write correct redirects after deleting and
recreating every Garments item from scratch.

Deliberately does NOT trust Airtable's own "Slug" field/CSV column as
the source of truth for what's currently live — today's session proved
repeatedly that Airtable's stored copy can drift from what's actually
published on Webflow (Consonance Dress, the Alemais product-code
fallback case, etc.). Instead, this pulls every Garments item DIRECTLY
from Webflow's API and reads each item's real, current slug field.

Two ways to run it:
  1. With a CSV export from Airtable (Record ID + Webflow Item ID
     columns) as the join list — pass the CSV path as an argument.
  2. With no argument — fetches the Airtable Garments table live via the
     API instead (safer if the CSV might be stale by the time you run
     this, e.g. if garments have been added/matched since the export).

Output: current_slugs_snapshot.csv — airtable_record_id, webflow_item_id,
current_slug, name. This file is the ground truth to diff against after
the rebuild: for any Airtable ID whose NEW slug differs from what's in
this snapshot, write a redirect from the old slug to the new one.

Required env vars:
  WEBFLOW_API_TOKEN
  AIRTABLE_TOKEN   (only needed if running without a CSV argument)
"""

import os
import sys
import csv
import json
import requests

WEBFLOW_API_TOKEN = os.environ["WEBFLOW_API_TOKEN"]
GARMENTS_COLLECTION_ID = "68774f3e850c7a30ebc3a0aa"

AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN")
AIRTABLE_BASE  = "appUk1ThnHvWwFDHG"
GARMENTS_TABLE = "All Garments"

WEBFLOW_HEADERS = {
    "Authorization": f"Bearer {WEBFLOW_API_TOKEN}",
    "accept": "application/json",
}

OUTPUT_PATH = "current_slugs_snapshot.csv"


def fetch_all_webflow_garments():
    """Pulls every item in the Garments collection directly from Webflow —
    this is the authoritative source for 'what slug is actually live
    right now', not Airtable's copy."""
    items, offset, limit = [], 0, 100
    while True:
        res = requests.get(
            f"https://api.webflow.com/v2/collections/{GARMENTS_COLLECTION_ID}/items",
            headers=WEBFLOW_HEADERS,
            params={"limit": limit, "offset": offset},
            timeout=30,
        )
        res.raise_for_status()
        data = res.json()
        items.extend(data.get("items", []))
        total = data.get("pagination", {}).get("total", 0)
        offset += len(data.get("items", []))
        if offset >= total or not data.get("items"):
            break
    return items


def load_pairs_from_csv(csv_path):
    """Returns list of (airtable_record_id, webflow_item_id) from an
    Airtable CSV export. Only rows with BOTH populated are usable."""
    pairs = []
    with open(csv_path, newline="", encoding="utf-8-sig") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            record_id = (row.get("Record ID") or "").strip()
            webflow_id = (row.get("Webflow Item ID") or "").strip()
            if record_id and webflow_id:
                pairs.append((record_id, webflow_id))
    return pairs


def load_pairs_from_airtable_api():
    """Fetches the Garments table live via Airtable's API instead of a
    CSV — use this if the CSV might be stale."""
    if not AIRTABLE_TOKEN:
        print("ERROR: no CSV path given AND no AIRTABLE_TOKEN set — can't build the join list.")
        sys.exit(1)
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{GARMENTS_TABLE}"
    pairs, offset = [], None
    while True:
        params = {"fields[]": ["Webflow Item ID"]}
        if offset:
            params["offset"] = offset
        res = requests.get(url, headers=headers, params=params, timeout=30)
        res.raise_for_status()
        data = res.json()
        for r in data.get("records", []):
            webflow_id = (r["fields"].get("Webflow Item ID") or "").strip()
            if webflow_id:
                pairs.append((r["id"], webflow_id))
        offset = data.get("offset")
        if not offset:
            break
    return pairs


def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else None

    print("=== snapshot_current_slugs.py — starting ===")

    if csv_path:
        print(f"Loading Airtable Record ID / Webflow Item ID pairs from {csv_path}...")
        pairs = load_pairs_from_csv(csv_path)
    else:
        print("No CSV given — fetching Airtable Garments table live instead...")
        pairs = load_pairs_from_airtable_api()

    print(f"  {len(pairs)} Airtable records with a Webflow Item ID")

    print("Fetching every current Garments item directly from Webflow...")
    webflow_items = fetch_all_webflow_garments()
    print(f"  {len(webflow_items)} live Webflow items")

    webflow_by_id = {item["id"]: item for item in webflow_items}

    snapshot = []
    orphaned = []
    for airtable_id, webflow_id in pairs:
        item = webflow_by_id.get(webflow_id)
        if not item:
            orphaned.append((airtable_id, webflow_id))
            continue
        field_data = item.get("fieldData", {})
        snapshot.append({
            "airtable_record_id": airtable_id,
            "webflow_item_id": webflow_id,
            "current_slug": field_data.get("slug", ""),
            "name": field_data.get("name", ""),
        })

    with open(OUTPUT_PATH, "w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=["airtable_record_id", "webflow_item_id", "current_slug", "name"])
        writer.writeheader()
        writer.writerows(snapshot)

    print(f"\nWrote {len(snapshot)} rows to {OUTPUT_PATH}")

    if orphaned:
        print(f"\n{len(orphaned)} Airtable records point to a Webflow Item ID that no longer exists")
        print("(item was deleted at some point, link never cleared) — these have no current slug to")
        print("preserve, so no redirect will be needed for them regardless — nothing to do here.")

    print("\nThis file is your reference for writing redirects after the rebuild:")
    print("for any airtable_record_id whose NEWLY generated slug differs from current_slug here,")
    print("write a KV redirect from current_slug -> new_slug.")


if __name__ == "__main__":
    main()
