"""
cleanup_duplicate_garments.py — one-off

Finds Webflow Garments items that share the same "Airtable Record ID"
field value (a duplicate create — root cause: Airtable's offset
pagination returned the same record twice during a long-running sync,
so the same garment got created twice from one stale in-memory fetch).
webflow_sync.py's airtable_fetch_all() now dedupes against this
happening again going forward — this script cleans up duplicates that
were already created before that fix existed.

For each duplicate group:
  1. Prefer keeping whichever item Airtable's own "Webflow Item ID"
     field currently points to, if it's one of the group.
  2. Otherwise keep the earliest-created item (by Webflow's createdOn).
  3. Delete the rest.
  4. Ensure Airtable's "Webflow Item ID" field points at the survivor.

Defaults to DRY_RUN=true — reports every duplicate group found with no
deletions. Set DRY_RUN=false to actually delete and fix links.

Required env vars:
  AIRTABLE_TOKEN
  WEBFLOW_API_TOKEN
"""

import os
import sys
import time
import requests

AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE  = "appUk1ThnHvWwFDHG"
GARMENTS_TABLE = "All Garments"

WEBFLOW_API_TOKEN = os.environ["WEBFLOW_API_TOKEN"]
GARMENTS_COLLECTION_ID = "68774f3e850c7a30ebc3a0aa"

WF_FIELD_AIRTABLE_ID = "airtable-record-id"
FLD_WEBFLOW_ITEM_ID  = "Webflow Item ID"

AIRTABLE_HEADERS = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
WEBFLOW_HEADERS = {
    "Authorization": f"Bearer {WEBFLOW_API_TOKEN}",
    "accept": "application/json",
    "content-type": "application/json",
}

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"  # defaults SAFE


def webflow_request(method, url, json_body=None, max_retries=5):
    for attempt in range(max_retries):
        res = requests.request(method, url, headers=WEBFLOW_HEADERS, json=json_body, timeout=30)
        if res.ok:
            return res
        if res.status_code == 429 or res.status_code >= 500:
            wait = int(res.headers.get("Retry-After", 5 * (attempt + 1)))
            print(f"  Webflow {res.status_code} — waiting {wait}s, retry {attempt + 1}/{max_retries}")
            time.sleep(wait)
            continue
        raise requests.exceptions.HTTPError(f"{res.status_code} on {method} {url}: {res.text[:300]}", response=res)
    raise requests.exceptions.HTTPError(f"Exhausted retries on {method} {url}")


def fetch_all_garment_items():
    items, offset, limit = [], 0, 100
    while True:
        res = webflow_request(
            "GET",
            f"https://api.webflow.com/v2/collections/{GARMENTS_COLLECTION_ID}/items"
            f"?limit={limit}&offset={offset}",
        )
        data = res.json()
        items.extend(data.get("items", []))
        total = data.get("pagination", {}).get("total", 0)
        offset += len(data.get("items", []))
        if offset >= total or not data.get("items"):
            break
    return items


def airtable_get_webflow_item_id(record_id):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{GARMENTS_TABLE}/{record_id}"
    res = requests.get(url, headers=AIRTABLE_HEADERS, timeout=15)
    if not res.ok:
        return None
    return res.json().get("fields", {}).get(FLD_WEBFLOW_ITEM_ID)


def airtable_set_webflow_item_id(record_id, webflow_item_id):
    if DRY_RUN:
        print(f"    [DRY RUN] would set Airtable {record_id}.{FLD_WEBFLOW_ITEM_ID} = {webflow_item_id}")
        return
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{GARMENTS_TABLE}/{record_id}"
    res = requests.patch(url, headers=AIRTABLE_HEADERS,
                          json={"fields": {FLD_WEBFLOW_ITEM_ID: webflow_item_id}}, timeout=15)
    if not res.ok:
        print(f"    WARNING: failed to update Airtable link for {record_id}: {res.status_code} {res.text[:200]}")


def delete_webflow_item(item_id):
    if DRY_RUN:
        print(f"    [DRY RUN] would DELETE Webflow item {item_id}")
        return
    webflow_request("DELETE", f"https://api.webflow.com/v2/collections/{GARMENTS_COLLECTION_ID}/items/{item_id}")


def main():
    print("=== cleanup_duplicate_garments.py — starting ===")
    if DRY_RUN:
        print("*** DRY RUN — no deletions or Airtable writes will happen ***")

    print("Fetching all Webflow Garments items...")
    items = fetch_all_garment_items()
    print(f"  {len(items)} total items")

    groups = {}
    for item in items:
        airtable_id = item.get("fieldData", {}).get(WF_FIELD_AIRTABLE_ID)
        if not airtable_id:
            continue  # item predates the Airtable Record ID field being populated — not this script's concern
        groups.setdefault(airtable_id, []).append(item)

    duplicate_groups = {k: v for k, v in groups.items() if len(v) > 1}
    print(f"\nDuplicate groups found: {len(duplicate_groups)} "
          f"({sum(len(v) for v in duplicate_groups.values())} items involved)")

    if not duplicate_groups:
        print("Nothing to clean up.")
        return

    resolved, errors = 0, 0
    for airtable_id, group_items in duplicate_groups.items():
        names = [i.get("fieldData", {}).get("name", "?") for i in group_items]
        item_ids = [i["id"] for i in group_items]
        print(f"\n  Airtable {airtable_id}: {len(group_items)} Webflow items — {names}")

        # Decide which to keep: prefer whatever Airtable's own field
        # currently points to, if it's actually one of this group.
        linked_id = airtable_get_webflow_item_id(airtable_id)
        keeper = None
        if linked_id and linked_id in item_ids:
            keeper = next(i for i in group_items if i["id"] == linked_id)
            print(f"    Keeping {keeper['id']} (already linked from Airtable)")
        else:
            # Fall back to earliest-created
            group_items_sorted = sorted(group_items, key=lambda i: i.get("createdOn", ""))
            keeper = group_items_sorted[0]
            print(f"    Keeping {keeper['id']} (earliest created — Airtable link was blank/stale)")

        to_delete = [i for i in group_items if i["id"] != keeper["id"]]

        try:
            airtable_set_webflow_item_id(airtable_id, keeper["id"])
            for extra in to_delete:
                print(f"    Deleting duplicate {extra['id']} ({extra.get('fieldData', {}).get('name', '?')})")
                delete_webflow_item(extra["id"])
                time.sleep(0.2)
            resolved += 1
        except Exception as e:
            errors += 1
            print(f"    FAILED to resolve group {airtable_id}: {e}")

    print(f"\nDone — {resolved} groups resolved, {errors} errors.")
    if DRY_RUN:
        print("This was a dry run — nothing was actually deleted or changed. Set DRY_RUN=false to apply.")


if __name__ == "__main__":
    main()
