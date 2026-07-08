"""
wipe_garments.py — one-off, IRREVERSIBLE, run with real caution

Deletes EVERY item in Webflow's Garments collection, and clears the
"Webflow Item ID" field on every corresponding Airtable Garments record,
so the next webflow_sync.py run treats every qualifying garment as a
brand-new create.

THIS MUST NOT BE RUN until:
  1. snapshot_current_slugs.py has been run successfully and
     current_slugs_snapshot.csv is committed to the repo (this is your
     ONLY way to write correct redirects afterward — there is no undo
     once this script deletes items).
  2. You are genuinely ready for garment pages to 404 for a real window
     of time (likely hours) until webflow_sync.py finishes recreating
     everything and the site is republished.

Safety gates, all deliberate:
  - Defaults to DRY_RUN=true. Must explicitly set DRY_RUN=false to
    delete/clear anything for real.
  - On a real run, requires typing the exact confirmation phrase
    interactively OR passing --confirm on the command line (for
    non-interactive CI use) — either way, nothing destructive happens
    without an explicit, unambiguous signal.
  - Only clears "Webflow Item ID" on Airtable records that actually HAD
    one populated (i.e. records this script is about to orphan by
    deleting their Webflow item) — doesn't touch records with no
    Webflow presence at all.

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
FLD_WEBFLOW_ITEM_ID = "Webflow Item ID"

WEBFLOW_API_TOKEN = os.environ["WEBFLOW_API_TOKEN"]
GARMENTS_COLLECTION_ID = "68774f3e850c7a30ebc3a0aa"

AIRTABLE_HEADERS = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
WEBFLOW_HEADERS = {
    "Authorization": f"Bearer {WEBFLOW_API_TOKEN}",
    "accept": "application/json",
    "content-type": "application/json",
}

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"  # defaults SAFE
CONFIRM_PHRASE = "DELETE ALL GARMENTS"


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


def fetch_all_webflow_items():
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


def fetch_airtable_records_with_webflow_id():
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{GARMENTS_TABLE}"
    records, offset = [], None
    while True:
        params = {"fields[]": [FLD_WEBFLOW_ITEM_ID], "filterByFormula": f"NOT({{{FLD_WEBFLOW_ITEM_ID}}} = '')"}
        if offset:
            params["offset"] = offset
        res = requests.get(url, headers=AIRTABLE_HEADERS, params=params, timeout=30)
        res.raise_for_status()
        data = res.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return records


def clear_airtable_webflow_id(record_id):
    if DRY_RUN:
        print(f"    [DRY RUN] would clear Airtable {record_id}.{FLD_WEBFLOW_ITEM_ID}")
        return
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{GARMENTS_TABLE}/{record_id}"
    res = requests.patch(url, headers=AIRTABLE_HEADERS, json={"fields": {FLD_WEBFLOW_ITEM_ID: ""}}, timeout=15)
    if not res.ok:
        print(f"    WARNING: failed to clear link for {record_id}: {res.status_code} {res.text[:200]}")


def delete_webflow_item(item_id):
    if DRY_RUN:
        print(f"    [DRY RUN] would DELETE Webflow item {item_id}")
        return
    webflow_request("DELETE", f"https://api.webflow.com/v2/collections/{GARMENTS_COLLECTION_ID}/items/{item_id}")


def main():
    print("=== wipe_garments.py — starting ===")

    if not DRY_RUN:
        confirmed = "--confirm" in sys.argv or os.environ.get("CONFIRM_TEXT", "").strip() == CONFIRM_PHRASE
        if not confirmed and sys.stdin.isatty():
            print(f"\nThis will PERMANENTLY DELETE every item in the Garments collection")
            print(f"and clear the Webflow Item ID link on every corresponding Airtable record.")
            typed = input(f"\nType exactly '{CONFIRM_PHRASE}' to proceed, anything else to abort: ")
            confirmed = typed.strip() == CONFIRM_PHRASE
        if not confirmed:
            print(f"Not confirmed (no matching CONFIRM_TEXT env var and no interactive terminal) — aborting.")
            print("Nothing was touched.")
            sys.exit(1)
        print("Confirmed. Proceeding with REAL deletion.")
    else:
        print("*** DRY RUN — no deletions or Airtable writes will happen ***")

    print("\nFetching every current Webflow Garments item...")
    webflow_items = fetch_all_webflow_items()
    print(f"  {len(webflow_items)} items to delete")

    print("Fetching Airtable records that currently have a Webflow Item ID...")
    airtable_records = fetch_airtable_records_with_webflow_id()
    print(f"  {len(airtable_records)} Airtable records to clear")

    print(f"\nDeleting {len(webflow_items)} Webflow items...")
    deleted, delete_errors = 0, 0
    successfully_deleted_ids = set()
    for i, item in enumerate(webflow_items, 1):
        try:
            delete_webflow_item(item["id"])
            deleted += 1
            successfully_deleted_ids.add(item["id"])
        except Exception as e:
            delete_errors += 1
            print(f"  FAILED to delete {item['id']}: {e}")
        if i % 100 == 0:
            print(f"  ...{i}/{len(webflow_items)} processed")
        time.sleep(0.15)

    # Only clear the Airtable link for records whose Webflow item was
    # ACTUALLY deleted. Clearing it unconditionally (the original bug)
    # would orphan-in-reverse: for any item that failed to delete (e.g.
    # a 409 conflict because Resale Sightings still references it), the
    # Webflow item still genuinely exists, but Airtable would think it
    # doesn't — and webflow_sync.py would then create a brand new
    # duplicate for it, recreating the exact problem this wipe exists to
    # fix.
    records_to_clear = [
        r for r in airtable_records
        if r["fields"].get(FLD_WEBFLOW_ITEM_ID) in successfully_deleted_ids
    ]
    skipped_still_linked = len(airtable_records) - len(records_to_clear)

    print(f"\nClearing Webflow Item ID on {len(records_to_clear)} Airtable records "
          f"(successfully deleted only)...")
    if skipped_still_linked:
        print(f"  {skipped_still_linked} records LEFT LINKED because their Webflow item failed to "
              f"delete — these still genuinely exist in Webflow and must not be treated as gone.")

    cleared = 0
    for i, record in enumerate(records_to_clear, 1):
        clear_airtable_webflow_id(record["id"])
        cleared += 1
        if i % 200 == 0:
            print(f"  ...{i}/{len(records_to_clear)} processed")
        time.sleep(0.25)  # stay under Airtable's 5 req/sec cap

    print(f"\nDone — {deleted} Webflow items deleted ({delete_errors} errors), "
          f"{cleared} Airtable links cleared, {skipped_still_linked} left linked (delete failed).")
    if DRY_RUN:
        print("This was a dry run — nothing was actually deleted or changed. Set DRY_RUN=false to apply.")
    else:
        if delete_errors:
            print(f"\n{delete_errors} items could not be deleted — almost certainly still referenced by")
            print("Resale Sightings. These need Sightings dealt with first before a re-run of this")
            print("script can fully clear them. Do NOT run webflow_sync.py yet if a large number of")
            print("items are still undeleted — it will only recreate what's missing, not fix the mix.")
        print("\nNext step: run webflow_sync.py for real to recreate everything from Airtable.")
        print("After that completes, diff new slugs against current_slugs_snapshot.csv and write redirects.")


if __name__ == "__main__":
    main()
