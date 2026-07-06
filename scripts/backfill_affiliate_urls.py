"""
backfill_affiliate_urls.py — one-off

Adds EPN affiliate tracking params to every existing Resale Sightings
"Listing URL" in Airtable that doesn't already have them. Needed because
new_listings.py only started wrapping URLs with affiliate params as of a
recent fix — every sighting created BEFORE that fix has a raw,
un-monetised eBay URL sitting in Airtable (and, transitively, in Supabase
and garments.json once synced).

Safe to re-run: any URL that already contains campid=5339108963 is
skipped, so running this twice does no harm.

Does NOT touch Supabase's `listings` table directly — see the printed
summary at the end for why, and what to check before assuming this alone
is enough.
"""

import os
import time
import requests
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE  = "appUk1ThnHvWwFDHG"
SIGHTINGS_TABLE = "Resale Sightings"

FLD_LISTING_URL = "Listing URL"

AFFILIATE_PARAMS = {
    "campid": "5339108963",
    "mkcid":  "1",
    "mkrid":  "705-53470-19255-0",
    "siteid": "15",
    "toolid": "10001",
    "mkevt":  "1",
}

HEADERS = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"


def already_wrapped(url):
    return "campid=5339108963" in url


def add_affiliate_params(url):
    """Merges the EPN params into the URL's existing query string without
    disturbing anything already there (e.g. an eBay item ID path, or any
    other params already present)."""
    parts = urlsplit(url)
    existing_params = dict(parse_qsl(parts.query))
    merged = {**existing_params, **AFFILIATE_PARAMS}
    new_query = urlencode(merged)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def fetch_all_sightings_with_url():
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{SIGHTINGS_TABLE}"
    records, offset = [], None
    while True:
        params = {"fields[]": [FLD_LISTING_URL]}
        if offset:
            params["offset"] = offset
        res = requests.get(url, headers=HEADERS, params=params, timeout=30)
        res.raise_for_status()
        data = res.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return records


def update_listing_url(record_id, new_url, max_retries=3):
    if DRY_RUN:
        print(f"  [DRY RUN] would update {record_id} -> {new_url}")
        return True
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{SIGHTINGS_TABLE}/{record_id}"
    for attempt in range(max_retries):
        res = requests.patch(url, headers=HEADERS, json={"fields": {FLD_LISTING_URL: new_url}}, timeout=30)
        if res.ok:
            return True
        if res.status_code == 429:
            wait = int(res.headers.get("Retry-After", 5))
            print(f"  Rate limited (429) on {record_id} — waiting {wait}s, retry {attempt + 1}/{max_retries}")
            time.sleep(wait)
            continue
        print(f"  FAILED {record_id}: {res.status_code} {res.text[:150]}")
        return False
    print(f"  FAILED {record_id}: exhausted retries after repeated 429s")
    return False


def main():
    print("=== backfill_affiliate_urls.py — starting ===")
    if DRY_RUN:
        print("*** DRY RUN — no Airtable writes will happen ***")

    records = fetch_all_sightings_with_url()
    print(f"Total sightings fetched: {len(records)}")

    to_update = []
    for r in records:
        current_url = r["fields"].get(FLD_LISTING_URL)
        if not current_url:
            continue
        if already_wrapped(current_url):
            continue
        to_update.append((r["id"], current_url, add_affiliate_params(current_url)))

    print(f"URLs needing affiliate wrapping: {len(to_update)}")

    updated, failed = 0, 0
    for i, (record_id, old_url, new_url) in enumerate(to_update, 1):
        ok = update_listing_url(record_id, new_url)
        if ok:
            updated += 1
        else:
            failed += 1
        if i % 100 == 0:
            print(f"  ...{i}/{len(to_update)} processed ({updated} updated, {failed} failed)")
        time.sleep(0.25)  # ~4 req/sec — stays under Airtable's 5 req/sec/base cap

    print(f"Done — {updated} updated, {failed} failed (out of {len(to_update)} that needed fixing).")
    if failed:
        print("Re-running this script is safe — already-wrapped URLs are skipped automatically,")
        print("so a re-run will only retry the ones that failed above.")
    print()
    print("NOTE: this script only updates Airtable. It does NOT touch Supabase's")
    print("`listings` table listing_url column. Before considering this fully done,")
    print("confirm whether listings_sync.py OVERWRITES listing_url on every run for")
    print("existing rows, or only INSERTS new rows and leaves existing ones alone.")
    print("If it's insert-only, Supabase's copies of these URLs are still stale and")
    print("need a separate one-off update (or a small tweak to listings_sync.py to")
    print("also refresh listing_url on existing rows, not just insert new ones).")


if __name__ == "__main__":
    main()
