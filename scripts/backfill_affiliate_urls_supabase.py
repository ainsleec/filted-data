"""
backfill_affiliate_urls_supabase.py — one-off

Companion to backfill_affiliate_urls.py. That script fixes Airtable's
Resale Sightings "Listing URL" field, but listings_sync.py is confirmed
insert-only (dedups by ebay_item_id/airtable_id, never updates existing
rows) — so every listing already synced into Supabase before the
affiliate-wrapping fix keeps its stale, unwrapped URL forever unless
fixed here directly.

Run this AFTER (or independently of) backfill_affiliate_urls.py — order
doesn't matter between the two, they touch different systems.

Safe to re-run: any URL that already contains campid=5339108963 is
skipped.

Required env vars:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
"""

import os
import time
import requests
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

AFFILIATE_PARAMS = {
    "campid": "5339108963",
    "mkcid":  "1",
    "mkrid":  "705-53470-19255-0",
    "siteid": "15",
    "toolid": "10001",
    "mkevt":  "1",
}

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"


def headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def already_wrapped(url):
    return "campid=5339108963" in url


def add_affiliate_params(url):
    parts = urlsplit(url)
    existing_params = dict(parse_qsl(parts.query))
    merged = {**existing_params, **AFFILIATE_PARAMS}
    new_query = urlencode(merged)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def load_all_listings():
    """Paginated load of every listings row with a non-empty listing_url,
    using the same Range-header pagination pattern as listings_sync.py."""
    page_size = 1000
    offset = 0
    rows = []
    while True:
        h = headers()
        h["Range-Unit"] = "items"
        h["Range"] = f"{offset}-{offset + page_size - 1}"
        res = requests.get(
            f"{SUPABASE_URL}/rest/v1/listings",
            headers=h,
            params={"select": "id,listing_url", "listing_url": "not.is.null"},
            timeout=30,
        )
        if not res.ok:
            print(f"  WARNING: load failed at offset {offset}: {res.status_code} {res.text[:200]}")
            break
        batch = res.json()
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def update_listing_url(row_id, new_url, max_retries=3):
    if DRY_RUN:
        print(f"  [DRY RUN] would update listings.id={row_id} -> {new_url}")
        return True
    for attempt in range(max_retries):
        res = requests.patch(
            f"{SUPABASE_URL}/rest/v1/listings",
            headers=headers(),
            params={"id": f"eq.{row_id}"},
            json={"listing_url": new_url},
            timeout=15,
        )
        if res.ok:
            return True
        if res.status_code == 429 or res.status_code >= 500:
            wait = 5 * (attempt + 1)
            print(f"  Transient error ({res.status_code}) on {row_id} — waiting {wait}s, retry {attempt + 1}/{max_retries}")
            time.sleep(wait)
            continue
        print(f"  FAILED {row_id}: {res.status_code} {res.text[:150]}")
        return False
    print(f"  FAILED {row_id}: exhausted retries after repeated transient errors")
    return False


def main():
    print("=== backfill_affiliate_urls_supabase.py — starting ===")
    if DRY_RUN:
        print("*** DRY RUN — no Supabase writes will happen ***")

    rows = load_all_listings()
    print(f"Total listings with a URL: {len(rows)}")

    to_update = [
        (r["id"], r["listing_url"], add_affiliate_params(r["listing_url"]))
        for r in rows
        if r.get("listing_url") and not already_wrapped(r["listing_url"])
    ]
    print(f"URLs needing affiliate wrapping: {len(to_update)}")

    updated, errors = 0, 0
    for i, (row_id, old_url, new_url) in enumerate(to_update, 1):
        ok = update_listing_url(row_id, new_url)
        if ok:
            updated += 1
        else:
            errors += 1
            print(f"  FAILED to update listings.id={row_id}")
        if i % 200 == 0:
            print(f"  ...{i}/{len(to_update)} processed")
        time.sleep(0.08)

    print(f"\nDone — {updated} updated, {errors} errors.")


if __name__ == "__main__":
    main()
