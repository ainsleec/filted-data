#!/usr/bin/env python3
"""
Filted — Airtable Sightings → Supabase Listings Sync
Standalone script. Run daily (recommended: after expiry_checker.py).

Finds Airtable Resale Sightings (Active or Sold) that don't yet exist in
Supabase's `listings` table, resolves each one's garment_id by matching
Airtable's linked Garment record against Supabase's `garments.airtable_id`,
and inserts the missing rows.

This closes the gap where new_listings.py adds sightings to Airtable but
nothing downstream ever creates the matching Supabase listings row.

Required env vars:
  AIRTABLE_TOKEN
  AIRTABLE_BASE
  RESALE_SIGHTINGS_TABLE
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
"""

import os, sys, time, requests
from datetime import datetime, timezone

DRY_RUN = "--dry-run" in sys.argv

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN         = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE          = os.environ["AIRTABLE_BASE"]
RESALE_SIGHTINGS_TABLE = os.environ["RESALE_SIGHTINGS_TABLE"]

SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

HEADERS_AT = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type":  "application/json"
}

KEEP_STATUSES = {"Active", "Sold"}


def get_supabase_headers():
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }


# ── Paginated Supabase loaders ─────────────────────────────────────────────────
def load_all_supabase_rows(table, select):
    """Generic paginated Supabase loader using Range headers."""
    page_size = 1000
    offset    = 0
    rows      = []

    while True:
        headers = get_supabase_headers()
        headers["Range-Unit"] = "items"
        headers["Range"]      = f"{offset}-{offset + page_size - 1}"

        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=headers,
            params={"select": select},
            timeout=30
        )
        if not resp.ok:
            print(f"   ⚠️  Supabase load failed on {table} at offset {offset}: {resp.status_code}")
            break

        batch = resp.json()
        rows.extend(batch)

        if len(batch) < page_size:
            break
        offset += page_size

    return rows


# ── Airtable loader ────────────────────────────────────────────────────────────
def load_sightings_to_sync():
    """All Active or Sold sightings from Airtable, with the fields we need."""
    all_sightings = []
    params = {
        "fields[]": [
            "eBay Item ID", "Listing URL", "eBay Title", "Seller Name",
            "Condition", "Listed Price", "Date Listed", "Status",
            "Garment", "Date Sold",
        ],
        "filterByFormula": 'OR(Status = "Active", Status = "Sold")',
        "pageSize": 100
    }
    while True:
        resp = requests.get(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{RESALE_SIGHTINGS_TABLE}",
            headers=HEADERS_AT, params=params
        )
        resp.raise_for_status()
        data = resp.json()
        all_sightings.extend(data.get("records", []))
        if not data.get("offset"):
            break
        params["offset"] = data["offset"]
    return all_sightings


# ── Insert new listings into Supabase ──────────────────────────────────────────
def insert_listings(rows):
    """Bulk insert in batches of 50. On batch failure, retry rows individually
    so one bad row doesn't sink the other 49 good ones."""
    inserted = 0
    errors   = 0
    failed_rows = []

    for i in range(0, len(rows), 50):
        batch = rows[i:i + 50]
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/listings",
            headers=get_supabase_headers(),
            json=batch,
            timeout=30
        )
        if resp.ok:
            inserted += len(batch)
        else:
            print(f"   ⚠️  Batch failed ({resp.status_code}), retrying rows individually...")
            for row in batch:
                r = requests.post(
                    f"{SUPABASE_URL}/rest/v1/listings",
                    headers=get_supabase_headers(),
                    json=[row],
                    timeout=15
                )
                if r.ok:
                    inserted += 1
                else:
                    errors += 1
                    failed_rows.append((row.get("ebay_item_id"), r.status_code, r.text[:150]))
                time.sleep(0.05)
        time.sleep(0.2)

    if failed_rows:
        print(f"\n   ⚠️  {len(failed_rows)} rows failed individually:")
        for eid, code, msg in failed_rows[:15]:
            print(f"      {eid} — {code} {msg}")
        if len(failed_rows) > 15:
            print(f"      ...and {len(failed_rows) - 15} more")

    return inserted, errors


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    run_start = datetime.now(timezone.utc)
    print("🔄 Filted — Listings Sync (Airtable → Supabase)")
    print(f"   {run_start.strftime('%Y-%m-%d %H:%M UTC')}")
    if DRY_RUN:
        print("   🧪 DRY RUN MODE — no writes will be made\n")
    else:
        print()

    print("📊 Loading existing Supabase listings (for dedup)...")
    existing_listings = load_all_supabase_rows("listings", "ebay_item_id,airtable_id")
    existing_ids = {str(r["ebay_item_id"]) for r in existing_listings if r.get("ebay_item_id")}
    existing_airtable_ids = {r["airtable_id"] for r in existing_listings if r.get("airtable_id")}
    print(f"   {len(existing_ids)} existing listing IDs loaded")
    print(f"   {len(existing_airtable_ids)} existing airtable_ids loaded")

    print("\n📊 Loading Supabase garments (for garment_id matching)...")
    garment_rows = load_all_supabase_rows("garments", "id,airtable_id")
    garment_lookup = {r["airtable_id"]: r["id"] for r in garment_rows if r.get("airtable_id")}
    print(f"   {len(garment_lookup)} garments loaded")

    print("\n📦 Loading Airtable sightings (Active + Sold)...")
    sightings = load_sightings_to_sync()
    print(f"   {len(sightings)} sightings to check")

    to_insert        = []
    already_synced    = 0
    already_by_at_id  = 0
    no_item_id        = 0
    no_garment_match  = 0

    for rec in sightings:
        f = rec["fields"]
        item_id = str(f.get("eBay Item ID") or "").strip()

        if not item_id:
            no_item_id += 1
            continue

        if item_id in existing_ids:
            already_synced += 1
            continue

        if rec["id"] in existing_airtable_ids:
            already_by_at_id += 1
            continue

        linked_garments = f.get("Garment", [])
        garment_airtable_id = linked_garments[0] if linked_garments else None
        garment_id = garment_lookup.get(garment_airtable_id) if garment_airtable_id else None

        if not garment_id:
            no_garment_match += 1
            # Still insert — garment_id is nullable, can be backfilled later
            # once the sighting is matched to a garment in Airtable.

        status = f.get("Status")

        row = {
            "airtable_id":  rec["id"],
            "garment_id":   garment_id,
            "ebay_item_id": item_id,
            "title":        f.get("eBay Title", "") or "",
            "listing_url":  f.get("Listing URL", "") or "",
            "seller_name":  f.get("Seller Name", "") or "",
            "condition":    f.get("Condition", "") or "",
            "listed_price": f.get("Listed Price"),
            "started_at":   f.get("Date Listed") or None,
            # Always include these keys (even as None) so every row in a batch
            # has an identical key set — PostgREST bulk insert rejects batches
            # where object shapes differ.
            "sold_price":   f.get("Listed Price") if status == "Sold" else None,
            "sold_at":      (f.get("Date Sold") or None) if status == "Sold" else None,
        }

        to_insert.append(row)
        existing_ids.add(item_id)  # guard against dupes within this same run
        existing_airtable_ids.add(rec["id"])

    print(f"\n   Already synced (by eBay ID): {already_synced}")
    print(f"   Already synced (by airtable_id, no eBay ID match): {already_by_at_id}")
    print(f"   Skipped — no eBay ID: {no_item_id}")
    print(f"   No garment match (inserted anyway, garment_id null): {no_garment_match}")
    print(f"   🔧 New listings to insert: {len(to_insert)}")

    if to_insert:
        if DRY_RUN:
            print(f"\n🧪 DRY RUN — would insert {len(to_insert)} new listings. No writes made.")
            print("   Sample of first 5 rows that would be inserted:")
            for row in to_insert[:5]:
                print(f"   • {row['ebay_item_id']} | {row['title'][:50]!r} | garment_id={row['garment_id']}")
        else:
            print(f"\n📝 Inserting {len(to_insert)} new listings into Supabase...")
            inserted, errors = insert_listings(to_insert)
            print(f"   {inserted} inserted | {errors} errors")
    else:
        print("\n   Nothing new to insert.")

    elapsed = (datetime.now(timezone.utc) - run_start).seconds
    print(f"\n✅ Done in {elapsed}s")


if __name__ == "__main__":
    main()
