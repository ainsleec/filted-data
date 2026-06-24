#!/usr/bin/env python3
"""
Filted — Airtable Sold → Supabase Sync (with creation pass)
Runs via GitHub Actions, AFTER expiry_checker each day.

Airtable is the source of truth for sales. You verify them by hand, set
Status = "Sold", and fill Price Sold + Date Sold. This script is the ONLY
writer of ended_reason = 'sold' in Supabase.

Two passes:

  1. MATCH-UPDATE (always): for every Airtable sold sighting whose listing was
     scraped while active, find the Supabase row by ebay_item_id and stamp
     ended_reason='sold', sold_price, sold_at, ended_at. Overwrites the expiry
     checker's earlier 'expired' guess — that's the expired->sold correction.

  2. CREATE-MISSING (when CREATE_MISSING=1): many sold comps were never scraped
     active (you found the completed sale directly). Those have no listings row.
     This pass inserts one, joined to the right garment, so the achieved price
     feeds the embed's per-garment stats. Rows that can't resolve a garment are
     REPORTED as orphans, not inserted (an orphan helps nothing — the embed
     reads by garment — and it doubles as a natural filter for junk matches).

Rules baked in (all agreed):
  - Only create rows that resolve a real garment; orphans reported, not inserted.
  - One row per sold sighting, idempotent on ebay_item_id (no garment-level dedupe
    — two genuine sales of the same dress both count toward its median).
  - listed_price stays NULL unless Airtable's Listed Price is populated. Never
    copied from sold_price — that's the asking-equals-sold trap we already fixed.
  - listing_url is stored VERBATIM from Airtable, preserving eBay Partner Network
    affiliate params (?campid=...). The bare item ID is used only for matching.

Flags:
  DRY_RUN=1        count only, no writes (works for both passes)
  CREATE_MISSING=1 enable the creation pass (off = match-update only)

Required env (same secrets as expiry_checker.py):
  AIRTABLE_TOKEN, AIRTABLE_BASE, RESALE_SIGHTINGS_TABLE,
  SUPABASE_URL, SUPABASE_SERVICE_KEY
"""

import os
import re
import time
import requests
from datetime import datetime, timezone

# -- Config --------------------------------------------------------------------
AIRTABLE_TOKEN         = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE          = os.environ["AIRTABLE_BASE"]
RESALE_SIGHTINGS_TABLE = os.environ["RESALE_SIGHTINGS_TABLE"]

SUPABASE_URL         = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

DRY_RUN        = os.environ.get("DRY_RUN") == "1"
CREATE_MISSING = os.environ.get("CREATE_MISSING") == "1"

# Exact Airtable field names (from the Sold view)
F_ITEM_ID      = "eBay Item ID"
F_URL          = "Listing URL"
F_STATUS       = "Status"
F_PRICE_SOLD   = "Price Sold"
F_DATE_SOLD    = "Date Sold"
F_CONDITION    = "Condition"
F_GARMENT      = "Garment"        # linked record -> [Airtable garment record id]
F_LISTED_PRICE = "Listed Price"
F_DATE_LISTED  = "Date Listed"
F_TITLE        = "eBay Title"
F_SELLER       = "Seller Name"

SOLD_STATUS = "Sold"

HEADERS_AT = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type":  "application/json",
}


def sb_headers(return_minimal=True):
    h = {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
    }
    if return_minimal:
        h["Prefer"] = "return=minimal"
    return h


# -- Parse helpers -------------------------------------------------------------
ITEM_RE = re.compile(r"/itm/(\d+)")


def resolve_item_id(fields):
    """Bare numeric item ID — for matching/dedupe only. Never used as the URL."""
    raw = str(fields.get(F_ITEM_ID) or "").strip()
    if raw:
        return raw
    m = ITEM_RE.search(fields.get(F_URL, "") or "")
    return m.group(1) if m else None


def parse_price(value):
    if value in (None, "", []):
        return None
    try:
        return round(float(str(value).replace("$", "").replace(",", "").strip()), 2)
    except (ValueError, TypeError):
        return None


def parse_date(value):
    if not value:
        return None
    s = str(value).strip()
    return s.split("T")[0][:10] or None


def first_linked(value):
    """Airtable linked-record field -> first record id, or None."""
    if isinstance(value, list):
        return value[0] if value else None
    return value or None


# -- Supabase loads ------------------------------------------------------------
def _paginate(path, select):
    rows, limit, offset = [], 1000, 0
    while True:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/{path}",
            headers=sb_headers(return_minimal=False),
            params={"select": select, "limit": limit, "offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        page = resp.json()
        rows.extend(page)
        if len(page) < limit:
            break
        offset += limit
    return rows


def load_listing_map():
    """{ebay_item_id (str): listing id}."""
    return {
        str(r["ebay_item_id"]).strip(): r["id"]
        for r in _paginate("listings", "id,ebay_item_id")
        if r.get("ebay_item_id")
    }


def load_garment_map():
    """{airtable garment record id: garment uuid}."""
    return {
        str(r["airtable_id"]).strip(): r["id"]
        for r in _paginate("garments", "id,airtable_id")
        if r.get("airtable_id")
    }


# -- Airtable load -------------------------------------------------------------
def load_sold_sightings():
    out = []
    params = {
        "fields[]": [
            F_ITEM_ID, F_URL, F_STATUS, F_PRICE_SOLD, F_DATE_SOLD, F_CONDITION,
            F_GARMENT, F_LISTED_PRICE, F_DATE_LISTED, F_TITLE, F_SELLER,
        ],
        "filterByFormula": f'{{{F_STATUS}}}="{SOLD_STATUS}"',
        "pageSize": 100,
    }
    while True:
        resp = requests.get(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{RESALE_SIGHTINGS_TABLE}",
            headers=HEADERS_AT, params=params, timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        out.extend(data.get("records", []))
        if not data.get("offset"):
            break
        params["offset"] = data["offset"]
    return out


# -- Supabase writes -----------------------------------------------------------
def mark_sold(listing_id, sold_price, sold_at):
    payload = {"ended_reason": "sold"}
    if sold_price is not None:
        payload["sold_price"] = sold_price
    if sold_at:
        payload["sold_at"]  = sold_at
        payload["ended_at"] = f"{sold_at}T00:00:00+00:00"
    if DRY_RUN:
        return True
    resp = requests.patch(
        f"{SUPABASE_URL}/rest/v1/listings",
        headers=sb_headers(),
        params={"id": f"eq.{listing_id}"},
        json=payload,
        timeout=15,
    )
    return resp.ok


def create_listing(item_id, url, garment_uuid, sold_price, sold_at,
                   listed_price, date_listed, condition, title, seller):
    payload = {
        "ebay_item_id": item_id,
        "listing_url":  url,            # VERBATIM — keeps EPN affiliate params intact
        "garment_id":   garment_uuid,
        "ended_reason": "sold",
    }
    if condition:    payload["condition"]    = condition
    if title:        payload["title"]        = title
    if seller:       payload["seller_name"]  = seller
    if sold_price is not None:
        payload["sold_price"] = sold_price
    if sold_at:
        payload["sold_at"]  = sold_at
        payload["ended_at"] = f"{sold_at}T00:00:00+00:00"
    if date_listed:  payload["started_at"]   = date_listed
    if listed_price is not None:               # only a REAL ask; never sold_price
        payload["listed_price"] = listed_price
    if DRY_RUN:
        return True
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/listings",
        headers=sb_headers(),
        json=payload,
        timeout=15,
    )
    return resp.ok


# -- Main ----------------------------------------------------------------------
def main():
    start = datetime.now(timezone.utc)
    mode = []
    if DRY_RUN:        mode.append("DRY RUN")
    if CREATE_MISSING: mode.append("CREATE MISSING")
    print("=" * 60)
    print("  Filted - Airtable Sold -> Supabase Sync")
    print(f"  {start.strftime('%Y-%m-%d %H:%M UTC')}" + (f"  [{' . '.join(mode)}]" if mode else ""))
    print("=" * 60)

    print("\n  Loading Supabase listing map...")
    listing_map = load_listing_map()
    print(f"  {len(listing_map)} listings with an eBay item ID")

    garment_map = {}
    if CREATE_MISSING:
        print("\n  Loading Supabase garment map...")
        garment_map = load_garment_map()
        print(f"  {len(garment_map)} garments with an airtable_id")

    print("\n  Loading sold sightings from Airtable...")
    sold = load_sold_sightings()
    print(f"  {len(sold)} sightings marked Sold")

    updated = no_price = unmatched = no_id = errors = 0
    created = created_no_price = orphaned = 0
    created_ids = set()
    unmatched_samples, orphan_samples = [], []

    for rec in sold:
        f = rec.get("fields", {})
        item_id = resolve_item_id(f)
        if not item_id:
            no_id += 1
            continue

        # Pass 1: update an existing listing
        if item_id in listing_map:
            price   = parse_price(f.get(F_PRICE_SOLD))
            sold_at = parse_date(f.get(F_DATE_SOLD))
            if price is None:
                no_price += 1
            if mark_sold(listing_map[item_id], price, sold_at):
                updated += 1
            else:
                errors += 1
            if not DRY_RUN:
                time.sleep(0.04)
            continue

        # Pass 2: no listing row exists
        if not CREATE_MISSING:
            unmatched += 1
            if len(unmatched_samples) < 10:
                unmatched_samples.append((item_id, f.get(F_URL, "")))
            continue

        garment_uuid = garment_map.get(str(first_linked(f.get(F_GARMENT)) or "").strip())
        if not garment_uuid:
            orphaned += 1
            if len(orphan_samples) < 15:
                orphan_samples.append((item_id, f.get(F_TITLE, ""), first_linked(f.get(F_GARMENT))))
            continue
        if item_id in created_ids:
            continue

        price   = parse_price(f.get(F_PRICE_SOLD))
        sold_at = parse_date(f.get(F_DATE_SOLD))
        listed  = parse_price(f.get(F_LISTED_PRICE))
        dlisted = parse_date(f.get(F_DATE_LISTED))
        if price is None:
            created_no_price += 1

        if create_listing(
            item_id, f.get(F_URL), garment_uuid, price, sold_at,
            listed, dlisted, f.get(F_CONDITION), f.get(F_TITLE), f.get(F_SELLER),
        ):
            created += 1
            created_ids.add(item_id)
            listing_map[item_id] = "new"   # guard against in-run dupes
        else:
            errors += 1
        if not DRY_RUN:
            time.sleep(0.04)

    print("\n" + "-" * 60)
    print(f"  Updated existing -> sold:  {updated}")
    print(f"    of which no price:       {no_price}")
    if CREATE_MISSING:
        print(f"  Created new sold rows:     {created}")
        print(f"    of which no price:       {created_no_price}")
        print(f"  Orphaned (no garment):     {orphaned}  (reported, NOT inserted)")
    else:
        print(f"  No Supabase listing:       {unmatched}  (run with CREATE_MISSING=1 to insert)")
    print(f"  No item ID/URL:            {no_id}")
    print(f"  Errors:                    {errors}")

    if unmatched_samples:
        print("\n  Sample unmatched:")
        for iid, url in unmatched_samples:
            print(f"    {iid}  {url}")
    if orphan_samples:
        print("\n  Sample orphans (sold, but garment not in Supabase - eyeball for junk):")
        for iid, title, gid in orphan_samples:
            print(f"    {iid}  garment={gid}  {str(title)[:60]}")

    elapsed = (datetime.now(timezone.utc) - start).seconds
    print(f"\n  Done in {elapsed}s" + ("  [no writes - DRY RUN]" if DRY_RUN else ""))
    print("=" * 60)


if __name__ == "__main__":
    main()
