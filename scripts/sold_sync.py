#!/usr/bin/env python3
"""
Filted — Airtable Sold → Supabase Sync
Runs via GitHub Actions, AFTER expiry_checker each day.

Airtable is the source of truth for sales. You verify them by hand, set
Status = "Sold", and fill Price Sold + Date Sold. This script is the ONLY
writer of ended_reason = 'sold' in Supabase. It carries your hand-verified
achieved price into the listings table so the embed can show real sold data
instead of asking prices wearing a "Sold" badge.

It is a forward, idempotent overwrite:
  • For every Airtable sighting with Status = "Sold", it finds the matching
    Supabase listing (by ebay_item_id, URL regex as fallback) and stamps
    ended_reason='sold', sold_price, sold_at, ended_at.
  • This intentionally OVERWRITES the expiry checker's earlier 'expired' guess
    — that's the expired→sold correction you do manually, made permanent.
  • Re-running changes nothing unless you've added sold data in Airtable.

It does NOT touch rows that aren't sold in Airtable, and it never invents a
sold price: if Price Sold is blank, the row is still marked sold but sold_price
stays null (the embed renders no figure for those).

Set DRY_RUN=1 to preview counts without writing.

Required env (same secrets as expiry_checker.py):
  AIRTABLE_TOKEN, AIRTABLE_BASE, RESALE_SIGHTINGS_TABLE,
  SUPABASE_URL, SUPABASE_SERVICE_KEY
"""

import os
import re
import time
import requests
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN         = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE          = os.environ["AIRTABLE_BASE"]
RESALE_SIGHTINGS_TABLE = os.environ["RESALE_SIGHTINGS_TABLE"]

SUPABASE_URL         = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

DRY_RUN = os.environ.get("DRY_RUN") == "1"

# Exact Airtable field names (from the Sold view)
F_ITEM_ID    = "eBay Item ID"
F_URL        = "Listing URL"
F_STATUS     = "Status"
F_PRICE_SOLD = "Price Sold"
F_DATE_SOLD  = "Date Sold"
F_CONDITION  = "Condition"

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


# ── Parse helpers ─────────────────────────────────────────────────────────────
ITEM_RE = re.compile(r"/itm/(\d+)")


def resolve_item_id(fields: dict) -> str | None:
    raw = str(fields.get(F_ITEM_ID) or "").strip()
    if raw:
        return raw
    m = ITEM_RE.search(fields.get(F_URL, "") or "")
    return m.group(1) if m else None


def parse_price(value) -> float | None:
    if value in (None, "", []):
        return None
    try:
        return round(float(str(value).replace("$", "").replace(",", "").strip()), 2)
    except (ValueError, TypeError):
        return None


def parse_date(value) -> str | None:
    """Airtable date field → 'YYYY-MM-DD' (None if blank/unparseable)."""
    if not value:
        return None
    s = str(value).strip()
    # Airtable may return ISO with time; keep the date part.
    return s.split("T")[0][:10] or None


# ── Load Supabase listing id map ──────────────────────────────────────────────
def load_listing_map() -> dict:
    """{ebay_item_id (str): listing id} across ALL listings, paginated."""
    mapping = {}
    limit, offset = 1000, 0
    while True:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/listings",
            headers=sb_headers(return_minimal=False),
            params={"select": "id,ebay_item_id", "limit": limit, "offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        rows = resp.json()
        for r in rows:
            item = r.get("ebay_item_id")
            if item:
                mapping[str(item).strip()] = r["id"]
        if len(rows) < limit:
            break
        offset += limit
    return mapping


# ── Load Airtable sold sightings ──────────────────────────────────────────────
def load_sold_sightings() -> list:
    out = []
    params = {
        "fields[]": [F_ITEM_ID, F_URL, F_STATUS, F_PRICE_SOLD, F_DATE_SOLD, F_CONDITION],
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


# ── Supabase write ────────────────────────────────────────────────────────────
def mark_sold(listing_id: str, sold_price, sold_at) -> bool:
    payload = {"ended_reason": "sold"}
    if sold_price is not None:
        payload["sold_price"] = sold_price
    if sold_at:
        payload["sold_at"]  = sold_at
        payload["ended_at"] = f"{sold_at}T00:00:00+00:00"  # reflect the sale date
    # If no Date Sold, leave any existing ended_at untouched.

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


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    start = datetime.now(timezone.utc)
    print("=" * 60)
    print("  Filted — Airtable Sold → Supabase Sync")
    print(f"  {start.strftime('%Y-%m-%d %H:%M UTC')}" + ("  [DRY RUN]" if DRY_RUN else ""))
    print("=" * 60)

    print("\n  Loading Supabase listing map...")
    listing_map = load_listing_map()
    print(f"  {len(listing_map)} listings with an eBay item ID")

    print("\n  Loading sold sightings from Airtable...")
    sold = load_sold_sightings()
    print(f"  {len(sold)} sightings marked Sold")

    updated = no_price = unmatched = no_id = errors = 0
    unmatched_samples = []

    for rec in sold:
        f = rec.get("fields", {})
        item_id = resolve_item_id(f)
        if not item_id:
            no_id += 1
            continue

        listing_id = listing_map.get(item_id)
        if not listing_id:
            unmatched += 1
            if len(unmatched_samples) < 10:
                unmatched_samples.append((item_id, f.get(F_URL, "")))
            continue

        price   = parse_price(f.get(F_PRICE_SOLD))
        sold_at = parse_date(f.get(F_DATE_SOLD))
        if price is None:
            no_price += 1  # still marked sold below, just without a figure

        if mark_sold(listing_id, price, sold_at):
            updated += 1
        else:
            errors += 1

        if not DRY_RUN:
            time.sleep(0.04)
        if updated and updated % 250 == 0:
            print(f"  ... {updated} updated")

    print("\n" + "-" * 60)
    print(f"  Updated to sold:      {updated}")
    print(f"    of which no price:  {no_price}  (marked sold, sold_price left null)")
    print(f"  No Supabase listing:  {unmatched}  (sold, but never scraped while active)")
    print(f"  No item ID/URL:       {no_id}")
    print(f"  Errors:               {errors}")
    if unmatched_samples:
        print("\n  Sample unmatched (sold in Airtable, no listings row):")
        for iid, url in unmatched_samples:
            print(f"    {iid}  {url}")
    elapsed = (datetime.now(timezone.utc) - start).seconds
    print(f"\n  Done in {elapsed}s" + ("  [no writes — DRY RUN]" if DRY_RUN else ""))
    print("=" * 60)


if __name__ == "__main__":
    main()
