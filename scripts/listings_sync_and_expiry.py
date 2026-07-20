#!/usr/bin/env python3
"""
Filted — Listings Sync + eBay Expiry Checker + Sold Verification (consolidated)

Was three separate scripts (listings_sync.py, expiry_checker.py, and
sold_sync.py) on independent GitHub Actions schedules. Merged into one,
run in strict sequence within a single job, to close two real gaps.

GAP 1 (original merge, Phase 1 + Phase 2): if a listing's entire active
life on eBay was shorter than the time between the two scripts' separate
runs, it could flip from Active to Expired in Airtable BEFORE
listings_sync ever synced it — and since listings_sync only fetched
Active/Sold sightings, that record fell permanently outside its query.
No Supabase row ever got created, expiry_checker had nothing to close
out, and that listing's resale price data was silently lost forever (it
still powers price history on garment pages).

Two structural fixes, both needed together:
  1. The sync phase fetches ALL sightings (no Status filter) and relies
     purely on dedup-by-id, so a sighting can never fall outside the
     query window based on its current status.
  2. Any sighting synced for the first time in a NON-Active state (Sold
     or Expired) gets ended_at/end_reason set immediately at insert.

THIRD fix (prior revision) — the "no eBay Item ID field" gap: some
Airtable sightings have their dedicated "eBay Item ID" field blank, with
the ID only present inside the Listing URL. Phase 1 now uses the same
URL-fallback Phase 2 already had, and reconciliation backfills
ebay_item_id onto the Supabase row whenever it derives one.

FOURTH fix (prior revision) — the end_reason/ended_reason split: the
listings table has two separate columns, `end_reason` (read by the
Worker's /garment-history endpoint — the one the frontend actually
displays) and `ended_reason` (a leftover from an earlier schema
iteration). Every write in this script sets BOTH columns together now,
via a single shared write path, so they can never drift apart again.

GAP 2 / FIFTH fix (this revision) — Sold Sync consolidation: the old
standalone sold_sync.py ran on its own separate schedule, trusted to
fire after this script by cron timing alone rather than anything
structural, AND it only ever wrote `ended_reason` — never `end_reason` —
meaning every hand-verified sale (the most trustworthy data in the whole
pipeline) silently kept whichever `end_reason` guess Phase 2 had already
made (usually "expired", since eBay's Browse API can't distinguish sold
from just-ended). That's now Phase 3 below, sharing this script's single
end-state write path (mark_sold_listing / create_sold_listing), so
ordering is guaranteed by the code instead of two independent cron
schedules, and end_reason/ended_reason can't split again for sold rows
either.

SIXTH fix (this revision) — aggregate stats were never recomputed: the
`garments` table carries active_listing_count / listed_price_median /
listed_price_min / listed_price_max columns, read by both the campaign
embed's resale pill and search_garments()'s ranking tie-break — but
nothing anywhere (no script, no Postgres trigger, no pg_cron job) ever
recalculated them after each garment row was first created. They were
frozen at whatever value existed on day one, silently drifting further
from reality with every listing opened or closed since. Phase 4 below
calls a Postgres function (recompute_garment_stats() — see
recompute_garment_stats.sql, run once manually before this script's
first run with this phase) that recalculates all four columns from
CURRENTLY ACTIVE listings in one pass, at the end of every run.

Run order within main(): sync, then expiry-check, then sold-verify, then
recompute-stats — so every run starts from as complete a picture as this
pass can make it, checks what's still genuinely active on eBay, lets
Airtable's hand-verified sales overwrite any "expired" guess with the
real outcome, and finally recalculates every garment's aggregate numbers
from that now-correct end state.

Required env vars:
  AIRTABLE_TOKEN, AIRTABLE_BASE, RESALE_SIGHTINGS_TABLE
  EBAY_CLIENT_ID, EBAY_CLIENT_SECRET
  SUPABASE_URL, SUPABASE_SERVICE_KEY

Optional flags:
  DRY_RUN=true         count/log only, no writes (covers all three phases)
  CREATE_MISSING=true  enable Phase 3's creation pass — inserts a Supabase
                        row for sold comps that were never scraped active
                        (off by default: match-update only, since creation
                        produces an orphan report worth eyeballing before
                        you turn this on for the first time)
"""

import os, re, sys, json, time, base64, requests
from datetime import datetime, timezone, timedelta

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true" or "--dry-run" in sys.argv
CREATE_MISSING = os.environ.get("CREATE_MISSING", "false").lower() == "true"

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN         = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE          = os.environ["AIRTABLE_BASE"]
RESALE_SIGHTINGS_TABLE = os.environ["RESALE_SIGHTINGS_TABLE"]

EBAY_CLIENT_ID     = os.environ["EBAY_CLIENT_ID"]
EBAY_CLIENT_SECRET = os.environ["EBAY_CLIENT_SECRET"]

SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

MIN_AGE_DAYS = 1
RECHECK_DAYS = 1
BATCH_LIMIT  = 2000

HEADERS_AT = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type":  "application/json"
}

UNMATCHED_REPORT_PATH = "expiry_checker_unmatched.json"

ITEM_ID_URL_RE = re.compile(r"/itm/(\d+)")

# Field names used specifically by Phase 3 (Sold verification) — matches
# the exact Airtable field names from the old sold_sync.py's Sold view.
F_PRICE_SOLD = "Price Sold"


def extract_item_id(item_id_field, listing_url):
    """Shared by all three phases. Prefer the dedicated field; fall back
    to parsing it out of the Listing URL when that field is blank —
    this is the fix for sightings that only ever had the URL populated."""
    item_id = str(item_id_field or "").strip()
    if item_id:
        return item_id
    if listing_url:
        m = ITEM_ID_URL_RE.search(listing_url)
        if m:
            return m.group(1)
    return ""


def parse_price(value):
    """Used by Phase 3 to clean Airtable's Price Sold field, which may
    arrive as a raw number, a currency-formatted string, or blank."""
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


def get_supabase_headers():
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }


# ── Shared paginated loaders ────────────────────────────────────────────────
def load_all_supabase_rows(table, select, extra_params=None):
    page_size = 1000
    offset    = 0
    rows      = []
    while True:
        headers = get_supabase_headers()
        headers["Range-Unit"] = "items"
        headers["Range"]      = f"{offset}-{offset + page_size - 1}"
        params = {"select": select}
        if extra_params:
            params.update(extra_params)
        resp = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=headers, params=params, timeout=30)
        if not resp.ok:
            print(f"   ⚠️  Supabase load failed on {table} at offset {offset}: {resp.status_code}")
            break
        batch = resp.json()
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def load_all_airtable_sightings(fields, filter_formula=None):
    """No Status filter by default — fetches every sighting regardless of
    status, which is the actual fix for the sync-gap this script exists
    to close. Dedup against Supabase happens by id, not by status."""
    all_records = []
    params = {"fields[]": fields, "pageSize": 100}
    if filter_formula:
        params["filterByFormula"] = filter_formula
    while True:
        resp = requests.get(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{RESALE_SIGHTINGS_TABLE}",
            headers=HEADERS_AT, params=params
        )
        resp.raise_for_status()
        data = resp.json()
        all_records.extend(data.get("records", []))
        if not data.get("offset"):
            break
        params["offset"] = data["offset"]
    return all_records


# ── Shared end-state write path (used by Phase 3, and available to any ──────
# future caller) — the single place that ever sets end_reason/ended_reason
# for a confirmed sale, so the two columns can't drift apart again.
def mark_sold_listing(listing_id, sold_price, sold_at):
    payload = {"end_reason": "sold", "ended_reason": "sold"}
    if sold_price is not None:
        payload["sold_price"] = sold_price
    if sold_at:
        payload["sold_at"]  = sold_at
        payload["ended_at"] = f"{sold_at}T00:00:00+00:00"
    if DRY_RUN:
        print(f"   [DRY RUN] would mark listing {listing_id} sold: {payload}")
        return True
    resp = requests.patch(
        f"{SUPABASE_URL}/rest/v1/listings",
        headers=get_supabase_headers(),
        params={"id": f"eq.{listing_id}"},
        json=payload,
        timeout=15,
    )
    if not resp.ok:
        print(f"   ⚠️  Sold mark FAILED for {listing_id}: {resp.status_code} {resp.text[:150]}")
    return resp.ok


def create_sold_listing(item_id, url, garment_uuid, sold_price, sold_at,
                         listed_price, date_listed, condition, title, seller):
    payload = {
        "ebay_item_id": item_id,
        "listing_url":  url,            # VERBATIM — keeps EPN affiliate params intact
        "garment_id":   garment_uuid,
        "end_reason":   "sold",
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
        print(f"   [DRY RUN] would create sold listing {item_id}: {payload}")
        return True
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/listings",
        headers=get_supabase_headers(),
        json=payload,
        timeout=15,
    )
    if not resp.ok:
        print(f"   ⚠️  Sold create FAILED for {item_id}: {resp.status_code} {resp.text[:150]}")
    return resp.ok


# ══════════════════════════════════════════════════════════════════════════
# PHASE 1 — Sync: create any Supabase listings row that doesn't exist yet,
# for a sighting of ANY status (not just Active/Sold).
# ══════════════════════════════════════════════════════════════════════════

def reconcile_stale_listings(items):
    """Patch existing Supabase rows that are still open (ended_at null)
    despite Airtable already showing them Sold/Expired. One request per
    row rather than batching — these are individually distinct patches
    (different sold_price/sold_at per row), unlike mark_listings_ended's
    uniform batch update.

    Also backfills ebay_item_id when the row's stored value is null but
    this run derived one (via the dedicated field or the URL fallback) —
    this is what lets a previously-unmatchable row become matchable by
    future runs' Phase 2, instead of staying permanently stuck."""
    now = datetime.now(timezone.utc).isoformat()
    succeeded, failed = 0, 0

    for item in items:
        patch = {
            "ended_at": now,
            "end_reason": item["reason"],
            "ended_reason": item["reason"],
        }
        if item["reason"] == "sold":
            patch["sold_price"] = item["sold_price"]
            patch["sold_at"] = item["sold_at"] or now
        if item.get("backfill_item_id"):
            patch["ebay_item_id"] = item["backfill_item_id"]

        resp = requests.patch(
            f"{SUPABASE_URL}/rest/v1/listings",
            headers=get_supabase_headers(),
            params={"id": f"eq.{item['supabase_id']}"},
            json=patch,
            timeout=15,
        )
        if resp.ok:
            succeeded += 1
        else:
            failed += 1
            print(f"   ⚠️  Reconcile FAILED for {item['supabase_id']}: {resp.status_code} {resp.text[:150]}")
        time.sleep(0.05)

    return succeeded, failed


def reactivate_relisted_listings(items):
    """Handles TRUE relists — new_listings.py's relist detection matches
    an incoming eBay result on (seller, exact title) against an existing
    Airtable record, and deliberately overwrites that record in place
    with the new eBay Item ID/URL/price rather than creating a fresh
    record — this preserves the garment match and avoids re-running
    matching on every relist, which is the efficient behaviour we want
    to keep.

    But that means the matching Supabase row (found here via
    airtable_id, since ebay_item_id itself just changed) would otherwise
    keep pointing at the OLD, now-dead eBay Item ID forever — Phase 2
    can never find it again by the new ID to verify or close it out, and
    the prior price point vanishes with no trace it ever existed.

    This snapshots the row's old listed_price/started_at into
    price_history (appending to whatever's already there) before
    overwriting, then refreshes ebay_item_id/listing_url/listed_price/
    started_at to the new listing's values. If the row had already been
    closed out under the old ID (e.g. a previous run's Phase 2 marked it
    expired after the old listing vanished, not realising it was simply
    relisted), this also reopens it — ended_at/end_reason/ended_reason/
    sold_price/sold_at all cleared, since it's demonstrably active again
    under the new ID."""
    succeeded, failed = 0, 0
    for item in items:
        patch = {
            "ebay_item_id": item["new_item_id"],
            "listing_url":  item["new_url"],
            "listed_price": item["new_price"],
            "started_at":   item["new_started_at"],
            "price_history": item["price_history"],
        }
        if item["was_closed"]:
            patch.update({
                "ended_at": None,
                "end_reason": None,
                "ended_reason": None,
                "sold_price": None,
                "sold_at": None,
            })
        resp = requests.patch(
            f"{SUPABASE_URL}/rest/v1/listings",
            headers=get_supabase_headers(),
            params={"id": f"eq.{item['supabase_id']}"},
            json=patch,
            timeout=15,
        )
        if resp.ok:
            succeeded += 1
        else:
            failed += 1
            print(f"   ⚠️  Relist reactivation FAILED for {item['supabase_id']}: {resp.status_code} {resp.text[:150]}")
        time.sleep(0.05)
    return succeeded, failed


def backfill_ebay_item_ids(items):
    """Lightweight companion to reconcile_stale_listings — that function
    only backfills ebay_item_id as a side effect of closing out a
    Sold/Expired row. This handles the other half: a legacy row matched
    via airtable_id whose ebay_item_id is still null but whose CURRENT
    Airtable status is Active."""
    succeeded, failed = 0, 0
    for item in items:
        resp = requests.patch(
            f"{SUPABASE_URL}/rest/v1/listings",
            headers=get_supabase_headers(),
            params={"id": f"eq.{item['supabase_id']}"},
            json={"ebay_item_id": item["item_id"]},
            timeout=15,
        )
        if resp.ok:
            succeeded += 1
        else:
            failed += 1
            print(f"   ⚠️  ID backfill FAILED for {item['supabase_id']}: {resp.status_code} {resp.text[:150]}")
        time.sleep(0.05)
    return succeeded, failed


def insert_listings(rows):
    """Bulk insert in batches of 50. On batch failure, retry rows individually
    so one bad row doesn't sink the other 49 good ones."""
    inserted, errors, failed_rows = 0, 0, []

    for i in range(0, len(rows), 50):
        batch = rows[i:i + 50]
        resp = requests.post(f"{SUPABASE_URL}/rest/v1/listings", headers=get_supabase_headers(),
                              json=batch, timeout=30)
        if resp.ok:
            inserted += len(batch)
        else:
            print(f"   ⚠️  Batch failed ({resp.status_code}), retrying rows individually...")
            for row in batch:
                r = requests.post(f"{SUPABASE_URL}/rest/v1/listings", headers=get_supabase_headers(),
                                   json=[row], timeout=15)
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


def run_sync_phase():
    print("═" * 60)
    print("PHASE 1 — Sync (Airtable → Supabase, any status)")
    print("═" * 60)

    print("📊 Loading existing Supabase listings (for dedup + reconciliation)...")
    existing_listings = load_all_supabase_rows(
        "listings",
        "id,ebay_item_id,airtable_id,ended_at,listed_price,started_at,price_history"
    )
    existing_ids = {str(r["ebay_item_id"]) for r in existing_listings if r.get("ebay_item_id")}
    existing_airtable_ids = {r["airtable_id"] for r in existing_listings if r.get("airtable_id")}
    by_ebay_id = {str(r["ebay_item_id"]): r for r in existing_listings if r.get("ebay_item_id")}
    by_airtable_id = {r["airtable_id"]: r for r in existing_listings if r.get("airtable_id")}
    print(f"   {len(existing_ids)} existing listing IDs loaded")

    print("\n📊 Loading Supabase garments (for garment_id matching)...")
    garment_rows = load_all_supabase_rows("garments", "id,airtable_id")
    garment_lookup = {r["airtable_id"]: r["id"] for r in garment_rows if r.get("airtable_id")}
    print(f"   {len(garment_lookup)} garments loaded")

    print("\n📦 Loading ALL Airtable sightings (no status filter)...")
    sightings = load_all_airtable_sightings(fields=[
        "eBay Item ID", "Listing URL", "eBay Title", "Seller Name",
        "Condition", "Listed Price", "Date Listed", "Status", "Date Sold",
        "Garment",
    ])
    print(f"   {len(sightings)} total sightings")

    to_insert = []
    already_synced = already_by_at_id = no_item_id = no_garment_match = 0
    backfilled_ended = 0
    to_reconcile = []
    to_backfill_id_only = []
    to_reactivate = []  # TRUE relists — same Airtable record, new eBay Item ID

    run_now_iso = datetime.now(timezone.utc).isoformat()

    for rec in sightings:
        f = rec["fields"]
        listing_url = f.get("Listing URL", "") or ""
        item_id = extract_item_id(f.get("eBay Item ID"), listing_url)
        status = f.get("Status")

        if not item_id:
            no_item_id += 1
            continue
        if item_id in existing_ids:
            already_synced += 1
            if status in ("Sold", "Expired"):
                existing_row = by_ebay_id.get(item_id)
                if existing_row and existing_row.get("ended_at") is None:
                    to_reconcile.append({
                        "supabase_id": existing_row["id"],
                        "reason": "sold" if status == "Sold" else "expired",
                        "sold_price": f.get("Listed Price") if status == "Sold" else None,
                        "sold_at": f.get("Date Sold") if status == "Sold" else None,
                    })
            continue
        if rec["id"] in existing_airtable_ids:
            already_by_at_id += 1
            existing_row = by_airtable_id.get(rec["id"])
            if status in ("Sold", "Expired"):
                if existing_row and existing_row.get("ended_at") is None:
                    needs_backfill = not existing_row.get("ebay_item_id")
                    to_reconcile.append({
                        "supabase_id": existing_row["id"],
                        "reason": "sold" if status == "Sold" else "expired",
                        "sold_price": f.get("Listed Price") if status == "Sold" else None,
                        "sold_at": f.get("Date Sold") if status == "Sold" else None,
                        "backfill_item_id": item_id if needs_backfill else None,
                    })
            elif existing_row and not existing_row.get("ebay_item_id"):
                to_backfill_id_only.append({
                    "supabase_id": existing_row["id"],
                    "item_id": item_id,
                })
            elif status == "Active" and existing_row:
                stored_item_id = str(existing_row.get("ebay_item_id") or "")
                if stored_item_id and item_id and stored_item_id != item_id:
                    # TRUE RELIST: new_listings.py's relist detection matched
                    # this Airtable record by (seller, title) and overwrote it
                    # in place with a new eBay Item ID — the underlying
                    # listing genuinely changed, it's not just a field edit.
                    # Snapshot the old price/date before it's gone.
                    old_history = existing_row.get("price_history") or []
                    if existing_row.get("listed_price") is not None:
                        old_history = old_history + [{
                            "date": existing_row.get("started_at") or run_now_iso,
                            "price": existing_row.get("listed_price"),
                        }]
                    to_reactivate.append({
                        "supabase_id": existing_row["id"],
                        "new_item_id": item_id,
                        "new_url": listing_url,
                        "new_price": f.get("Listed Price"),
                        "new_started_at": f.get("Date Listed") or None,
                        "price_history": old_history,
                        "was_closed": existing_row.get("ended_at") is not None,
                    })
            continue

        linked_garments = f.get("Garment", [])
        garment_airtable_id = linked_garments[0] if linked_garments else None
        garment_id = garment_lookup.get(garment_airtable_id) if garment_airtable_id else None
        if not garment_id:
            no_garment_match += 1

        now_iso = datetime.now(timezone.utc).isoformat()

        row = {
            "airtable_id":  rec["id"],
            "garment_id":   garment_id,
            "ebay_item_id": item_id,
            "title":        f.get("eBay Title", "") or "",
            "listing_url":  listing_url,
            "seller_name":  f.get("Seller Name", "") or "",
            "condition":    f.get("Condition", "") or "",
            "listed_price": f.get("Listed Price"),
            "started_at":   f.get("Date Listed") or None,
            "sold_price":   None,
            "sold_at":      None,
            "ended_at":     None,
            "end_reason":   None,
            "ended_reason": None,
        }

        if status == "Sold":
            row["sold_price"]   = f.get("Listed Price")
            row["sold_at"]      = f.get("Date Sold") or None
            row["ended_at"]     = f.get("Date Sold") or now_iso
            row["end_reason"]   = "sold"
            row["ended_reason"] = "sold"
            backfilled_ended += 1
        elif status == "Expired":
            row["ended_at"]     = now_iso
            row["end_reason"]   = "expired"
            row["ended_reason"] = "expired"
            backfilled_ended += 1

        to_insert.append(row)
        existing_ids.add(item_id)
        existing_airtable_ids.add(rec["id"])

    print(f"\n   Already synced (by eBay ID): {already_synced}")
    print(f"   Already synced (by airtable_id): {already_by_at_id}")
    print(f"   Skipped — no eBay ID (field blank AND unparseable from URL): {no_item_id}")
    print(f"   No garment match (inserted anyway): {no_garment_match}")
    print(f"   New rows, already Sold/Expired at first sync (ended_at backfilled): {backfilled_ended}")
    print(f"   Existing rows needing reconciliation (Airtable already resolved, Supabase still open): {len(to_reconcile)}")
    print(f"   Existing rows relisted under a new eBay Item ID: {len(to_reactivate)}")
    print(f"   Existing Active rows needing ebay_item_id backfill only: {len(to_backfill_id_only)}")
    print(f"   🔧 New listings to insert: {len(to_insert)}")

    if to_insert:
        if DRY_RUN:
            print(f"\n🧪 DRY RUN — would insert {len(to_insert)} new listings. No writes made.")
            for row in to_insert[:5]:
                print(f"   • {row['ebay_item_id']} | {row['title'][:50]!r} | "
                      f"garment_id={row['garment_id']} | end_reason={row['end_reason']}")
        else:
            print(f"\n📝 Inserting {len(to_insert)} new listings into Supabase...")
            inserted, errors = insert_listings(to_insert)
            print(f"   {inserted} inserted | {errors} errors")
    else:
        print("\n   Nothing new to insert.")

    if to_reconcile:
        if DRY_RUN:
            print(f"\n🧪 DRY RUN — would reconcile {len(to_reconcile)} stale existing rows. No writes made.")
            for r in to_reconcile[:5]:
                tag = " (+ backfilling ebay_item_id)" if r.get("backfill_item_id") else ""
                print(f"   • supabase_id={r['supabase_id']} -> reason={r['reason']}{tag}")
        else:
            print(f"\n📝 Reconciling {len(to_reconcile)} stale existing rows "
                  f"(Airtable already resolved, Supabase never closed out)...")
            reconciled, recon_errors = reconcile_stale_listings(to_reconcile)
            print(f"   {reconciled} reconciled | {recon_errors} errors")
    else:
        print("\n   No stale existing rows to reconcile.")

    if to_backfill_id_only:
        if DRY_RUN:
            print(f"\n🧪 DRY RUN — would backfill ebay_item_id on {len(to_backfill_id_only)} Active rows. No writes made.")
        else:
            print(f"\n📝 Backfilling ebay_item_id on {len(to_backfill_id_only)} still-Active legacy rows...")
            bf_succeeded, bf_failed = backfill_ebay_item_ids(to_backfill_id_only)
            print(f"   {bf_succeeded} succeeded | {bf_failed} FAILED")
    else:
        print("\n   No Active rows needing ID-only backfill.")

    if to_reactivate:
        if DRY_RUN:
            print(f"\n🧪 DRY RUN — would reactivate {len(to_reactivate)} relisted rows under new eBay Item IDs. No writes made.")
            for r in to_reactivate[:5]:
                print(f"   • supabase_id={r['supabase_id']} -> new item_id={r['new_item_id']} "
                      f"(price history now has {len(r['price_history'])} point(s))")
        else:
            print(f"\n📝 Reactivating {len(to_reactivate)} relisted rows under new eBay Item IDs...")
            react_succeeded, react_failed = reactivate_relisted_listings(to_reactivate)
            print(f"   {react_succeeded} reactivated | {react_failed} FAILED")
    else:
        print("\n   No relisted rows needing reactivation.")


# ══════════════════════════════════════════════════════════════════════════
# PHASE 2 — Expiry check: verify currently-Active sightings against eBay,
# close out anything that's actually ended.
# ══════════════════════════════════════════════════════════════════════════

def mark_listings_ended(listing_ids, reason):
    if not listing_ids:
        return 0, 0
    now = datetime.now(timezone.utc).isoformat()
    succeeded, failed = 0, 0
    for i in range(0, len(listing_ids), 50):
        batch = listing_ids[i:i+50]
        id_list = ",".join(batch)
        resp = requests.patch(
            f"{SUPABASE_URL}/rest/v1/listings",
            headers=get_supabase_headers(),
            params={"id": f"in.({id_list})", "or": "(ended_reason.is.null,ended_reason.neq.sold)"},
            json={"ended_at": now, "end_reason": reason, "ended_reason": reason},
            timeout=15,
        )
        if resp.ok:
            succeeded += len(batch)
        else:
            failed += len(batch)
            print(f"   ⚠️  Supabase batch mark-ended FAILED for {len(batch)} rows: "
                  f"{resp.status_code} {resp.text[:200]}")
    return succeeded, failed


def get_ebay_token():
    credentials = base64.b64encode(f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}".encode()).decode()
    resp = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={"Authorization": f"Basic {credentials}", "Content-Type": "application/x-www-form-urlencoded"},
        data="grant_type=client_credentials&scope=https://api.ebay.com/oauth/api_scope",
        timeout=15
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"Token fetch failed: {resp.text}")
    print("   ✅ eBay token refreshed")
    return token


def check_listing_status(item_id, ebay_headers):
    try:
        resp = requests.get(f"https://api.ebay.com/buy/browse/v1/item/v1|{item_id}|0",
                             headers=ebay_headers, timeout=15)
        if resp.status_code == 404:
            return "Expired"
        if resp.status_code == 200:
            data = resp.json()
            end_date_raw = data.get("itemEndDate", "")
            if end_date_raw:
                end_dt = datetime.fromisoformat(end_date_raw.replace("Z", "+00:00"))
                if end_dt < datetime.now(timezone.utc):
                    return "Expired"
            availabilities = data.get("estimatedAvailabilities", [])
            if availabilities:
                avail = availabilities[0]
                status = avail.get("estimatedAvailabilityStatus", "")
                qty_sold = int(avail.get("estimatedSoldQuantity", 0))
                qty_remaining = int(avail.get("estimatedRemainingQuantity", 1))
                if status == "SOLD_OUT" or qty_sold > 0 or qty_remaining == 0:
                    return "Expired"
                if status in ("UNAVAILABLE", "OUT_OF_STOCK"):
                    return "Expired"
                if status == "IN_STOCK":
                    return "Active"
            if data.get("buyingOptions"):
                return "Active"
            return "Expired"
    except Exception:
        pass
    return "Active"


def push_updates(ended_ids, still_active_ids):
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url_at  = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{RESALE_SIGHTINGS_TABLE}"
    expired_updates = [{"id": rid, "fields": {"Status": "Expired", "Last Checked": now_iso}} for rid in ended_ids]
    active_updates  = [{"id": rid, "fields": {"Last Checked": now_iso}} for rid in still_active_ids]
    errors = 0

    print(f"   Updating {len(expired_updates)} expired...")
    for i in range(0, len(expired_updates), 10):
        resp = requests.patch(url_at, headers=HEADERS_AT, json={"records": expired_updates[i:i+10]})
        if not resp.ok:
            errors += 1
            if errors <= 3:
                print(f"   ⚠️  Batch error: {resp.status_code} {resp.text[:80]}")
        time.sleep(0.1)

    print(f"   Updating {len(active_updates)} still active...")
    for i in range(0, len(active_updates), 10):
        resp = requests.patch(url_at, headers=HEADERS_AT, json={"records": active_updates[i:i+10]})
        if not resp.ok:
            errors += 1
        time.sleep(0.1)

    return len(expired_updates) + len(active_updates), errors


def run_expiry_check_phase():
    print("\n" + "═" * 60)
    print("PHASE 2 — Expiry check (verify Active sightings against eBay)")
    print("═" * 60)

    print("🔑 Fetching eBay OAuth token...")
    ebay_token = get_ebay_token()
    ebay_headers = {"Authorization": f"Bearer {ebay_token}", "X-EBAY-C-MARKETPLACE-ID": "EBAY_AU"}

    print("\n📊 Loading active listings from Supabase...")
    sb_active = {}
    sb_all_ever = set()
    rows = load_all_supabase_rows("listings", "id,ebay_item_id,ended_at")
    for r in rows:
        eid = r.get("ebay_item_id")
        if not eid:
            continue
        eid = str(eid)
        sb_all_ever.add(eid)
        if r.get("ended_at") is None:
            sb_active[eid] = r["id"]
    sb_listings = sb_active
    print(f"   {len(sb_active)} active listings loaded ({len(sb_all_ever)} total rows including already-ended)")

    print("\n📦 Loading active sightings from Airtable...")
    recheck_cutoff = (datetime.now(timezone.utc) - timedelta(days=RECHECK_DAYS)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    all_sightings = load_all_airtable_sightings(
        fields=["Listing URL", "eBay Item ID", "Date Listed", "Status", "Last Checked"],
        filter_formula=(f'AND(Status = "Active", '
                         f'OR({{Last Checked}} = "", IS_BEFORE({{Last Checked}}, "{recheck_cutoff}")))'),
    )
    print(f"   {len(all_sightings)} sightings due for checking")

    fresh_cutoff = datetime.now(timezone.utc) - timedelta(days=MIN_AGE_DAYS)
    to_check, skipped_fresh, skipped_no_id = [], 0, 0

    for rec in all_sightings:
        if len(to_check) >= BATCH_LIMIT:
            break
        f = rec["fields"]
        url = f.get("Listing URL", "") or ""
        item_id = extract_item_id(f.get("eBay Item ID"), url)
        if not item_id:
            skipped_no_id += 1
            continue
        date_listed = f.get("Date Listed")
        if date_listed:
            try:
                if datetime.fromisoformat(date_listed.replace("Z", "+00:00")) > fresh_cutoff:
                    skipped_fresh += 1
                    continue
            except Exception:
                pass
        to_check.append({"id": rec["id"], "item_id": item_id})

    print(f"   {len(to_check)} to check | {skipped_fresh} too fresh | {skipped_no_id} no ID/URL\n")

    print("🔍 Checking eBay listing status...")
    ended_ids, ended_sb_ids, still_active_ids, unmatched = [], [], [], []

    for i, rec in enumerate(to_check):
        status = check_listing_status(rec["item_id"], ebay_headers)
        if status == "Expired":
            ended_ids.append(rec["id"])
            sb_id = sb_listings.get(rec["item_id"])
            if sb_id:
                ended_sb_ids.append(sb_id)
            elif rec["item_id"] in sb_all_ever:
                print(f"   ℹ️  {rec['item_id']!r} already closed out in Supabase previously "
                      f"— Airtable Status was just stale, now corrected (Airtable rec {rec['id']})")
            else:
                print(f"   ⚠️  No Supabase match for item_id={rec['item_id']!r} "
                      f"(Airtable rec {rec['id']}) — genuinely never existed, worth investigating")
                unmatched.append({"airtable_record_id": rec["id"], "ebay_item_id": rec["item_id"]})
        else:
            still_active_ids.append(rec["id"])
        time.sleep(0.3)
        if (i + 1) % 100 == 0:
            print(f"   ... {i + 1}/{len(to_check)} — expired: {len(ended_ids)}")

    print(f"\n   Expired: {len(ended_ids)} | Still active: {len(still_active_ids)}")

    if ended_sb_ids and not DRY_RUN:
        print(f"\n📊 Marking {len(ended_sb_ids)} listings ended in Supabase...")
        sb_succeeded, sb_failed = mark_listings_ended(ended_sb_ids, "expired")
        print(f"   {sb_succeeded} succeeded | {sb_failed} FAILED")

    if unmatched:
        with open(UNMATCHED_REPORT_PATH, "w") as fp:
            json.dump({"unmatched": unmatched, "count": len(unmatched)}, fp, indent=2)
        print(f"\n⚠️  {len(unmatched)} unmatched even after Phase 1 — written to {UNMATCHED_REPORT_PATH}, "
              f"worth investigating individually (different root cause than the original gap)")

    if not DRY_RUN:
        print(f"\n📝 Applying updates to Airtable...")
        total_updated, err_count = push_updates(ended_ids, still_active_ids)
        print(f"   {total_updated} records updated | {err_count} batch errors")
    else:
        print(f"\n🧪 DRY RUN — would update {len(ended_ids) + len(still_active_ids)} Airtable records")


# ══════════════════════════════════════════════════════════════════════════
# PHASE 3 — Sold verification: Airtable is the source of truth for confirmed
# sales. You verify a sale by hand, set Status = "Sold", and fill in
# Price Sold + Date Sold. This is the ONLY phase that ever has a hand-
# verified achieved price to work with — Phase 1/2 only ever guess
# "expired" when a listing vanishes from eBay, since the Browse API can't
# tell sold apart from just-ended.
#
# Two passes:
#   MATCH-UPDATE (always): for every Sold sighting whose listing was
#     already scraped active, find the Supabase row by ebay_item_id and
#     overwrite Phase 2's earlier "expired" guess with the real outcome —
#     via mark_sold_listing(), the shared write path that sets BOTH
#     end_reason and ended_reason together.
#   CREATE-MISSING (when CREATE_MISSING=true): many sold comps were never
#     scraped active (the sale was found directly, already completed).
#     Those have no listings row yet — this pass inserts one, joined to
#     the right garment, so the achieved price feeds per-garment stats.
#     Rows that can't resolve a garment are REPORTED as orphans, not
#     inserted.
# ══════════════════════════════════════════════════════════════════════════

def load_sold_sightings():
    return load_all_airtable_sightings(
        fields=[
            "eBay Item ID", "Listing URL", "Status", F_PRICE_SOLD, "Date Sold",
            "Condition", "Garment", "Listed Price", "Date Listed", "eBay Title", "Seller Name",
        ],
        filter_formula='{Status}="Sold"',
    )


def run_sold_sync_phase():
    print("\n" + "═" * 60)
    print("PHASE 3 — Sold verification (Airtable hand-verified sales → Supabase)")
    print("═" * 60)
    if CREATE_MISSING:
        print("   CREATE_MISSING enabled — will also insert rows for sold comps never scraped active")

    print("\n📊 Loading Supabase listing map...")
    listing_rows = load_all_supabase_rows("listings", "id,ebay_item_id")
    listing_map = {str(r["ebay_item_id"]).strip(): r["id"] for r in listing_rows if r.get("ebay_item_id")}
    print(f"   {len(listing_map)} listings with an eBay item ID")

    garment_map = {}
    if CREATE_MISSING:
        print("\n📊 Loading Supabase garment map...")
        garment_rows = load_all_supabase_rows("garments", "id,airtable_id")
        garment_map = {str(r["airtable_id"]).strip(): r["id"] for r in garment_rows if r.get("airtable_id")}
        print(f"   {len(garment_map)} garments with an airtable_id")

    print("\n📦 Loading sold sightings from Airtable...")
    sold = load_sold_sightings()
    print(f"   {len(sold)} sightings marked Sold")

    updated = no_price = unmatched = no_id = errors = 0
    created = created_no_price = orphaned = 0
    created_ids = set()
    unmatched_samples, orphan_samples = [], []

    for rec in sold:
        f = rec.get("fields", {})
        item_id = extract_item_id(f.get("eBay Item ID"), f.get("Listing URL", ""))
        if not item_id:
            no_id += 1
            continue

        # Pass 1: update an existing listing
        if item_id in listing_map:
            price   = parse_price(f.get(F_PRICE_SOLD))
            sold_at = parse_date(f.get("Date Sold"))
            if price is None:
                no_price += 1
            if mark_sold_listing(listing_map[item_id], price, sold_at):
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
                unmatched_samples.append((item_id, f.get("Listing URL", "")))
            continue

        garment_uuid = garment_map.get(str(first_linked(f.get("Garment")) or "").strip())
        if not garment_uuid:
            orphaned += 1
            if len(orphan_samples) < 15:
                orphan_samples.append((item_id, f.get("eBay Title", ""), first_linked(f.get("Garment"))))
            continue
        if item_id in created_ids:
            continue

        price   = parse_price(f.get(F_PRICE_SOLD))
        sold_at = parse_date(f.get("Date Sold"))
        listed  = parse_price(f.get("Listed Price"))
        dlisted = parse_date(f.get("Date Listed"))
        if price is None:
            created_no_price += 1

        if create_sold_listing(
            item_id, f.get("Listing URL"), garment_uuid, price, sold_at,
            listed, dlisted, f.get("Condition"), f.get("eBay Title"), f.get("Seller Name"),
        ):
            created += 1
            created_ids.add(item_id)
            listing_map[item_id] = "new"   # guard against in-run dupes
        else:
            errors += 1
        if not DRY_RUN:
            time.sleep(0.04)

    print("\n" + "-" * 60)
    print(f"   Updated existing -> sold:  {updated}")
    print(f"     of which no price:       {no_price}")
    if CREATE_MISSING:
        print(f"   Created new sold rows:     {created}")
        print(f"     of which no price:       {created_no_price}")
        print(f"   Orphaned (no garment):     {orphaned}  (reported, NOT inserted)")
    else:
        print(f"   No Supabase listing:       {unmatched}  (set CREATE_MISSING=true to insert)")
    print(f"   No item ID/URL:            {no_id}")
    print(f"   Errors:                    {errors}")

    if unmatched_samples:
        print("\n   Sample unmatched:")
        for iid, url in unmatched_samples:
            print(f"      {iid}  {url}")
    if orphan_samples:
        print("\n   Sample orphans (sold, but garment not in Supabase — eyeball for junk):")
        for iid, title, gid in orphan_samples:
            print(f"      {iid}  garment={gid}  {str(title)[:60]}")


# ══════════════════════════════════════════════════════════════════════════
# PHASE 4 — Recompute garment aggregate stats: active_listing_count,
# listed_price_median, listed_price_min, listed_price_max on the
# `garments` table. These are read by the campaign embed's resale pill
# and by search_garments()'s ranking tie-break, but nothing has ever
# recalculated them after each garment row's initial creation (confirmed:
# no Postgres function beyond search_garments(), no triggers on any
# table, pg_cron not even enabled). Calls a single Postgres function
# (recompute_garment_stats(), defined in recompute_garment_stats.sql —
# run that once manually before this script's first run with this phase)
# so the aggregation itself happens inside the database rather than
# pulling every listings row into Python to compute a median.
# ══════════════════════════════════════════════════════════════════════════

def run_recompute_stats_phase():
    print("\n" + "═" * 60)
    print("PHASE 4 — Recompute garment aggregate stats")
    print("═" * 60)

    if DRY_RUN:
        print("   🧪 DRY RUN — skipping recompute (this phase only ever reads/aggregates "
              "live listings data, nothing to preview differently in dry-run mode)")
        return

    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/recompute_garment_stats",
        headers=get_supabase_headers(),
        json={},
        timeout=60,
    )
    if resp.ok:
        print("   ✅ Garment aggregate stats recomputed (active_listing_count, "
              "listed_price_median, listed_price_min, listed_price_max)")
    else:
        print(f"   ⚠️  Recompute FAILED: {resp.status_code} {resp.text[:300]}")
        print("   NOTE: if this is the first run with this phase, confirm you've run "
              "recompute_garment_stats.sql once in the Supabase SQL editor first — "
              "this call will 404/error if that function doesn't exist yet.")


# ══════════════════════════════════════════════════════════════════════════
def main():
    run_start = datetime.now(timezone.utc)
    print(f"🔄 Filted — Listings Sync + Expiry Check + Sold Verification (consolidated)")
    print(f"   {run_start.strftime('%Y-%m-%d %H:%M UTC')}")
    if DRY_RUN:
        print("   🧪 DRY RUN MODE — no writes will be made")
    print()

    run_sync_phase()
    run_expiry_check_phase()
    run_sold_sync_phase()
    run_recompute_stats_phase()

    elapsed = (datetime.now(timezone.utc) - run_start).seconds
    print(f"\n✅ Done in {elapsed}s")


if __name__ == "__main__":
    main()
