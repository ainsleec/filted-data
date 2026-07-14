#!/usr/bin/env python3
"""
Filted — Listings Sync + eBay Expiry Checker (consolidated)

Was two separate scripts (listings_sync.py, expiry_checker.py) on
independent schedules. Merged into one, run in strict sequence within a
single job, to close a real gap: if a listing's entire active life on
eBay was shorter than the time between the two scripts' separate runs,
it could flip from Active to Expired in Airtable BEFORE listings_sync
ever synced it — and since listings_sync only fetched Active/Sold
sightings, that record fell permanently outside its query. No Supabase
row ever got created, expiry_checker had nothing to close out, and that
listing's resale price data was silently lost forever (it still powers
price history on garment pages).

Two structural fixes, both needed together:
  1. The sync phase now fetches ALL sightings (no Status filter) and
     relies purely on dedup-by-id, so a sighting can never fall outside
     the query window based on its current status.
  2. Any sighting synced for the first time in a NON-Active state (Sold
     or Expired) gets ended_at/end_reason set immediately at insert —
     previously only sold_price/sold_at were set for Sold rows, meaning
     a freshly-synced Sold listing would still count as "active" in
     Supabase (ended_at IS NULL) until something else closed it out,
     which nothing did.

THIRD fix (this revision) — the "no eBay Item ID field" gap:
  Some Airtable sightings have their dedicated "eBay Item ID" field
  blank, with the ID only present inside the Listing URL. Phase 1 used
  to require the dedicated field and skip (no_item_id++) anything
  without it — permanently, since that skip happens BEFORE any
  reconciliation-by-airtable_id check. Phase 2 already had a URL regex
  fallback for exactly this case, which is how it could still correctly
  detect and mark these as Expired in Airtable — but it could never
  close out the matching Supabase row, because rows with
  ebay_item_id IS NULL get filtered out of sb_listings/sb_all_ever
  entirely when Phase 2 builds its lookup dicts. Net effect: any
  sighting synced without its eBay Item ID field populated was stuck
  showing "active" on the site forever, with no path back to correct
  — Phase 1 skipped it going in, Phase 2 couldn't find it coming back.
  Fixed by: (a) giving Phase 1 the same URL-fallback Phase 2 already
  had, and (b) having reconciliation backfill ebay_item_id onto the
  Supabase row whenever it derives one, so the row becomes matchable
  by future runs even if this run only gets it partially fixed.

FOURTH fix (this revision) — the end_reason/ended_reason split:
  The listings table has two separate columns: `end_reason` (read by
  the Worker's /garment-history endpoint — the one the frontend
  actually displays) and `ended_reason` (a leftover from an earlier
  schema iteration, read by nothing). This script was writing
  exclusively to `ended_reason`, so every "why did this end" value was
  going into a column the site never looks at. Now writes to both —
  `end_reason` because it's the one that matters, `ended_reason` kept
  in sync too in case anything still references it, until that column
  is confirmed safe to drop entirely.

Run order within main(): sync first, then expiry-check — so every run
starts from as complete a picture as this pass can make it, before
checking what's still genuinely active on eBay.

Required env vars:
  AIRTABLE_TOKEN, AIRTABLE_BASE, RESALE_SIGHTINGS_TABLE
  EBAY_CLIENT_ID, EBAY_CLIENT_SECRET
  SUPABASE_URL, SUPABASE_SERVICE_KEY
"""

import os, re, sys, json, time, base64, requests
from datetime import datetime, timezone, timedelta

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true" or "--dry-run" in sys.argv

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


def extract_item_id(item_id_field, listing_url):
    """Shared by both phases now. Prefer the dedicated field; fall back
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

    Now also backfills ebay_item_id when the row's stored value is null
    but this run derived one (via the dedicated field or the URL
    fallback) — this is what lets a previously-unmatchable row become
    matchable by future runs' Phase 2, instead of staying permanently
    stuck the way the Sahara Blouse listing was."""
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
    existing_listings = load_all_supabase_rows("listings", "id,ebay_item_id,airtable_id,ended_at")
    existing_ids = {str(r["ebay_item_id"]) for r in existing_listings if r.get("ebay_item_id")}
    existing_airtable_ids = {r["airtable_id"] for r in existing_listings if r.get("airtable_id")}
    # For reconciliation: existing row's Supabase id + whether it's still
    # open (ended_at is null), keyed both ways since a sighting might be
    # matched by either id depending on which existed first.
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
    backfilled_ended = 0  # sightings synced for the first time already Sold/Expired
    to_reconcile = []  # existing Supabase rows that are stale — Airtable already
                        # resolved (Sold/Expired) but Supabase still shows ended_at=null.

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
            if status in ("Sold", "Expired"):
                existing_row = by_airtable_id.get(rec["id"])
                if existing_row and existing_row.get("ended_at") is None:
                    # This is the path that used to be unreachable for
                    # rows whose eBay Item ID field was blank — item_id
                    # here may have come from the URL fallback above,
                    # and the existing row's own ebay_item_id may still
                    # be null, so backfill it here.
                    needs_backfill = not existing_row.get("ebay_item_id")
                    to_reconcile.append({
                        "supabase_id": existing_row["id"],
                        "reason": "sold" if status == "Sold" else "expired",
                        "sold_price": f.get("Listed Price") if status == "Sold" else None,
                        "sold_at": f.get("Date Sold") if status == "Sold" else None,
                        "backfill_item_id": item_id if needs_backfill else None,
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
            # We're seeing this late — the true end date isn't known, so
            # "now" is the best available timestamp. Still infinitely
            # better than the row never existing at all (the old bug).
            row["ended_at"]     = now_iso
            row["end_reason"]   = "expired"
            row["ended_reason"] = "expired"
            backfilled_ended += 1
        # Active (or any other/unknown status) → all ended_* stay None,
        # correctly treated as currently active.

        to_insert.append(row)
        existing_ids.add(item_id)
        existing_airtable_ids.add(rec["id"])

    print(f"\n   Already synced (by eBay ID): {already_synced}")
    print(f"   Already synced (by airtable_id): {already_by_at_id}")
    print(f"   Skipped — no eBay ID (field blank AND unparseable from URL): {no_item_id}")
    print(f"   No garment match (inserted anyway): {no_garment_match}")
    print(f"   New rows, already Sold/Expired at first sync (ended_at backfilled): {backfilled_ended}")
    print(f"   Existing rows needing reconciliation (Airtable already resolved, Supabase still open): {len(to_reconcile)}")
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
    sb_all_ever = set()  # every ebay_item_id that has ANY Supabase row, active or already-ended
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
                # A Supabase row DOES exist for this — it was just already
                # closed out at some point before this run (ended_at
                # already set). Airtable's Status field was simply stale
                # until now; this run's push_updates below correctly
                # fixes that. Nothing actually missing, no action needed.
                print(f"   ℹ️  {rec['item_id']!r} already closed out in Supabase previously "
                      f"— Airtable Status was just stale, now corrected (Airtable rec {rec['id']})")
            else:
                # Genuinely no Supabase row at all, even after Phase 1 —
                # this is the real, still-unexplained gap worth digging
                # into (e.g. an eBay Item ID mismatch between systems).
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
def main():
    run_start = datetime.now(timezone.utc)
    print(f"🔄 Filted — Listings Sync + Expiry Check (consolidated)")
    print(f"   {run_start.strftime('%Y-%m-%d %H:%M UTC')}")
    if DRY_RUN:
        print("   🧪 DRY RUN MODE — no writes will be made")
    print()

    run_sync_phase()
    run_expiry_check_phase()

    elapsed = (datetime.now(timezone.utc) - run_start).seconds
    print(f"\n✅ Done in {elapsed}s")


if __name__ == "__main__":
    main()
