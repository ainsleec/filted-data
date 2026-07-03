#!/usr/bin/env python3
"""
Filted — eBay Expiry Checker + Supabase Price Tracker
Runs via GitHub Actions once daily.
Checks 2000 Active sightings per run against eBay Browse API.
Updates status to Expired in Airtable and stamps ended_at in Supabase.
"""

import os, re, requests, time, base64
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN         = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE          = os.environ["AIRTABLE_BASE"]
RESALE_SIGHTINGS_TABLE = os.environ["RESALE_SIGHTINGS_TABLE"]

EBAY_CLIENT_ID     = os.environ["EBAY_CLIENT_ID"]
EBAY_CLIENT_SECRET = os.environ["EBAY_CLIENT_SECRET"]

SUPABASE_URL         = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

MIN_AGE_DAYS  = 1
RECHECK_DAYS  = 1
BATCH_LIMIT   = 2000

HEADERS_AT = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type":  "application/json"
}


# ── Supabase helpers ──────────────────────────────────────────────────────────
def get_supabase_headers():
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }


def load_supabase_active_listings():
    """Load all active Supabase listings in one call for ended_at stamping."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("   ⚠️  Supabase not configured — skipping")
        return {}
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/listings",
        headers=get_supabase_headers(),
        params={"select": "id,ebay_item_id", "ended_at": "is.null"},
        timeout=30
    )
    if not resp.ok:
        print(f"   ⚠️  Supabase load failed: {resp.status_code}")
        return {}
    rows = resp.json()
    return {str(r["ebay_item_id"]): r["id"] for r in rows if r.get("ebay_item_id")}


def mark_listings_ended(listing_ids, reason):
    """Batch mark multiple listings as ended in one Supabase call per batch."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY or not listing_ids:
        return
    now = datetime.now(timezone.utc).isoformat()
    # Supabase doesn't support bulk patch by id list easily, do in small batches
    for i in range(0, len(listing_ids), 50):
        batch = listing_ids[i:i+50]
        id_list = ",".join(batch)
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/listings",
            headers=get_supabase_headers(),
            params={
                "id": f"in.({id_list})",
                "or": "(ended_reason.is.null,ended_reason.neq.sold)",  # never clobber a confirmed sale
            },
            json={"ended_at": now, "ended_reason": reason},
            timeout=15,
        )


# ── eBay OAuth token ──────────────────────────────────────────────────────────
def get_ebay_token():
    credentials = base64.b64encode(
        f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}".encode()
    ).decode()
    resp = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type":  "application/x-www-form-urlencoded"
        },
        data="grant_type=client_credentials&scope=https://api.ebay.com/oauth/api_scope",
        timeout=15
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"Token fetch failed: {resp.text}")
    print("   ✅ eBay token refreshed")
    return token


# ── eBay status check ─────────────────────────────────────────────────────────
def check_listing_status(item_id, ebay_headers):
    try:
        resp = requests.get(
            f"https://api.ebay.com/buy/browse/v1/item/v1|{item_id}|0",
            headers=ebay_headers, timeout=15
        )
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
                avail         = availabilities[0]
                status        = avail.get("estimatedAvailabilityStatus", "")
                qty_sold      = int(avail.get("estimatedSoldQuantity", 0))
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


# ── Airtable helpers ──────────────────────────────────────────────────────────
def load_active_sightings():
    recheck_cutoff = (
        datetime.now(timezone.utc) - timedelta(days=RECHECK_DAYS)
    ).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    all_sightings = []
    params = {
        "fields[]": ["Listing URL", "eBay Item ID", "Date Listed", "Status", "Last Checked"],
        "filterByFormula": (
            f'AND(Status = "Active", '
            f'OR({{Last Checked}} = "", IS_BEFORE({{Last Checked}}, "{recheck_cutoff}")))'
        ),
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


def push_updates(ended_ids, still_active_ids):
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url_at  = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{RESALE_SIGHTINGS_TABLE}"

    # Expired first — most important
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
        if (i // 10) % 20 == 0 and i > 0:
            print(f"   ... {min(i+10, len(expired_updates))}/{len(expired_updates)} expired updated")
        time.sleep(0.1)

    print(f"   Updating {len(active_updates)} still active...")
    for i in range(0, len(active_updates), 10):
        resp = requests.patch(url_at, headers=HEADERS_AT, json={"records": active_updates[i:i+10]})
        if not resp.ok:
            errors += 1
        if (i // 10) % 50 == 0 and i > 0:
            print(f"   ... {min(i+10, len(active_updates))}/{len(active_updates)} active updated")
        time.sleep(0.1)

    return len(expired_updates) + len(active_updates), errors


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    run_start = datetime.now(timezone.utc)
    print(f"🔄 Filted — eBay Expiry Checker")
    print(f"   {run_start.strftime('%Y-%m-%d %H:%M UTC')}\n")

    print("🔑 Fetching eBay OAuth token...")
    ebay_token = get_ebay_token()
    ebay_headers = {
        "Authorization":           f"Bearer {ebay_token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_AU"
    }

    print("\n📊 Loading active listings from Supabase...")
    sb_listings = load_supabase_active_listings()
    print(f"   {len(sb_listings)} active listings loaded")

    print("\n📦 Loading active sightings from Airtable...")
    all_sightings = load_active_sightings()
    print(f"   {len(all_sightings)} sightings due for checking")

    fresh_cutoff = datetime.now(timezone.utc) - timedelta(days=MIN_AGE_DAYS)
    to_check, skipped_fresh, skipped_no_id = [], 0, 0

    for rec in all_sightings:
        if len(to_check) >= BATCH_LIMIT:
            break
        f       = rec["fields"]
        url     = f.get("Listing URL", "")
        item_id = str(f.get("eBay Item ID") or "").strip()

        if not item_id:
            m = re.search(r"/itm/(\d+)", url)
            if not m:
                skipped_no_id += 1
                continue
            item_id = m.group(1)

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
    ended_ids        = []
    ended_sb_ids     = []
    still_active_ids = []

    for i, rec in enumerate(to_check):
        status = check_listing_status(rec["item_id"], ebay_headers)

        if status == "Expired":
            ended_ids.append(rec["id"])
            sb_id = sb_listings.get(rec["item_id"])
            if sb_id:
                ended_sb_ids.append(sb_id)
            else:
                print(f"   ⚠️  No Supabase match for item_id={rec['item_id']!r} (Airtable rec {rec['id']})")
        else:
            still_active_ids.append(rec["id"])

        time.sleep(0.3)
        if (i + 1) % 100 == 0:
            print(f"   ... {i + 1}/{len(to_check)} — expired: {len(ended_ids)}")

    print(f"\n   Expired: {len(ended_ids)} | Still active: {len(still_active_ids)}")

    # Batch mark ended in Supabase
    if ended_sb_ids:
        print(f"\n📊 Marking {len(ended_sb_ids)} listings ended in Supabase...")
        mark_listings_ended(ended_sb_ids, "expired")

    print(f"\n📝 Applying updates to Airtable...")
    total_updated, err_count = push_updates(ended_ids, still_active_ids)
    print(f"   {total_updated} records updated | {err_count} batch errors")

    elapsed = (datetime.now(timezone.utc) - run_start).seconds
    print(f"\n✅ Done in {elapsed}s")


if __name__ == "__main__":
    main()
