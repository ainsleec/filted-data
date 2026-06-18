#!/usr/bin/env python3
"""
Filted — eBay Expiry Checker + Supabase Price Tracker
Runs via GitHub Actions on a schedule.
Checks all Active sightings in Airtable against eBay Browse API.
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


def supabase_get(path, params=None):
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return []
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/{path}",
        headers=get_supabase_headers(),
        params=params,
        timeout=15
    )
    return resp.json() if resp.ok else []


def supabase_patch(path, match_params, data):
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return False
    resp = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{path}",
        headers=get_supabase_headers(),
        params=match_params,
        json=data,
        timeout=15
    )
    return resp.ok


def load_supabase_active_listings():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("   ⚠️  Supabase not configured — skipping price observations")
        return {}
    rows = supabase_get("listings", params={
        "select":   "id,ebay_item_id,listed_price",
        "ended_at": "is.null",
    })
    return {str(r["ebay_item_id"]): r for r in rows if r.get("ebay_item_id")}


def append_price_observation(listing_id, new_price, date_str):
    """Append price observation to price_history if price changed."""
    rows = supabase_get("listings", params={
        "select": "price_history",
        "id":     f"eq.{listing_id}",
    })
    if not rows:
        return False
    history    = rows[0].get("price_history") or []
    last_price = history[-1]["price"] if history else None
    if new_price == last_price:
        return False
    history.append({"date": date_str, "price": new_price})
    return supabase_patch("listings", {"id": f"eq.{listing_id}"}, {"price_history": history})


def mark_listing_ended(listing_id, reason):
    return supabase_patch(
        "listings",
        {"id": f"eq.{listing_id}"},
        {
            "ended_at":     datetime.now(timezone.utc).isoformat(),
            "ended_reason": reason,
        }
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
            return "Expired", None
        if resp.status_code == 200:
            data      = resp.json()
            price_str = data.get("price", {}).get("value")
            price_val = float(price_str) if price_str else None

            end_date_raw = data.get("itemEndDate", "")
            if end_date_raw:
                end_dt = datetime.fromisoformat(end_date_raw.replace("Z", "+00:00"))
                if end_dt < datetime.now(timezone.utc):
                    return "Expired", None

            availabilities = data.get("estimatedAvailabilities", [])
            if availabilities:
                avail         = availabilities[0]
                status        = avail.get("estimatedAvailabilityStatus", "")
                qty_sold      = int(avail.get("estimatedSoldQuantity", 0))
                qty_remaining = int(avail.get("estimatedRemainingQuantity", 1))
                if status == "SOLD_OUT" or qty_sold > 0 or qty_remaining == 0:
                    return "Expired", price_val
                if status in ("UNAVAILABLE", "OUT_OF_STOCK"):
                    return "Expired", None
                if status == "IN_STOCK":
                    return "Active", price_val

            if data.get("buyingOptions"):
                return "Active", price_val
            return "Expired", None
    except Exception:
        pass
    return "Active", None


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

    updates = (
        [{"id": rid, "fields": {"Status": "Expired", "Last Checked": now_iso}} for rid in ended_ids] +
        [{"id": rid, "fields": {"Last Checked": now_iso}} for rid in still_active_ids]
    )

    errors = 0
    for i in range(0, len(updates), 10):
        resp = requests.patch(url_at, headers=HEADERS_AT, json={"records": updates[i:i + 10]})
        if not resp.ok:
            print(f"  ⚠️  Batch {i // 10 + 1} error: {resp.text[:200]}")
            errors += 1
        time.sleep(0.2)
    return len(updates), errors


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    run_start = datetime.now(timezone.utc)
    today     = run_start.strftime("%Y-%m-%d")
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
    print(f"   {len(sb_listings)} active listings loaded for price tracking")

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
    ended_ids       = []
    still_active_ids = []
    price_updates   = 0

    for i, rec in enumerate(to_check):
        status, current_price = check_listing_status(rec["item_id"], ebay_headers)

        if status == "Expired":
            ended_ids.append(rec["id"])
            sb = sb_listings.get(rec["item_id"])
            if sb:
                mark_listing_ended(sb["id"], "expired")

        elif status == "Active":
            still_active_ids.append(rec["id"])
            sb = sb_listings.get(rec["item_id"])
            if sb and current_price:
                if append_price_observation(sb["id"], current_price, today):
                    price_updates += 1

        time.sleep(0.3)
        if (i + 1) % 100 == 0:
            print(f"   ... {i + 1}/{len(to_check)} — expired: {len(ended_ids)}, price changes: {price_updates}")

    print(f"\n   Expired: {len(ended_ids)} | Still active: {len(still_active_ids)}")
    print(f"   Price changes logged to Supabase: {price_updates}")

    print(f"\n📝 Applying updates to Airtable...")
    total_updated, err_count = push_updates(ended_ids, still_active_ids)
    print(f"   {total_updated} records updated | {err_count} batch errors")

    elapsed = (datetime.now(timezone.utc) - run_start).seconds
    print(f"\n✅ Done in {elapsed}s")


if __name__ == "__main__":
    main()
