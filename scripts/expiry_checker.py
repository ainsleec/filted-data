#!/usr/bin/env python3
"""
Filted — eBay Expiry Checker + Supabase Price Tracker
Runs once daily on PythonAnywhere (2am AEST).

Checks all Active sightings in Airtable against eBay Browse API.
Updates status to Expired or Sold, fetches final sold price where available.

NEW: Also writes price observations to Supabase listings table.
  - Detects price changes and appends to price_history JSONB
  - Stamps ended_at and ended_reason when a listing expires or sells
"""

import os, re, requests, time, base64, json
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN         = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE          = os.environ["AIRTABLE_BASE"]
RESALE_SIGHTINGS_TABLE = os.environ["RESALE_SIGHTINGS_TABLE"]

EBAY_CLIENT_ID     = os.environ["EBAY_CLIENT_ID"]
EBAY_CLIENT_SECRET = os.environ["EBAY_CLIENT_SECRET"]

# NEW: Supabase credentials
SUPABASE_URL         = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

MIN_AGE_DAYS  = 1
RECHECK_DAYS  = 1
BATCH_LIMIT   = 2000

HEADERS_AT = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type":  "application/json"
}


# ── NEW: Supabase helpers ─────────────────────────────────────────────────────
def get_supabase_headers():
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }


def supabase_get(path, params=None):
    """GET request to Supabase REST API."""
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/{path}",
        headers=get_supabase_headers(),
        params=params,
        timeout=15
    )
    return resp.json() if resp.ok else []


def supabase_patch(path, match_params, data):
    """PATCH (update) matching rows in Supabase."""
    resp = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{path}",
        headers=get_supabase_headers(),
        params=match_params,
        json=data,
        timeout=15
    )
    return resp.ok


def load_supabase_active_listings():
    """
    Fetch all active (not ended) listings from Supabase.
    Returns dict keyed by ebay_item_id for fast lookup.
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("   ⚠️  Supabase not configured — skipping price observations")
        return {}

    rows = supabase_get("listings", params={
        "select":   "id,ebay_item_id,price_history",
        "ended_at": "is.null",
    })
    return {r["ebay_item_id"]: r for r in rows if r.get("ebay_item_id")}


def append_price_observation(ebay_item_id, new_price, date_str, sb_listings):
    """
    Append a price observation to a listing's price_history if price changed.
    Only writes if price differs from the last recorded entry.
    Returns True if an update was made.
    """
    listing = sb_listings.get(ebay_item_id)
    if not listing or new_price is None:
        return False

    history   = listing.get("price_history") or []
    last_price = history[-1]["price"] if history else None

    if new_price == last_price:
        return False  # No change, skip

    history.append({"date": date_str, "price": new_price})
    ok = supabase_patch(
        "listings",
        {"ebay_item_id": f"eq.{ebay_item_id}"},
        {"price_history": history}
    )
    return ok


def mark_listing_ended(ebay_item_id, reason):
    """Stamp ended_at and ended_reason on a Supabase listing."""
    return supabase_patch(
        "listings",
        {"ebay_item_id": f"eq.{ebay_item_id}"},
        {
            "ended_at":     datetime.now(timezone.utc).isoformat(),
            "ended_reason": reason,
        }
    )


# ── eBay OAuth token (auto-refresh) ───────────────────────────────────────────
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


# ── eBay status checks ────────────────────────────────────────────────────────
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
                    return "Sold", price_val
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


def get_sold_price(item_id, ebay_app_id):
    try:
        resp = requests.get(
            "https://open.api.ebay.com/shopping",
            params={
                "callname":         "GetSingleItem",
                "appid":            ebay_app_id,
                "siteid":           "15",
                "ItemID":           item_id,
                "responseencoding": "JSON",
                "version":          "967",
            },
            timeout=15
        )
        if not resp.ok:
            return None, None
        data    = resp.json()
        item    = data.get("Item", {})
        selling = item.get("SellingStatus", {})
        price   = selling.get("CurrentPrice", {}).get("Value")
        end_time= item.get("EndTime", "")
        sold_price = float(price) if price else None
        sold_date  = None
        if end_time:
            try:
                sold_date = datetime.fromisoformat(
                    end_time.replace("Z", "+00:00")
                ).strftime("%Y-%m-%d")
            except Exception:
                pass
        return sold_price, sold_date
    except Exception:
        return None, None


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


def push_updates(ended, sold_with_price, still_active_ids):
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url_at  = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{RESALE_SIGHTINGS_TABLE}"

    sold_updates = []
    for r in sold_with_price:
        fields = {"Status": "Sold", "Last Checked": now_iso}
        if r["sold_price"] is not None:
            fields["Price Sold"] = r["sold_price"]
        if r["sold_date"]:
            fields["Date Sold"] = r["sold_date"]
        sold_updates.append({"id": r["id"], "fields": fields})

    updates = (
        [{"id": r["id"], "fields": {"Status": "Expired", "Last Checked": now_iso}} for r in ended] +
        sold_updates +
        [{"id": rid,     "fields": {"Last Checked": now_iso}} for rid in still_active_ids]
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
    today     = run_start.strftime("%Y-%m-%d")  # NEW: for price observations
    print(f"🔄 Filted — eBay Expiry Checker + Price Tracker")
    print(f"   {run_start.strftime('%Y-%m-%d %H:%M UTC')}\n")

    print("🔑 Fetching eBay OAuth token...")
    ebay_token = get_ebay_token()
    ebay_headers = {
        "Authorization":           f"Bearer {ebay_token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_AU"
    }

    # NEW: Load active listings from Supabase for price comparison
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
        f   = rec["fields"]
        url = f.get("Listing URL", "")

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
    ended        = []
    sold         = []
    still_active = []
    errors       = []
    price_updates = 0  # NEW: track how many price changes detected

    for i, rec in enumerate(to_check):
        status, current_price = check_listing_status(rec["item_id"], ebay_headers)

        if status == "Expired":
            ended.append(rec)
            # NEW: stamp ended in Supabase
            if rec["item_id"] in sb_listings:
                mark_listing_ended(rec["item_id"], "expired")

        elif status == "Sold":
            sold.append(rec)
            # NEW: stamp sold in Supabase
            if rec["item_id"] in sb_listings:
                mark_listing_ended(rec["item_id"], "sold")

        elif status == "Active":
            still_active.append(rec["id"])
            # NEW: detect and log price change
            if current_price and append_price_observation(rec["item_id"], current_price, today, sb_listings):
                price_updates += 1

        else:
            errors.append(rec)

        time.sleep(0.3)
        if (i + 1) % 100 == 0:
            print(f"   ... {i + 1}/{len(to_check)} — expired: {len(ended)}, sold: {len(sold)}, price changes: {price_updates}")

    print(f"\n   Expired: {len(ended)} | Sold: {len(sold)} | Still active: {len(still_active)} | Errors: {len(errors)}")
    print(f"   Price changes logged to Supabase: {price_updates}")  # NEW

    sold_with_price = []
    if sold:
        print(f"\n💰 Fetching final sold prices ({len(sold)} items)...")
        found, missing = 0, 0
        for rec in sold:
            sold_price, sold_date = get_sold_price(rec["item_id"], EBAY_CLIENT_ID)
            sold_with_price.append({
                "id":         rec["id"],
                "sold_price": sold_price,
                "sold_date":  sold_date,
            })
            if sold_price:
                found += 1
                print(f"   💲 {rec['item_id']} → ${sold_price:.2f} on {sold_date}")
            else:
                missing += 1
            time.sleep(0.25)
        print(f"\n   Prices found: {found} | Unavailable: {missing}")

    print(f"\n📝 Applying updates to Airtable...")
    total_updated, err_count = push_updates(ended, sold_with_price, still_active)
    print(f"   {total_updated} records updated | {err_count} batch errors")

    elapsed = (datetime.now(timezone.utc) - run_start).seconds
    print(f"\n✅ Done in {elapsed}s")


if __name__ == "__main__":
    main()
