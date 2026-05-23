#!/usr/bin/env python3
"""
Filted — eBay Expiry Checker
Runs once daily on PythonAnywhere (2am AEST).

Checks all Active sightings in Airtable against eBay Browse API.
Updates status to Expired or Sold, fetches final sold price where available.

Fixes vs original Colab cell:
  - get_sold_price() implemented (was referenced but undefined)
  - Bug fix: sold_updates used wrong key names (price_sold vs sold_price)
  - Token auto-refresh using client credentials (no manual token copy needed)
"""

import os, re, requests, time, base64
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN        = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE         = os.environ["AIRTABLE_BASE"]
RESALE_SIGHTINGS_TABLE= os.environ["RESALE_SIGHTINGS_TABLE"]

EBAY_CLIENT_ID        = os.environ["EBAY_CLIENT_ID"]     # Same as App ID
EBAY_CLIENT_SECRET    = os.environ["EBAY_CLIENT_SECRET"]

MIN_AGE_DAYS          = 1      # Don't check listings newer than this
RECHECK_DAYS          = 1      # Recheck listings not checked within this many days
BATCH_LIMIT           = 4000   # Max sightings to process per run

HEADERS_AT = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type":  "application/json"
}

# ── eBay OAuth token (auto-refresh) ───────────────────────────────────────────
def get_ebay_token():
    """
    Fetch a fresh eBay OAuth token via client credentials flow.
    Valid for ~2 hours — more than enough for one run.
    No more copying tokens manually.
    """
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
    """
    Check eBay listing status via Browse API.
    Returns (status, listed_price):
      status      — "Active", "Sold", or "Expired"
      listed_price — float or None
    """
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

            # Check if listing has passed its end date
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
    return "Active", None   # Default: assume active on error, check again tomorrow


def get_sold_price(item_id, ebay_app_id):
    """
    Fetch final sold price via eBay Shopping API GetSingleItem.
    Returns (price_float_or_None, date_str_or_None).
    Only works within ~90 days of sale — returns (None, None) after that.
    """
    try:
        resp = requests.get(
            "https://open.api.ebay.com/shopping",
            params={
                "callname":         "GetSingleItem",
                "appid":            ebay_app_id,
                "siteid":           "15",           # eBay AU
                "ItemID":           item_id,
                "responseencoding": "JSON",
                "version":          "967",
            },
            timeout=15
        )
        if not resp.ok:
            return None, None

        data = resp.json()
        item = data.get("Item", {})

        # SellingStatus contains final price for ended listings
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
    """Load Active sightings that are due for rechecking."""
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
    """Apply all status updates to Airtable in batches of 10."""
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url_at  = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{RESALE_SIGHTINGS_TABLE}"

    # Build sold update records — fixed key names (was price_sold/date_sold, now sold_price/sold_date)
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
    print(f"🔄 Filted — eBay Expiry Checker")
    print(f"   {run_start.strftime('%Y-%m-%d %H:%M UTC')}\n")

    # Get a fresh eBay token — no more manual copy-paste
    print("🔑 Fetching eBay OAuth token...")
    ebay_token = get_ebay_token()
    ebay_headers = {
        "Authorization":           f"Bearer {ebay_token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_AU"
    }

    # ── 1. Load active sightings ──────────────────────────────────────────────
    print("\n📦 Loading active sightings...")
    all_sightings = load_active_sightings()
    print(f"   {len(all_sightings)} sightings due for checking")

    # ── 2. Filter and cap ─────────────────────────────────────────────────────
    fresh_cutoff = datetime.now(timezone.utc) - timedelta(days=MIN_AGE_DAYS)
    to_check, skipped_fresh, skipped_no_id = [], 0, 0

    for rec in all_sightings:
        if len(to_check) >= BATCH_LIMIT:
            break
        f   = rec["fields"]
        url = f.get("Listing URL", "")

        # Prefer stored eBay Item ID; fall back to URL parse
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

    # ── 3. Check eBay status ──────────────────────────────────────────────────
    print("🔍 Checking eBay listing status...")

    ended        = []   # list of {id, item_id}
    sold         = []   # list of {id, item_id}
    still_active = []   # list of record IDs
    errors       = []

    for i, rec in enumerate(to_check):
        status, _ = check_listing_status(rec["item_id"], ebay_headers)
        if status == "Expired":
            ended.append(rec)
        elif status == "Sold":
            sold.append(rec)
        elif status == "Active":
            still_active.append(rec["id"])
        else:
            errors.append(rec)
        time.sleep(0.3)
        if (i + 1) % 100 == 0:
            print(f"   ... {i + 1}/{len(to_check)} — expired: {len(ended)}, sold: {len(sold)}")

    print(f"\n   Expired: {len(ended)} | Sold: {len(sold)} | Still active: {len(still_active)} | Errors: {len(errors)}")

    # ── 4. Fetch actual sold prices ───────────────────────────────────────────
    sold_with_price = []
    if sold:
        print(f"\n💰 Fetching final sold prices ({len(sold)} items)...")
        found, missing = 0, 0
        for rec in sold:
            sold_price, sold_date = get_sold_price(rec["item_id"], EBAY_CLIENT_ID)
            sold_with_price.append({
                "id":         rec["id"],
                "sold_price": sold_price,   # Fixed key name (was price_sold)
                "sold_date":  sold_date,    # Fixed key name (was date_sold)
            })
            if sold_price:
                found += 1
                print(f"   💲 {rec['item_id']} → ${sold_price:.2f} on {sold_date}")
            else:
                missing += 1
            time.sleep(0.25)
        print(f"\n   Prices found: {found} | Unavailable (>90 days or delisted): {missing}")

    # ── 5. Push updates to Airtable ───────────────────────────────────────────
    print(f"\n📝 Applying updates to Airtable...")
    total_updated, err_count = push_updates(ended, sold_with_price, still_active)
    print(f"   {total_updated} records updated | {err_count} batch errors")

    elapsed = (datetime.now(timezone.utc) - run_start).seconds
    print(f"\n✅ Done in {elapsed}s")


if __name__ == "__main__":
    main()
