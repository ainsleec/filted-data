#!/usr/bin/env python3
"""
Filted — New eBay Listings Scraper
Runs twice daily on GitHub Actions (7am + 7pm AEST).

Searches eBay AU strictly within Women's Clothing (category 15724).
Uses Browse API (same OAuth flow as expiry checker — no separate App ID needed).
Adds new unmatched sightings to Airtable Resale Sightings table.
"""

import os, requests, time, base64
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN      = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE       = os.environ["AIRTABLE_BASE"]
DESIGNERS_TABLE     = os.environ.get("DESIGNERS_TABLE", "Designers")
SIGHTINGS_TABLE     = os.environ["RESALE_SIGHTINGS_TABLE"]

EBAY_CLIENT_ID      = os.environ["EBAY_CLIENT_ID"]
EBAY_CLIENT_SECRET  = os.environ["EBAY_CLIENT_SECRET"]

# ── eBay strict settings ──────────────────────────────────────────────────────
EBAY_WOMENS_CAT     = "15724"    # eBay AU: Women's Clothing — no other categories
LOOKBACK_HOURS      = 14         # Runs twice daily, 14hr window with overlap
MAX_PER_DESIGNER    = 100        # Cap per designer per run

TITLE_EXCLUSIONS = [
    "kids", "girls", "boys", "baby", "toddler", "junior",
    "men's", "mens", "unisex", "costume", "cosplay",
    "pattern", "sewing pattern", "fabric", "material",
    "hanger", "bag", "shoes", "boots", "heels", "sandals",
    "sunglasses", "jewellery", "jewelry", "necklace", "ring", "earring",
    "belt", "scarf", "hat", "cap", "beanie", "perfume", "fragrance"
]

HEADERS_AT = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type":  "application/json"
}

# ── eBay OAuth ────────────────────────────────────────────────────────────────
def get_ebay_token():
    """Fetch a fresh eBay OAuth token via client credentials. Valid ~2hrs."""
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


# ── Airtable helpers ──────────────────────────────────────────────────────────
def fetch_designers():
    """Pull all designer names from Airtable Designers table."""
    designers = []
    params = {
        "fields[]": ["Designer Name"],
        "pageSize": 100
    }
    while True:
        resp = requests.get(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{DESIGNERS_TABLE}",
            headers=HEADERS_AT, params=params
        )
        resp.raise_for_status()
        data = resp.json()
        for rec in data.get("records", []):
            name = rec["fields"].get("Designer Name", "").strip()
            if name:
                designers.append(name)
        if not data.get("offset"):
            break
        params["offset"] = data["offset"]
    return designers


def fetch_existing_item_ids():
    """Load all eBay Item IDs already in Airtable to skip duplicates."""
    item_ids = set()
    params = {
        "fields[]": ["eBay Item ID"],
        "pageSize": 100
    }
    while True:
        resp = requests.get(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{SIGHTINGS_TABLE}",
            headers=HEADERS_AT, params=params
        )
        resp.raise_for_status()
        data = resp.json()
        for rec in data.get("records", []):
            iid = rec["fields"].get("eBay Item ID")
            if iid:
                item_ids.add(str(iid).strip())
        if not data.get("offset"):
            break
        params["offset"] = data["offset"]
    return item_ids


def add_sightings(new_items):
    """Batch-add new sightings to Airtable in groups of 10."""
    url_at = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{SIGHTINGS_TABLE}"
    added  = 0

    for i in range(0, len(new_items), 10):
        batch   = new_items[i:i + 10]
        records = []
        for item in batch:
            fields = {
                "eBay Item ID":  item["item_id"],
                "Listing URL":   item["url"],
                "eBay Title":    item["title"][:500],
                "Date Listed":   item["date_listed"],
                "Status":        "Active",
                "Seller Name":   item["seller"],
            }
            if item.get("price") is not None:
                fields["Listed Price"] = item["price"]
            if item.get("image_url"):
                fields["Main Image"] = [{"url": item["image_url"]}]
            records.append({"fields": fields})

        resp = requests.post(url_at, headers=HEADERS_AT, json={"records": records})
        if resp.ok:
            added += len(batch)
        else:
            print(f"  ⚠️  Airtable batch error: {resp.text[:300]}")
        time.sleep(2)

    return added


# ── eBay Browse API search ────────────────────────────────────────────────────
def search_ebay(keyword, ebay_headers, since_hours):
    """
    Search eBay AU Browse API.
    STRICT: category 15724 (Women's Clothing) only, AU only, newly listed.
    Returns list of item summary dicts.
    """
    try:
        resp = requests.get(
            "https://api.ebay.com/buy/browse/v1/item_summary/search",
            headers=ebay_headers,
            params={
                "q":            keyword,
                "category_ids": EBAY_WOMENS_CAT,
                "filter":       "itemLocationCountry:AU,price:[20..],priceCurrency:AUD",
                "sort":         "newlyListed",
                "limit":        str(MAX_PER_DESIGNER),
            },
            timeout=20
        )
        if not resp.ok:
            print(f"  eBay error for '{keyword}': {resp.status_code} {resp.text[:200]}")
            return []
        return resp.json().get("itemSummaries", [])
    except Exception as e:
        print(f"  Exception searching '{keyword}': {e}")
        return []


def is_new_enough(item, since_hours):
    """Return True if item was listed within the lookback window."""
    date_str = item.get("itemCreationDate", "")
    if not date_str:
        return True  # Can't tell, include it
    try:
        listed_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        cutoff    = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        return listed_dt >= cutoff
    except Exception:
        return True


def is_excluded(title):
    """Return True if title suggests a non-clothing item."""
    t = title.lower()
    return any(excl in t for excl in TITLE_EXCLUSIONS)


def parse_item(raw, designer_name):
    """Extract clean fields from a Browse API item summary dict."""
    # Browse API returns v1|ITEMID|0 — extract numeric ID
    raw_id  = raw.get("itemId", "")
    parts   = raw_id.split("|")
    item_id = parts[1] if len(parts) >= 2 else raw_id

    title     = raw.get("title", "")
    url       = raw.get("itemWebUrl", "")
    image_url = raw.get("image", {}).get("imageUrl", "")
    seller    = raw.get("seller", {}).get("username", "")
    price_val = raw.get("price", {}).get("value")
    date_str  = raw.get("itemCreationDate", "")

    try:
        price = float(price_val) if price_val else None
    except (ValueError, TypeError):
        price = None

    try:
        date_listed = datetime.fromisoformat(
            date_str.replace("Z", "+00:00")
        ).strftime("%Y-%m-%d")
    except Exception:
        date_listed = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return {
        "item_id":     item_id.strip(),
        "title":       title,
        "url":         url,
        "price":       price,
        "image_url":   image_url,
        "date_listed": date_listed,
        "seller":      seller,
        "designer":    designer_name,
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    run_start = datetime.now(timezone.utc)
    print(f"🛍  Filted — New Listings Scrape")
    print(f"    {run_start.strftime('%Y-%m-%d %H:%M UTC')}  |  Category: Women's Clothing ({EBAY_WOMENS_CAT}) ONLY\n")

    print("🔑 Fetching eBay OAuth token...")
    ebay_token = get_ebay_token()
    ebay_headers = {
        "Authorization":           f"Bearer {ebay_token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_AU"
    }

    print("\n📋 Fetching designers from Airtable...")
    designers = fetch_designers()
    print(f"   {len(designers)} designer(s): {', '.join(designers)}\n")

    print("🗂  Loading existing eBay Item IDs (dedup)...")
    existing_ids = fetch_existing_item_ids()
    print(f"   {len(existing_ids)} existing sightings\n")

    all_new        = []
    total_found    = 0
    total_old      = 0
    total_excluded = 0
    total_dupes    = 0

    print(f"🔍 Searching eBay AU (last {LOOKBACK_HOURS}hrs)...")
    for designer in designers:
        raw_items = search_ebay(designer, ebay_headers, since_hours=LOOKBACK_HOURS)
        new_count = 0

        for raw in raw_items:
            total_found += 1

            # Skip if outside lookback window
            if not is_new_enough(raw, LOOKBACK_HOURS):
                total_old += 1
                continue

            item = parse_item(raw, designer)

            if is_excluded(item["title"]):
                total_excluded += 1
                continue

            if item["item_id"] in existing_ids:
                total_dupes += 1
                continue

            existing_ids.add(item["item_id"])
            all_new.append(item)
            new_count += 1

        if raw_items:
            print(f"   {designer}: {len(raw_items)} found → {new_count} new")
        else:
            print(f"   {designer}: no results")
        time.sleep(0.5)

    print(f"\n   Total found: {total_found} | Too old: {total_old} | Excluded: {total_excluded} | Dupes: {total_dupes} | New: {len(all_new)}")

    if all_new:
        print(f"\n📝 Adding {len(all_new)} new sightings to Airtable (unmatched)...")
        added = add_sightings(all_new)
        print(f"   ✅ {added} added")
    else:
        print("\n   Nothing new to add.")

    elapsed = (datetime.now(timezone.utc) - run_start).seconds
    print(f"\n✅ Done in {elapsed}s")


if __name__ == "__main__":
    main()
