#!/usr/bin/env python3
"""
Filted — New eBay Listings Scraper
Runs twice daily on PythonAnywhere (7am + 7pm AEST).

Searches eBay AU strictly within Women's Clothing (category 15724).
Adds new unmatched sightings to Airtable Resale Sightings table.
Unmatched = no garment link yet, but visible in live feed.
"""

import os, requests, time
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN      = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE       = os.environ["AIRTABLE_BASE"]
DESIGNERS_TABLE     = os.environ.get("DESIGNERS_TABLE", "Designers")
SIGHTINGS_TABLE     = os.environ["RESALE_SIGHTINGS_TABLE"]

EBAY_APP_ID         = os.environ["EBAY_APP_ID"]   # Client ID from eBay Developer portal

# ── eBay strict settings ──────────────────────────────────────────────────────
EBAY_WOMENS_CAT     = "15724"    # eBay AU: Women's Clothing — no other categories accepted
LOOKBACK_HOURS      = 14         # Runs twice daily, 14hr window catches everything with overlap
MAX_PER_DESIGNER    = 100        # Cap per designer per run

# These title keywords indicate non-women's items slipping through — skip them
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

# ── Airtable helpers ──────────────────────────────────────────────────────────
def fetch_designers():
    """
    Pull all active designer names from Airtable Designers table.
    Uses 'eBay Search Term' field if set (allows custom search strings),
    falls back to 'Name'. This means adding a designer to Airtable
    automatically picks it up here — no script changes needed.
    """
    designers = []
    params = {
        "fields[]": ["Name", "eBay Search Term"],
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
            f    = rec["fields"]
            term = f.get("eBay Search Term") or f.get("Name", "")
            if term.strip():
                designers.append(term.strip())
        if not data.get("offset"):
            break
        params["offset"] = data["offset"]
    return designers


def fetch_existing_item_ids():
    """
    Load all eBay Item IDs already stored in Airtable.
    Used to skip duplicates without hitting eBay API again.
    """
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
                "eBay Title":    item["title"][:500],   # Airtable text field limit safety
                "Date Listed":   item["date_listed"],
                "Status":        "Active",
                "Seller Name":   item["seller"],
            }
            if item.get("price") is not None:
                fields["Listed Price"] = item["price"]
            # Gallery URL works as an attachment — low-res but fine for unmatched feed
            if item.get("image_url"):
                fields["Main Image"] = [{"url": item["image_url"]}]
            records.append({"fields": fields})

        resp = requests.post(url_at, headers=HEADERS_AT, json={"records": records})
        if resp.ok:
            added += len(batch)
        else:
            print(f"  ⚠️  Airtable batch error: {resp.text[:300]}")
        time.sleep(0.3)

    return added


# ── eBay Finding API ──────────────────────────────────────────────────────────
def search_ebay(keyword, since_hours):
    """
    Search eBay AU Finding API.
    STRICT: category 15724 (Women's Clothing) only, AU located, Fixed Price only.
    Returns list of raw item dicts.
    """
    since_dt = (
        datetime.now(timezone.utc) - timedelta(hours=since_hours)
    ).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    params = {
        "OPERATION-NAME":                   "findItemsAdvanced",
        "SERVICE-VERSION":                  "1.13.0",
        "SECURITY-APPNAME":                 EBAY_APP_ID,
        "RESPONSE-DATA-FORMAT":             "JSON",
        "GLOBAL-ID":                        "EBAY-AU",
        # ── Strict category — Women's Clothing only ──────────────────────────
        "categoryId":                       EBAY_WOMENS_CAT,
        "keywords":                         keyword,
        "sortOrder":                        "StartTimeNewest",
        # ── Filters ──────────────────────────────────────────────────────────
        "itemFilter(0).name":               "ListingType",
        "itemFilter(0).value":              "FixedPrice",
        "itemFilter(1).name":               "StartTimeFrom",
        "itemFilter(1).value":              since_dt,
        "itemFilter(2).name":               "LocatedIn",
        "itemFilter(2).value":              "AU",
        "itemFilter(3).name":               "MinPrice",
        "itemFilter(3).value":              "20",        # Skip obvious junk / accessories
        "itemFilter(3).paramName":          "Currency",
        "itemFilter(3).paramValue":         "AUD",
        "paginationInput.entriesPerPage":   str(MAX_PER_DESIGNER),
    }

    try:
        resp = requests.get(
            "https://svcs.ebay.com/services/search/FindingService/v1",
            params=params, timeout=20
        )
        resp.raise_for_status()
        data         = resp.json()
        search_resp  = data.get("findItemsAdvancedResponse", [{}])[0]
        ack          = search_resp.get("ack", ["Failure"])[0]
        if ack != "Success":
            err = search_resp.get("errorMessage", [{}])[0].get("error", [{}])[0].get("message", ["Unknown"])[0]
            print(f"  eBay error for '{keyword}': {err}")
            return []
        items_wrap = search_resp.get("searchResult", [{}])[0]
        return items_wrap.get("item", [])
    except Exception as e:
        print(f"  Exception searching '{keyword}': {e}")
        return []


def is_excluded(title):
    """Return True if title contains a keyword indicating a non-women's-clothing item."""
    t = title.lower()
    return any(excl in t for excl in TITLE_EXCLUSIONS)


def parse_item(raw, designer_name):
    """Extract clean fields from a Finding API item dict."""
    def v(field, default=""):
        val = raw.get(field, [default])
        return val[0] if isinstance(val, list) else val

    item_id    = v("itemId")
    title      = v("title")
    url        = v("viewItemURL")
    image_url  = v("galleryURL")
    seller     = raw.get("sellerInfo", [{}])[0].get("sellerUserName", [""])[0]
    price_raw  = raw.get("sellingStatus", [{}])[0].get("currentPrice", [{}])[0].get("__value__")
    start_time = raw.get("listingInfo", [{}])[0].get("startTime", [""])[0]

    try:
        price = float(price_raw) if price_raw else None
    except (ValueError, TypeError):
        price = None

    try:
        date_listed = datetime.fromisoformat(
            start_time.replace("Z", "+00:00")
        ).strftime("%Y-%m-%d")
    except Exception:
        date_listed = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return {
        "item_id":     str(item_id).strip(),
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

    print("📋 Fetching designers from Airtable...")
    designers = fetch_designers()
    print(f"   {len(designers)} designer(s): {', '.join(designers)}\n")

    print("🗂  Loading existing eBay Item IDs (dedup)...")
    existing_ids = fetch_existing_item_ids()
    print(f"   {len(existing_ids)} existing sightings\n")

    all_new       = []
    total_found   = 0
    total_excluded= 0
    total_dupes   = 0

    print(f"🔍 Searching eBay AU (last {LOOKBACK_HOURS}hrs)...")
    for designer in designers:
        raw_items = search_ebay(designer, since_hours=LOOKBACK_HOURS)
        new_count = 0

        for raw in raw_items:
            total_found += 1
            item = parse_item(raw, designer)

            # Strict title exclusion check
            if is_excluded(item["title"]):
                total_excluded += 1
                continue

            # Skip if already in Airtable
            if item["item_id"] in existing_ids:
                total_dupes += 1
                continue

            existing_ids.add(item["item_id"])  # Prevent within-run dupes
            all_new.append(item)
            new_count += 1

        if raw_items:
            print(f"   {designer}: {len(raw_items)} found → {new_count} new")
        else:
            print(f"   {designer}: no results")
        time.sleep(0.5)

    print(f"\n   Total found: {total_found} | Category-excluded: {total_excluded} | Dupes: {total_dupes} | New: {len(all_new)}")

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
