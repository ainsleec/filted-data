#!/usr/bin/env python3
"""
Filted — New eBay Listings Scraper (with garment matching + relist detection)
Runs twice daily on GitHub Actions (7am + 7pm AEST).

Restores two capabilities that existed in the original Colab pipeline but
were missing from the plain rewrite of this script:

  1. Garment matching — cleans the eBay title and each candidate garment's
     name (stripping size/colour/fabric/condition noise), scores similarity,
     and auto-links anything scoring >= AUTO_MATCH_THRESHOLD. Scores between
     SUGGEST_THRESHOLD and AUTO_MATCH_THRESHOLD are left unmatched but
     flagged "Needs Review" with a "Best Match Guess" for manual confirmation.

  2. Relist detection — before creating a new sighting, checks whether the
     same (seller, exact title) pair already exists as a Resale Sightings
     record. If so, that existing record is reactivated (Status -> Active,
     refreshed URL/price/end date) instead of creating a duplicate — this
     is what preserves a garment match across an expire-then-relist cycle.

--------------------------------------------------------------------------
2026-07 REVISION NOTES (non-AU listing slipped through):

3. A US-located listing was found synced into Airtable despite this
   script being AU-only by design (EBAY_WOMENS_CAT under the AU category
   tree, "itemLocationCountry:AU" in the Browse API filter string,
   priceCurrency:AUD, and the EBAY_AU marketplace header). The root
   cause: search_ebay()'s "itemLocationCountry:AU" filter was trusted as
   authoritative and NOTHING downstream ever re-checked a listing's
   actual location once eBay returned it.

   eBay's Browse API filters are best-effort, not a hard guarantee —
   sellers self-report itemLocationCountry, it can be blank/stale/wrong,
   and Global Shipping Program listings in particular can appear in the
   AU marketplace's results while their true itemLocation is overseas.
   A missing or inconsistent field on eBay's side can silently pass
   through a filter that has nothing concrete to compare against.

   Fixed with a local, independent second check: parse_item() now reads
   raw["itemLocation"]["country"] directly off each Browse API result,
   and any item whose reported country is present AND not "AU" is
   excluded before it ever reaches garment matching or Airtable — same
   place as the existing title/condition exclusion checks, so it's
   counted and logged the same way. An item with a BLANK/missing
   location field is not auto-excluded (there's nothing to check it
   against), so this is a strict improvement, not a guarantee — treat
   eBay's own filter as the first pass and this as the safety net, not
   as making the AU-only requirement airtight against a seller who
   simply never fills the field in.
--------------------------------------------------------------------------
"""

import os, re, requests, time, base64
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN      = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE       = os.environ["AIRTABLE_BASE"]
DESIGNERS_TABLE     = os.environ.get("DESIGNERS_TABLE", "Designers")
SIGHTINGS_TABLE     = os.environ["RESALE_SIGHTINGS_TABLE"]
GARMENTS_TABLE_ID   = "tblmqjU4WqgCzP7cR"

EBAY_CLIENT_ID      = os.environ["EBAY_CLIENT_ID"]
EBAY_CLIENT_SECRET  = os.environ["EBAY_CLIENT_SECRET"]

# ── eBay strict settings ──────────────────────────────────────────────────────
EBAY_WOMENS_CAT     = "15724"    # eBay AU: Women's Clothing — no other categories
LOOKBACK_HOURS      = 72         # Runs twice daily, 14hr window with overlap
MAX_PER_DESIGNER    = 100        # Cap per designer per run

# Listings must genuinely be located in Australia. eBay's own
# itemLocationCountry filter (in search_ebay() below) is the first pass;
# this is the authoritative value this script itself requires, checked
# again locally in parse_item()/is_wrong_country() rather than trusted
# purely server-side — see 2026-07 revision notes above.
REQUIRED_ITEM_COUNTRY = "AU"

# ── Matching thresholds (unchanged from the original working Colab logic) ────
AUTO_MATCH_THRESHOLD = 0.65
SUGGEST_THRESHOLD    = 0.45

# ── eBay Partner Network affiliate tracking ───────────────────────────────────
# Single campaign ID applied to every listing link, per your confirmation.
# mkrid is your Rover affiliate ID — this is what actually attributes clicks/
# sales to your account. Without these params, links earn zero commission.
EPN_CAMPAIGN_ID = "5339108963"
EPN_PARAMS      = "mkcid=1&mkrid=705-53470-19255-0&siteid=15&toolid=10001&mkevt=1"


def make_affiliate_url(listing_url, campaign_id=EPN_CAMPAIGN_ID):
    """Wrap a raw eBay listing URL with EPN affiliate tracking params."""
    if not listing_url:
        return listing_url
    return f"{listing_url.split('?')[0]}?campid={campaign_id}&{EPN_PARAMS}"

TITLE_EXCLUSIONS = [
    "kids", "girls", "boys", "baby", "toddler", "junior",
    "men's", "mens", "unisex", "costume", "cosplay",
    "pattern", "sewing pattern", "fabric", "material",
    "hanger", "bag", "shoes", "boots", "heels", "sandals",
    "sunglasses", "jewellery", "jewelry", "necklace", "ring", "earring",
    "belt", "scarf", "hat", "cap", "beanie", "perfume", "fragrance",
    "damaged", "repair", "faulty", "bundle", "retro", "inspired", "vintage",
    " mens", "smart", "damaged"
]

EXCLUDED_CONDITIONS = [
    "Acceptable",
    "Fair",
    "For parts or not working"
]

HEADERS_AT = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type":  "application/json"
}


# ── eBay OAuth ────────────────────────────────────────────────────────────────
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


# ── Airtable helpers ──────────────────────────────────────────────────────────
def at_list_all(table, fields=None):
    records = []
    params = {"pageSize": 100}
    if fields:
        params["fields[]"] = fields
    while True:
        resp = requests.get(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{table}",
            headers=HEADERS_AT, params=params
        )
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
        if not data.get("offset"):
            break
        params["offset"] = data["offset"]
    return records


def at_update(table, record_id, fields):
    resp = requests.patch(
        f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{table}/{record_id}",
        headers=HEADERS_AT,
        json={"fields": fields}
    )
    if not resp.ok:
        print(f"    ⚠️  Airtable update error {resp.status_code}: {resp.text[:200]}")
    return resp.ok


def fetch_designers():
    designers = []
    params = {"fields[]": ["Designer Name"], "pageSize": 100}
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


def fetch_garments():
    """Load garments for matching. Table has both 'Name' and 'Garment Name'
    fields — prefer 'Name' (what the original matching logic used), fall
    back to 'Garment Name' if blank."""
    return at_list_all(GARMENTS_TABLE_ID, fields=["Name", "Garment Name", "Product Colour", "Designer"])


def fetch_existing_sightings():
    """Load existing sightings for dedup + relist lookup."""
    return at_list_all(
        SIGHTINGS_TABLE,
        fields=["Listing URL", "eBay Item ID", "Status", "Seller Name", "eBay Title"]
    )


# ── Matching (ported from the original working Colab logic) ──────────────────
def similarity(a, b):
    a = re.sub(r'[^\w\s]', ' ', a)
    b = re.sub(r'[^\w\s]', ' ', b)
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())
    if not words_a or not words_b:
        return 0.0
    intersection = words_a & words_b
    union        = words_a | words_b
    jaccard      = len(intersection) / len(union)
    coverage     = len(intersection) / len(words_b)
    return (jaccard * 0.3) + (coverage * 0.7)


WORD_NORMALIZE = {
    "pants": "pant", "trousers": "trouser", "shorts": "short", "jeans": "jean",
    "blazers": "blazer", "skirts": "skirt", "tops": "top", "boots": "boot", "heels": "heel",
}

_NOISE_WORDS = (
    "linen ramie cotton chiffon crepe velvet wool knit woven silk satin georgette "
    "polyester nylon spandex jersey denim midi mini maxi casual holiday sexy cocktail "
    "romantic formal wedding evening party brunch summer spring autumn winter beach "
    "resort vacation boho bohemian chic elegant cute gorgeous stunning tie belt button "
    "zip pocket pockets sleeve sleeveless long short neck collar strap straps off slip "
    "ruffle frill lace wrap smocked pleated green blue red pink black white yellow "
    "orange purple brown grey gray cream ivory nude navy teal lilac rust sage olive "
    "blush coral gold silver multicolor multicolour neutrals neutral new cond condition "
    "worn used loved owned womens women ladies australian designer authentic genuine "
    "luxury rare beautiful with and the floral print belted pre-loved preloved pre loved"
).split()


def clean_title(title, designer_name=None, is_garment=False):
    t = title
    if designer_name:
        t = re.sub(re.escape(designer_name), "", t, flags=re.IGNORECASE)

    t = re.sub(r'\b(size|sz)\s*\d+\b', "", t, flags=re.IGNORECASE)
    t = re.sub(r'\b(au|us|uk|eu)\s*\d+\b', "", t, flags=re.IGNORECASE)
    t = re.sub(r'\b\d+\s*(au|us|uk|eu)\b', "", t, flags=re.IGNORECASE)
    t = re.sub(r'\b(xs|s|m|l|xl|xxl)\b', "", t, flags=re.IGNORECASE)
    t = re.sub(r'\bsize\b', "", t, flags=re.IGNORECASE)
    t = re.sub(r'\b(au|us|uk|eu)\b', "", t, flags=re.IGNORECASE)

    for word in ["bnwt", "nwt", "new with tags", "brand new", "unworn", "rrp", "rpp"]:
        t = re.sub(re.escape(word), "", t, flags=re.IGNORECASE)

    compounds = {
        "shirt dress": "shirtdress", "shirt-dress": "shirtdress",
        "midi dress": "mididress", "mini dress": "minidress", "maxi dress": "maxidress",
        "cut out": "cutout", "cut-out": "cutout", "cut offs": "cutoffs",
    }
    for old, new in compounds.items():
        t = re.sub(rf'\b{re.escape(old)}\b', new, t, flags=re.IGNORECASE)

    if not is_garment:
        t = re.sub(r'[|/\\]', ' ', t)
        for word in _NOISE_WORDS:
            t = re.sub(rf'\b{re.escape(word)}\b', "", t, flags=re.IGNORECASE)

    t = re.sub(r'\s+', " ", t).strip(" -–$")
    words = t.split()
    t = " ".join(WORD_NORMALIZE.get(w.lower(), w) for w in words)
    return t


def garment_name(g_fields):
    return get_str(g_fields.get("Name")) or get_str(g_fields.get("Garment Name"))


def get_str(value, default=""):
    if value is None:
        return default
    if isinstance(value, list):
        return str(value[0]).strip() if value else default
    return str(value).strip()


def extract_colour(title):
    """Best-effort colour guess straight from the title (Browse API search
    results don't reliably expose localizedAspects the way item detail does)."""
    return None  # kept simple deliberately — colour tie-break is a nice-to-have,
                 # not required for the core matching to function correctly


def find_best_match(ebay_title, garments, designer_name=None, ebay_colour=None):
    cleaned = clean_title(ebay_title, designer_name)
    scored = []

    for g in garments:
        name = garment_name(g["fields"])
        if not name:
            continue
        if designer_name:
            gd = g["fields"].get("Designer", "")
            if isinstance(gd, list):
                if not any(designer_name.lower() in str(d).lower() for d in gd):
                    continue
            elif gd and designer_name.lower() not in str(gd).lower():
                continue
        clean_name = clean_title(name, designer_name, is_garment=True)
        score = similarity(cleaned, clean_name)
        scored.append((score, g))

    if not scored:
        return None, 0.0

    scored.sort(key=lambda x: x[0], reverse=True)
    top_score = scored[0][0]

    if ebay_colour:
        close = [(s, g) for s, g in scored if top_score - s < 0.15]
        if len(close) > 1:
            for s, g in close:
                if ebay_colour in (g["fields"].get("Product Colour") or "").lower():
                    return g, s

    return scored[0][1], scored[0][0]


# ── eBay Browse API search ────────────────────────────────────────────────────
def search_ebay(keyword, ebay_headers, since_hours):
    try:
        resp = requests.get(
            "https://api.ebay.com/buy/browse/v1/item_summary/search",
            headers=ebay_headers,
            params={
                "q":            keyword,
                "category_ids": EBAY_WOMENS_CAT,
                "filter":       "itemLocationCountry:AU,price:[5..],priceCurrency:AUD",
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
    date_str = item.get("itemCreationDate", "")
    if not date_str:
        return True
    try:
        listed_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        cutoff    = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        return listed_dt >= cutoff
    except Exception:
        return True


def is_excluded(title):
    t = title.lower()
    return any(excl in t for excl in TITLE_EXCLUSIONS)


def is_excluded_condition(condition):
    return condition in EXCLUDED_CONDITIONS


def is_wrong_country(location_country):
    """2026-07 addition — local, independent check on top of eBay's own
    itemLocationCountry search filter (which is best-effort, not a hard
    guarantee — see revision notes at the top of this file). A BLANK
    location_country is NOT treated as wrong — there's nothing to check
    it against, so this can only catch listings that positively report a
    non-AU location, not ones that omit it entirely."""
    if not location_country:
        return False
    return location_country.strip().upper() != REQUIRED_ITEM_COUNTRY


def parse_item(raw, designer_name):
    raw_id  = raw.get("itemId", "")
    parts   = raw_id.split("|")
    item_id = parts[1] if len(parts) >= 2 else raw_id

    title     = raw.get("title", "")
    url       = raw.get("itemWebUrl", "")
    image_url = raw.get("image", {}).get("imageUrl", "")
    seller    = raw.get("seller", {}).get("username", "")
    price_val = raw.get("price", {}).get("value")
    date_str  = raw.get("itemCreationDate", "")
    condition = raw.get("condition", "")

    # Independent local check — see is_wrong_country() and the 2026-07
    # revision notes at the top of this file. Browse API item summaries
    # expose this as itemLocation.country (ISO 3166-1 alpha-2, e.g. "AU",
    # "US", "GB") when the seller has set a location at all.
    location_country = raw.get("itemLocation", {}).get("country", "")

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
        "item_id":          item_id.strip(),
        "title":            title,
        "url":              url,
        "price":            price,
        "image_url":        image_url,
        "date_listed":      date_listed,
        "seller":           seller,
        "designer":         designer_name,
        "condition":        condition,
        "location_country": location_country,
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    run_start = datetime.now(timezone.utc)
    print(f"🛍  Filted — New Listings Scrape (with matching + relist detection)")
    print(f"    {run_start.strftime('%Y-%m-%d %H:%M UTC')}  |  Category: Women's Clothing ({EBAY_WOMENS_CAT}) ONLY\n")

    print("🔑 Fetching eBay OAuth token...")
    ebay_token = get_ebay_token()
    ebay_headers = {
        "Authorization":           f"Bearer {ebay_token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_AU"
    }

    print("\n📋 Fetching designers from Airtable...")
    designers = fetch_designers()
    print(f"   {len(designers)} designer(s): {', '.join(designers)}")

    print("\n👗 Fetching garments for matching...")
    garments = fetch_garments()
    print(f"   {len(garments)} garments loaded")

    print("\n🗂  Loading existing sightings (dedup + relist lookup)...")
    existing = fetch_existing_sightings()
    existing_ids = set()
    relist_lookup = {}
    for rec in existing:
        f = rec["fields"]
        iid = f.get("eBay Item ID")
        if iid:
            existing_ids.add(str(iid).strip())
        seller_key = (f.get("Seller Name") or "").lower().strip()
        title_key  = (f.get("eBay Title") or "").lower().strip()
        if seller_key and title_key:
            relist_lookup[(seller_key, title_key)] = rec["id"]
    print(f"   {len(existing_ids)} existing item IDs | {len(relist_lookup)} relist lookup entries")

    all_new = []
    total_found = total_old = total_excluded = total_dupes = total_bad_condition = 0
    total_relisted = total_auto_matched = total_needs_review = total_no_match = 0
    total_wrong_country = 0

    print(f"\n🔍 Searching eBay AU (last {LOOKBACK_HOURS}hrs)...")
    for designer in designers:
        raw_items = search_ebay(designer, ebay_headers, since_hours=LOOKBACK_HOURS)
        new_count = relisted_count = 0

        for raw in raw_items:
            total_found += 1

            if not is_new_enough(raw, LOOKBACK_HOURS):
                total_old += 1
                continue

            item = parse_item(raw, designer)

            if is_wrong_country(item["location_country"]):
                total_wrong_country += 1
                print(f"   ⚠️  Excluded non-AU listing: {item['item_id']} "
                      f"(location={item['location_country']!r}) — {item['title'][:60]!r}")
                continue

            if is_excluded(item["title"]):
                total_excluded += 1
                continue
            if is_excluded_condition(item["condition"]):
                total_bad_condition += 1
                continue

            # ── Relist check FIRST — before treating as new/duplicate ──────
            relist_key = (item["seller"].lower().strip(), item["title"].lower().strip())
            if relist_key in relist_lookup:
                existing_record_id = relist_lookup[relist_key]
                at_update(SIGHTINGS_TABLE, existing_record_id, {
                    "Status":       "Active",
                    "eBay Item ID": item["item_id"],
                    "Listing URL":  make_affiliate_url(item["url"]),
                    **({"Listed Price": item["price"]} if item["price"] is not None else {}),
                })
                existing_ids.add(item["item_id"])
                relisted_count += 1
                total_relisted += 1
                time.sleep(0.1)
                continue

            if item["item_id"] in existing_ids:
                total_dupes += 1
                continue

            existing_ids.add(item["item_id"])
            all_new.append(item)
            new_count += 1

        if raw_items:
            print(f"   {designer}: {len(raw_items)} found → {new_count} new | {relisted_count} relisted")
        else:
            print(f"   {designer}: no results")
        time.sleep(0.5)

    print(f"\n   Total found: {total_found} | Too old: {total_old} | Excluded: {total_excluded} | "
          f"Bad condition: {total_bad_condition} | Wrong country: {total_wrong_country} | "
          f"Relisted: {total_relisted} | New: {len(all_new)}")

    if all_new:
        print(f"\n🔗 Matching {len(all_new)} new listings to garments...")
        url_at = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{SIGHTINGS_TABLE}"

        for i in range(0, len(all_new), 10):
            batch = all_new[i:i + 10]
            records = []
            for item in batch:
                match, score = find_best_match(item["title"], garments, designer_name=item["designer"])

                fields = {
                    "eBay Item ID":  item["item_id"],
                    "Listing URL":   make_affiliate_url(item["url"]),
                    "eBay Title":    item["title"][:500],
                    "Date Listed":   item["date_listed"],
                    "Status":        "Active",
                    "Seller Name":   item["seller"],
                }
                if item.get("price") is not None:
                    fields["Listed Price"] = item["price"]
                if item.get("image_url"):
                    fields["eBay Image"] = [{"url": item["image_url"]}]
                if item.get("condition"):
                    fields["Condition"] = item["condition"]

                if match and score >= AUTO_MATCH_THRESHOLD:
                    fields["Garment"] = [match["id"]]
                    total_auto_matched += 1
                elif match and score >= SUGGEST_THRESHOLD:
                    fields["Needs Review"]      = True
                    fields["Best Match Guess"]  = garment_name(match["fields"])
                    total_needs_review += 1
                else:
                    total_no_match += 1

                records.append({"fields": fields})

            resp = requests.post(url_at, headers=HEADERS_AT, json={"records": records})
            if not resp.ok:
                print(f"  ⚠️  Airtable batch error: {resp.text[:300]}")
            time.sleep(2)

        print(f"\n   Matching: {total_auto_matched} auto-matched | "
              f"{total_needs_review} flagged for review | {total_no_match} no match")
    else:
        print("\n   Nothing new to add.")

    elapsed = (datetime.now(timezone.utc) - run_start).seconds
    print(f"\n✅ Done in {elapsed}s")


if __name__ == "__main__":
    main()
