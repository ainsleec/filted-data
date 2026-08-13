"""
webflow_sync.py — Filted consolidated Webflow sync

Replaces: sync.py, webflow_sync_stage2a.py, webflow_sync_stage2b.py,
image_upload.py (image upload folds away entirely — Webflow hosts images
via its own CDN once an item is created, no separate upload step needed).

Scope: Campaigns + Garments only. Sightings are NOT synced to Webflow at
all — that collection is being retired. Active-listing data lives purely
in Supabase and is read live by the garment page / campaign embed via the
Cloudflare Worker.

Qualifying rule for a Garment to get a Webflow page (create-once-then-
permanent-live — never drafted or deleted after creation):
    has an Active or Sold sighting   OR   (has a populated Product Code
    AND has an Image 1) — the image requirement only applies to the
    Product Code path, since a page created off code alone with no photo
    would be a thin result. Sighting history qualifies on its own
    regardless of image, since that data is rarer/more valuable.

Run order dependency: this script expects listings_sync.py to have
already created a row in Supabase's `garments` table (with `airtable_id`
populated) for any garment with sighting activity. If a qualifying
garment has no matching Supabase row yet (e.g. it only qualifies via
Product Code and has never had a sighting), the "Supabase Garment ID"
Webflow field is simply left blank — it'll backfill on a later run once/
if that garment ever gets a Supabase row. This is expected, not an error.

BEFORE FIRST PRODUCTION RUN: confirm every Webflow field slug referenced
below (AIRTABLE_ID_FIELD_SLUG, SUPABASE_ID_FIELD_SLUG, etc.) against the
real API response from GET /v2/collections/{id}/fields — Webflow's slug
is not reliably just the lowercased display name (this bit a previous
version of this pipeline once already: 'Listed Price' -> 'listing-price',
not 'listed-price'). Treat every *_FIELD_SLUG constant below as a
placeholder to verify, not a confirmed value.

--------------------------------------------------------------------------
2026-08 REVISION NOTES (multi-image sync — only Image 1 was ever sent):

9. Only Image 1 (Webflow slug main-photo) was ever read from Airtable and
   sent to Webflow — Image 2 through Image 7 in Airtable were populated
   but never synced, so garment pages only ever showed one photo despite
   more being available. This mapping existed in an earlier version of
   this pipeline (filted_airtable_webflow_sync.py) but was dropped during
   the consolidation into this file.

   Confirmed live Webflow slugs (2026-08, via GET /v2/collections/{id}):
     Image 1 -> main-photo
     Image 2 -> image-2
     Image 3 -> model-back
     Image 4 -> image-3
     Image 5 -> image-4
     Image 6 -> image-5
     Image 7 -> image-7
   (Image 8 in Airtable has no corresponding Webflow field — the
   collection only has 7 image slots — so it is not synced.)

   Fixed by generalizing the single-image block in sync_garments() into
   a loop over IMAGE_FIELD_MAP, and adding an image_url_n field to each
   synced-garment record so a future garments.json consumer could use
   more than the first photo too, without needing another schema change.
--------------------------------------------------------------------------
"""

import os
import re
import sys
import time
import json
import unicodedata
from datetime import datetime, timezone

import requests

# ── Config ───────────────────────────────────────────────────────────────
AIRTABLE_TOKEN   = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE    = "appUk1ThnHvWwFDHG"

SUPABASE_URL       = os.environ["SUPABASE_URL"]
SUPABASE_ANON_KEY  = os.environ["SUPABASE_ANON_KEY"]

WEBFLOW_API_TOKEN = os.environ["WEBFLOW_API_TOKEN"]
WEBFLOW_SITE_ID   = "68773ee63655ed07cdaa9e75"

GARMENTS_COLLECTION_ID  = "68774f3e850c7a30ebc3a0aa"
CAMPAIGNS_COLLECTION_ID = "68944133543f0cbb26b4aeb9"
DESIGNERS_COLLECTION_ID = "687a1d0eeb0f06f63aef724f"

CLOUDFLARE_ACCOUNT_ID       = os.environ["CLOUDFLARE_ACCOUNT_ID"]
CLOUDFLARE_KV_NAMESPACE_ID  = os.environ["CLOUDFLARE_KV_NAMESPACE_ID"]
CLOUDFLARE_API_TOKEN        = os.environ["CLOUDFLARE_API_TOKEN"]

# Airtable field names (display names — Airtable's API accepts these directly,
# unlike Webflow which needs the internal slug)
FLD_GARMENT_NAME     = "Garment Name"
FLD_NAME_FORMULA     = "Name"                # the "Name (Formula)" field — already includes colour
FLD_DESIGNER         = "Designer"
FLD_COLLECTION       = "Collection"
FLD_PRODUCT_CODE     = "Product Code"
FLD_PRODUCT_COLOUR   = "Product Colour"
FLD_CATEGORY         = "Category"
FLD_RRP              = "RRP"
FLD_IMAGE_1          = "Image 1"
FLD_AIRTABLE_SLUG    = "Slug"
FLD_WEBFLOW_ITEM_ID  = "Webflow Item ID"

DESIGNERS_TABLE  = "Designers"
COLLECTIONS_TABLE = "Collections"
GARMENTS_TABLE   = "All Garments"
SIGHTINGS_TABLE  = "Resale Sightings"

FLD_DESIGNER_NAME       = "Designer Name"
FLD_DESIGNER_WEBFLOW_ID = "Webflow Item ID"  # same field name as on Garments/Collections tables

# Base path for campaign pages — CONFIRMED via the live Webflow preview
# URL (www.filted.com.au/collections/{slug}) on Jul 2026. NOT /campaigns/
# despite the collection being called "Campaigns" in Webflow's CMS.
CAMPAIGN_URL_PREFIX = "/collections/"

# Webflow CMS field slugs — CONFIRMED via GET /v2/collections/{id} on the
# real site (not guessed). If any of these ever look wrong again, re-run
# the diagnostic in the same way rather than assuming.
WF_FIELD_NAME              = "name"
WF_FIELD_SLUG              = "slug"
WF_FIELD_PRODUCT_CODE      = "style-id"          # NOT "product-code" — confirmed real slug
WF_FIELD_RRP               = "rrp"
WF_FIELD_FABRIC            = "fabric"
WF_FIELD_PRODUCT_COLOUR    = "product-colour"
WF_FIELD_CATEGORY          = "category-3"        # field was recreated at least twice — confirmed real slug
WF_FIELD_CAMPAIGN_REF      = "campaign-2"        # NOT "campaign" — confirmed real slug
WF_FIELD_AIRTABLE_ID       = "airtable-record-id"
WF_FIELD_SUPABASE_ID       = "supabase-garment-id"
WF_FIELD_IMAGE_1           = "main-photo"        # NOT "image-1" — confirmed real slug

# 2026-08 (revision note 9): full Airtable-attachment-field -> Webflow-
# image-slug map, confirmed live via GET /v2/collections/{id}. Image 8 in
# Airtable is deliberately absent — the Webflow collection only has 7
# image slots, there's nowhere for it to go.
IMAGE_FIELD_MAP = {
    "Image 1": "main-photo",
    "Image 2": "image-2",
    "Image 3": "model-back",
    "Image 4": "image-3",
    "Image 5": "image-4",
    "Image 6": "image-5",
    "Image 7": "image-7",
}

# Designer is a single-reference field (Webflow's panel shows "(Reference)",
# not "(Multi-reference)") — payload is a plain item ID string, not an array.
WF_FIELD_DESIGNER_REF      = "designers"  # CONFIRMED SLUG, CONFIRMED single-reference

WF_DESIGNER_FIELD_NAME = "name"
WF_DESIGNER_FIELD_SLUG = "slug"

# Campaigns collection — confirmed slugs
WF_CAMPAIGN_FIELD_NAME           = "name"
WF_CAMPAIGN_FIELD_SLUG           = "slug"
WF_CAMPAIGN_FIELD_AIRTABLE_ID    = "airtable-collection-id"
WF_CAMPAIGN_FIELD_DESIGNER_NAME  = "designer-name"   # plain text on Campaigns — NOT a reference, unlike Garments' Designer field
WF_CAMPAIGN_FIELD_SEASON_CODE    = "season-code"

# Set DRY_RUN=true as an env var (or GitHub Actions input) to log every
# intended create/update/publish/redirect-write without actually calling
# any write endpoint. Reads (Airtable fetch, Supabase lookup, Webflow GET)
# still happen normally, so you see real, accurate qualifying counts —
# only the mutating calls are short-circuited.
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

# Optional narrowing for faster test/dry runs — leave both unset for a
# full production run. DESIGNER_FILTER: comma-separated designer names,
# e.g. "Alemais" or "Alemais,Aje" — intersected with the normal In Feed
# scope, doesn't bypass it. LIMIT: caps the number of qualifying garments
# actually processed (useful to just eyeball a handful quickly).
DESIGNER_FILTER = {
    d.strip() for d in os.environ.get("DESIGNER_FILTER", "").split(",") if d.strip()
}
LIMIT = int(os.environ["LIMIT"]) if os.environ.get("LIMIT") else None

# SIGHTED_ONLY=true restricts the qualifying set to garments that have an
# Active/Sold sighting right now — useful for a small test batch aimed at
# actually exercising the Active Listings widget with real, non-empty
# data, rather than only ever landing on garments with nothing listed.
SIGHTED_ONLY = os.environ.get("SIGHTED_ONLY", "false").lower() == "true"

AIRTABLE_HEADERS = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
WEBFLOW_HEADERS  = {
    "Authorization": f"Bearer {WEBFLOW_API_TOKEN}",
    "accept": "application/json",
    "content-type": "application/json",
}
SUPABASE_HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
}

GARMENTS_JSON_PATH = "garments.json"

# Incremental sync state — ISO-8601 UTC timestamp of the last successful
# run's START time, committed to the repo by the Actions workflow (make
# sure the workflow's commit step includes this file, not just
# garments.json). Missing/unreadable file -> full sync.
LAST_SYNC_PATH = "webflow_last_sync.txt"

# FULL_SYNC=true forces every qualifying garment to be processed,
# ignoring webflow_last_sync.txt. Use after changing field mappings, slug
# logic, or anything else where already-synced items need a re-push.
FULL_SYNC = os.environ.get("FULL_SYNC", "false").lower() == "true"

# The Airtable fields whose modification should trigger a garment
# re-sync — i.e. exactly the set of source fields this script pushes to
# Webflow. Field-scoped so writes to OTHER fields on the same record
# (sighting rollups, listings_sync.py bookkeeping, this script's own
# "Webflow Item ID" write-back on create) do NOT count as modifications.
# "Name (Formula)" can't be tracked directly (formula fields have no
# modified time) — its inputs (Garment Name, Product Colour) are listed
# instead. If that formula ever gains another input, add it here.
#
# 2026-08 (revision note 9): now includes every field in IMAGE_FIELD_MAP,
# not just Image 1 — otherwise a garment whose only change was adding
# Image 4/5/6/7 in Airtable would never be picked up by an incremental
# run.
MODIFIED_TRACKED_FIELDS = [
    FLD_GARMENT_NAME, FLD_DESIGNER, FLD_COLLECTION, FLD_PRODUCT_CODE,
    FLD_PRODUCT_COLOUR, FLD_CATEGORY, FLD_RRP,
] + list(IMAGE_FIELD_MAP.keys())

# Seconds to sleep between pages when paginating a full Webflow
# collection fetch. 1.1s keeps the fetch at ~54 requests/minute — safely
# under Webflow's 60/min limit with headroom for the occasional call
# elsewhere in the run — so webflow_all_items() almost never has to fall
# back to the reactive 429 retry path.
WEBFLOW_PAGINATION_SLEEP = 1.1


# ── Airtable helpers ─────────────────────────────────────────────────────
def airtable_request(method, url, json_body=None, params=None, max_retries=5):
    for attempt in range(max_retries):
        res = requests.request(method, url, headers=AIRTABLE_HEADERS,
                               json=json_body, params=params, timeout=30)
        if res.ok:
            return res
        if res.status_code == 429 or res.status_code >= 500:
            wait = int(res.headers.get("Retry-After", 5 * (attempt + 1)))
            print(f"  Airtable {res.status_code} on {method} {url} — waiting {wait}s, retry {attempt + 1}/{max_retries}")
            time.sleep(wait + 1)
            continue
        raise requests.exceptions.HTTPError(
            f"{res.status_code} on {method} {url}: {res.text[:500]} | payload: {json_body}",
            response=res,
        )
    raise requests.exceptions.HTTPError(
        f"Exhausted {max_retries} retries on {method} {url} (repeated 429/5xx): {json_body}"
    )


def airtable_fetch_all(table, filter_formula=None, fields=None):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{requests.utils.quote(table)}"
    records, offset = [], None
    while True:
        params = {}
        if filter_formula:
            params["filterByFormula"] = filter_formula
        if offset:
            params["offset"] = offset
        if fields:
            params["fields[]"] = fields
        res = requests.get(url, headers=AIRTABLE_HEADERS, params=params, timeout=30)
        res.raise_for_status()
        data = res.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break

    seen = {}
    for r in records:
        seen[r["id"]] = r
    deduped = list(seen.values())
    if len(deduped) != len(records):
        print(f"  NOTE: {table} fetch returned {len(records)} rows, "
              f"{len(records) - len(deduped)} duplicate(s) removed (pagination artifact)")
    return deduped


def airtable_update(table, record_id, fields):
    if DRY_RUN:
        print(f"  [DRY RUN] would update Airtable {table}/{record_id} with {fields}")
        return {"id": record_id, "fields": fields}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{requests.utils.quote(table)}/{record_id}"
    res = airtable_request("PATCH", url, json_body={"fields": fields})
    return res.json()


# ── Incremental sync helpers ─────────────────────────────────────────────
def read_last_sync_timestamp():
    if FULL_SYNC:
        print("FULL_SYNC=true — ignoring last-sync timestamp, processing all qualifying garments.")
        return None
    try:
        with open(LAST_SYNC_PATH) as fp:
            ts = fp.read().strip()
        if not ts:
            return None
        datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return ts
    except FileNotFoundError:
        print(f"No {LAST_SYNC_PATH} found — treating as first run, full sync.")
        return None
    except ValueError:
        print(f"WARNING: {LAST_SYNC_PATH} contents unparseable — falling back to full sync.")
        return None


def write_last_sync_timestamp(ts):
    if DRY_RUN:
        print(f"  [DRY RUN] would write {LAST_SYNC_PATH} = {ts}")
        return
    with open(LAST_SYNC_PATH, "w") as fp:
        fp.write(ts + "\n")
    print(f"Wrote {LAST_SYNC_PATH} = {ts}")


def get_modified_garment_ids(last_sync_iso):
    scoped_fields = ", ".join("{" + fld + "}" for fld in MODIFIED_TRACKED_FIELDS)
    formula = (
        f"OR({{{FLD_WEBFLOW_ITEM_ID}}}='', "
        f"IS_AFTER(LAST_MODIFIED_TIME({scoped_fields}), '{last_sync_iso}'))"
    )
    records = airtable_fetch_all(GARMENTS_TABLE, filter_formula=formula, fields=[FLD_GARMENT_NAME])
    return {r["id"] for r in records}


# ── Supabase helper ──────────────────────────────────────────────────────
def get_supabase_garment_uuid(airtable_id):
    url = f"{SUPABASE_URL}/rest/v1/garments?airtable_id=eq.{airtable_id}&select=id"
    res = requests.get(url, headers=SUPABASE_HEADERS, timeout=15)
    res.raise_for_status()
    rows = res.json()
    return rows[0]["id"] if rows else None


def update_supabase_garment_slug(supabase_uuid, slug):
    if DRY_RUN:
        print(f"  [DRY RUN] would update Supabase garments.slug for {supabase_uuid} -> {slug}")
        return
    url = f"{SUPABASE_URL}/rest/v1/garments?id=eq.{supabase_uuid}"
    res = requests.patch(url, headers=SUPABASE_HEADERS, json={"slug": slug}, timeout=15)
    if not res.ok:
        print(f"  WARNING: failed to update Supabase slug for {supabase_uuid}: {res.status_code} {res.text[:200]}")


def update_supabase_garment_image(supabase_uuid, image_url):
    if not image_url:
        return
    if DRY_RUN:
        print(f"  [DRY RUN] would update Supabase garments.image_url for {supabase_uuid} -> {image_url}")
        return
    url = f"{SUPABASE_URL}/rest/v1/garments?id=eq.{supabase_uuid}"
    res = requests.patch(url, headers=SUPABASE_HEADERS, json={"image_url": image_url}, timeout=15)
    if not res.ok:
        print(f"  WARNING: failed to update Supabase image_url for {supabase_uuid}: {res.status_code} {res.text[:200]}")


def update_supabase_garment_webflow_id(supabase_uuid, webflow_item_id):
    if not webflow_item_id:
        return
    if DRY_RUN:
        print(f"  [DRY RUN] would update Supabase garments.webflow_item_id for {supabase_uuid} -> {webflow_item_id}")
        return
    url = f"{SUPABASE_URL}/rest/v1/garments?id=eq.{supabase_uuid}"
    res = requests.patch(url, headers=SUPABASE_HEADERS, json={"webflow_item_id": webflow_item_id}, timeout=15)
    if not res.ok:
        print(f"  WARNING: failed to update Supabase webflow_item_id for {supabase_uuid}: {res.status_code} {res.text[:200]}")


# ── Cloudflare KV helper (redirects) ─────────────────────────────────────
def kv_write_redirect(old_slug, new_slug, path_prefix="/garments/"):
    key = f"redirect:{path_prefix}{old_slug}"
    value = f"{path_prefix}{new_slug}"
    if DRY_RUN:
        print(f"  [DRY RUN] would write KV redirect: {key} -> {value}")
        return
    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}"
        f"/storage/kv/namespaces/{CLOUDFLARE_KV_NAMESPACE_ID}/values/{key}"
    )
    res = requests.put(
        url,
        headers={"Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}"},
        data=value,
        timeout=15,
    )
    if not res.ok:
        print(f"  WARNING: failed to write redirect {old_slug} -> {new_slug}: {res.status_code} {res.text}")


# ── Slug logic ────────────────────────────────────────────────────────────
def strip_accents(text):
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", "ignore").decode("ascii")


def slugify(text):
    text = (text or "").strip()
    text = strip_accents(text)
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    text = text.strip("-")
    return text[:80]


def base_slug_for_garment(name_formula, fallback_name):
    base = name_formula or fallback_name or ""
    slug = slugify(base)
    if not slug:
        slug = slugify(fallback_name or "garment")
    return slug


def build_slug(record_id, name_formula, fallback_name, product_code, existing_slugs, self_wf_id=None):
    slug = base_slug_for_garment(name_formula, fallback_name)
    if len(slug) < 3:
        slug = f"{slug}-{record_id[-8:].lower()}"

    def taken(candidate):
        owner = existing_slugs.get(candidate)
        return owner is not None and owner != self_wf_id

    if not taken(slug):
        return slug

    if product_code:
        candidate = f"{slug}-{slugify(product_code)}"
        if candidate and not taken(candidate):
            return candidate

    suffix = 2
    candidate = f"{slug}-{suffix}"
    while taken(candidate):
        suffix += 1
        candidate = f"{slug}-{suffix}"
    return candidate


# ── Webflow helpers ───────────────────────────────────────────────────────
def webflow_request(method, url, json_body=None, params=None, max_retries=5, allow_404=False):
    for attempt in range(max_retries):
        res = requests.request(method, url, headers=WEBFLOW_HEADERS,
                               json=json_body, params=params, timeout=30)
        if allow_404 and res.status_code == 404:
            return None
        if res.ok:
            return res
        if res.status_code == 429 or res.status_code >= 500:
            wait = int(res.headers.get("Retry-After", 5 * (attempt + 1)))
            print(f"  Webflow {res.status_code} on {method} {url} — waiting {wait}s, retry {attempt + 1}/{max_retries}")
            time.sleep(wait + 1)
            continue
        raise requests.exceptions.HTTPError(
            f"{res.status_code} on {method} {url}: {res.text[:500]} | payload: {json_body}",
            response=res,
        )
    raise requests.exceptions.HTTPError(
        f"Exhausted {max_retries} retries on {method} {url} (repeated 429/5xx): {json_body}"
    )


def webflow_create_item(collection_id, field_data):
    if DRY_RUN:
        fake_id = f"dryrun-{abs(hash((collection_id, field_data.get('name'), field_data.get('slug')))) % 10**8}"
        print(f"  [DRY RUN] would CREATE item in {collection_id}: {field_data}")
        return {"id": fake_id, "fieldData": field_data}
    res = webflow_request("POST", f"https://api.webflow.com/v2/collections/{collection_id}/items",
                           json_body={"fieldData": field_data})
    return res.json()


def webflow_update_item(collection_id, item_id, field_data):
    if DRY_RUN:
        print(f"  [DRY RUN] would UPDATE item {item_id} in {collection_id}: {field_data}")
        return {"id": item_id, "fieldData": field_data}
    res = webflow_request("PATCH", f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}",
                           json_body={"fieldData": field_data})
    return res.json()


def webflow_get_item(collection_id, item_id):
    res = webflow_request(
        "GET",
        f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}",
        allow_404=True,
    )
    return res.json() if res is not None else None


def webflow_undraft_item(collection_id, item_id):
    if DRY_RUN:
        print(f"  [DRY RUN] would un-draft item {item_id} in {collection_id}")
        return {"id": item_id, "isDraft": False}
    res = webflow_request("PATCH", f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}",
                           json_body={"isDraft": False})
    return res.json()


def webflow_site_publish():
    if DRY_RUN:
        print("  [DRY RUN] would trigger single site-wide publish")
        return {"queued": True, "dryRun": True}

    site_res = webflow_request("GET", f"https://api.webflow.com/v2/sites/{WEBFLOW_SITE_ID}")
    site_data = site_res.json()
    custom_domain_ids = [d["id"] for d in site_data.get("customDomains", [])]

    if not custom_domain_ids:
        print("  WARNING: no custom domains found on this site — falling back to "
              "publishToWebflowSubdomain:True so at least the staging domain publishes.")
        payload = {"publishToWebflowSubdomain": True}
    else:
        payload = {"customDomains": custom_domain_ids, "publishToWebflowSubdomain": False}

    res = webflow_request("POST", f"https://api.webflow.com/v2/sites/{WEBFLOW_SITE_ID}/publish",
                           json_body=payload)
    return res.json()


def webflow_all_items(collection_id):
    items, offset, limit = [], 0, 100
    while True:
        res = webflow_request(
            "GET",
            f"https://api.webflow.com/v2/collections/{collection_id}/items",
            params={"limit": limit, "offset": offset},
        )
        data = res.json()
        page_items = data.get("items", [])
        items.extend(page_items)
        total = data.get("pagination", {}).get("total", 0)
        offset += len(page_items)
        if offset >= total or not page_items:
            break
        time.sleep(WEBFLOW_PAGINATION_SLEEP)
    return items


# ── Step 1: dynamic designer scope ───────────────────────────────────────
def get_in_feed_designers():
    records = airtable_fetch_all(
        DESIGNERS_TABLE,
        filter_formula="{In Feed}=1",
        fields=["Designer Name"],
    )
    names = [r["fields"].get("Designer Name") for r in records if r["fields"].get("Designer Name")]
    print(f"In-feed designers ({len(names)}): {', '.join(names)}")
    return set(names)


# ── Step 2: which garments qualify for a Webflow page ────────────────────
def get_qualifying_garment_ids():
    records = airtable_fetch_all(
        SIGHTINGS_TABLE,
        filter_formula='OR({Status}="Active",{Status}="Sold")',
        fields=["Garment"],
    )
    ids = set()
    for r in records:
        garment_link = r["fields"].get("Garment") or []
        ids.update(garment_link)
    return ids


def get_qualifying_garments(in_feed_designers):
    sighted_ids = get_qualifying_garment_ids()
    print(f"Garments with an Active/Sold sighting: {len(sighted_ids)}")

    all_garments = airtable_fetch_all(
        GARMENTS_TABLE,
        fields=[
            FLD_GARMENT_NAME, FLD_NAME_FORMULA, FLD_DESIGNER, FLD_COLLECTION,
            FLD_PRODUCT_CODE, FLD_PRODUCT_COLOUR, FLD_CATEGORY, FLD_RRP,
            FLD_AIRTABLE_SLUG, FLD_WEBFLOW_ITEM_ID,
        ] + list(IMAGE_FIELD_MAP.keys()),
    )

    qualifying = []
    for r in all_garments:
        f = r["fields"]
        designer = f.get(FLD_DESIGNER)
        if designer not in in_feed_designers:
            continue
        has_collection = bool(f.get(FLD_COLLECTION))
        has_image = bool(f.get(FLD_IMAGE_1))
        if has_collection and has_image:
            qualifying.append(r)

    print(f"Qualifying garments (Collection assigned AND Image 1, in-feed designers): {len(qualifying)}")

    if SIGHTED_ONLY:
        qualifying = [r for r in qualifying if r["id"] in sighted_ids]
        print(f"  Narrowed by SIGHTED_ONLY: {len(qualifying)} remain")

    if DESIGNER_FILTER:
        qualifying = [r for r in qualifying if r["fields"].get(FLD_DESIGNER) in DESIGNER_FILTER]
        print(f"  Narrowed by DESIGNER_FILTER={sorted(DESIGNER_FILTER)}: {len(qualifying)} remain")

    if LIMIT:
        qualifying = qualifying[:LIMIT]
        print(f"  Capped by LIMIT={LIMIT}: {len(qualifying)} will actually be processed")

    return qualifying


# ── Step 3: sync Designers (no drafting, create-once) ────────────────────
def sync_designers():
    records = airtable_fetch_all(
        DESIGNERS_TABLE,
        filter_formula="{In Feed}=1",
        fields=[FLD_DESIGNER_NAME, FLD_DESIGNER_WEBFLOW_ID],
    )

    designer_webflow_ids = {}
    failures = []

    for record in records:
        f = record["fields"]
        name = f.get(FLD_DESIGNER_NAME)
        if not name:
            continue

        airtable_id = record["id"]
        try:
            existing_wf_id = f.get(FLD_DESIGNER_WEBFLOW_ID)

            field_data = {
                WF_DESIGNER_FIELD_NAME: name,
                WF_DESIGNER_FIELD_SLUG: slugify(name),
            }

            if existing_wf_id:
                existing = webflow_get_item(DESIGNERS_COLLECTION_ID, existing_wf_id)
                if existing:
                    webflow_update_item(DESIGNERS_COLLECTION_ID, existing_wf_id, field_data)
                    designer_webflow_ids[name] = existing_wf_id
                    continue

            created = webflow_create_item(DESIGNERS_COLLECTION_ID, field_data)
            new_id = created["id"]
            airtable_update(DESIGNERS_TABLE, airtable_id, {FLD_DESIGNER_WEBFLOW_ID: new_id})
            designer_webflow_ids[name] = new_id
            webflow_undraft_item(DESIGNERS_COLLECTION_ID, new_id)

        except Exception as e:
            failures.append({"airtable_id": airtable_id, "name": name, "error": str(e)})
            print(f"  FAILED designer {airtable_id} ({name}): {e}")

        time.sleep(0.2)

    print(f"Designers synced: {len(designer_webflow_ids)}")
    if failures:
        print(f"Designers FAILED: {len(failures)}")
        for fail in failures:
            print(f"  - {fail['airtable_id']} ({fail['name']}): {fail['error'][:200]}")
    return designer_webflow_ids


# ── Step 4: sync Campaigns (no drafting, create-once) ────────────────────
def sync_campaigns(qualifying_garments):
    collection_ids_needed = set()
    for r in qualifying_garments:
        links = r["fields"].get(FLD_COLLECTION) or []
        collection_ids_needed.update(links)

    if not collection_ids_needed:
        return {}

    campaign_records = {
        r["id"]: r for r in airtable_fetch_all(COLLECTIONS_TABLE)
        if r["id"] in collection_ids_needed
    }

    campaign_webflow_ids = {}
    failures = []

    for airtable_id, record in campaign_records.items():
        f = record["fields"]
        collection_name = f.get("Collection Name", "")
        try:
            existing_wf_id = f.get(FLD_WEBFLOW_ITEM_ID)

            designer_raw = f.get("Designer Name") or f.get("Designer") or ""
            designer_name = ", ".join(designer_raw) if isinstance(designer_raw, list) else designer_raw

            field_data = {
                WF_CAMPAIGN_FIELD_NAME: collection_name,
                WF_CAMPAIGN_FIELD_SLUG: slugify(f"{designer_name} {collection_name}"),
                WF_CAMPAIGN_FIELD_AIRTABLE_ID: airtable_id,
                WF_CAMPAIGN_FIELD_DESIGNER_NAME: designer_name,
                WF_CAMPAIGN_FIELD_SEASON_CODE: f.get("Season Code", ""),
            }

            if existing_wf_id:
                existing = webflow_get_item(CAMPAIGNS_COLLECTION_ID, existing_wf_id)
                if existing:
                    old_slug = existing.get("fieldData", {}).get(WF_CAMPAIGN_FIELD_SLUG)
                    new_slug = field_data[WF_CAMPAIGN_FIELD_SLUG]
                    webflow_update_item(CAMPAIGNS_COLLECTION_ID, existing_wf_id, field_data)
                    campaign_webflow_ids[airtable_id] = existing_wf_id
                    if old_slug and old_slug != new_slug:
                        kv_write_redirect(old_slug, new_slug, path_prefix=CAMPAIGN_URL_PREFIX)
                        print(f"  Campaign slug changed: {old_slug} -> {new_slug} (redirect written)")
                    continue

            created = webflow_create_item(CAMPAIGNS_COLLECTION_ID, field_data)
            new_id = created["id"]
            airtable_update(COLLECTIONS_TABLE, airtable_id, {FLD_WEBFLOW_ITEM_ID: new_id})
            campaign_webflow_ids[airtable_id] = new_id
            webflow_undraft_item(CAMPAIGNS_COLLECTION_ID, new_id)

        except Exception as e:
            failures.append({"airtable_id": airtable_id, "name": collection_name, "error": str(e)})
            print(f"  FAILED campaign {airtable_id} ({collection_name}): {e}")

        time.sleep(0.2)

    print(f"Campaigns synced: {len(campaign_webflow_ids)}")
    if failures:
        print(f"Campaigns FAILED: {len(failures)}")
        for fail in failures:
            print(f"  - {fail['airtable_id']} ({fail['name']}): {fail['error'][:200]}")
    return campaign_webflow_ids


# ── Step 4.5: reconcile already-orphaned items ────────────────────────────
def reconcile_orphaned_items(existing_items):
    print("Reconciling orphaned Webflow items against Airtable...")

    webflow_by_airtable_id = {}
    for item in existing_items:
        fd = item.get("fieldData", {})
        art_id = fd.get(WF_FIELD_AIRTABLE_ID)
        if not art_id:
            continue
        if art_id in webflow_by_airtable_id and webflow_by_airtable_id[art_id] != item["id"]:
            print(f"  WARNING: multiple live Webflow items reference Airtable record {art_id} "
                  f"({webflow_by_airtable_id[art_id]} and {item['id']}) — needs manual review, skipping.")
            continue
        webflow_by_airtable_id[art_id] = item["id"]

    if not webflow_by_airtable_id:
        print("  No Airtable-record-ID-tagged items found — nothing to reconcile.")
        return

    airtable_rows = airtable_fetch_all(
        GARMENTS_TABLE,
        filter_formula=f"{{{FLD_WEBFLOW_ITEM_ID}}}=''",
        fields=[FLD_GARMENT_NAME, FLD_WEBFLOW_ITEM_ID],
    )

    reconciled = 0
    for r in airtable_rows:
        wf_id = webflow_by_airtable_id.get(r["id"])
        if not wf_id:
            continue
        name = r["fields"].get(FLD_GARMENT_NAME, "(unnamed)")
        print(f"  RECONCILED: {r['id']} ({name}) — Airtable was blank, "
              f"live Webflow item {wf_id} already references it. Backfilling.")
        airtable_update(GARMENTS_TABLE, r["id"], {FLD_WEBFLOW_ITEM_ID: wf_id})
        reconciled += 1

    print(f"Reconciliation complete: {reconciled} Airtable record(s) backfilled.")


# ── Step 5: sync Garments ─────────────────────────────────────────────────
def sync_garments(qualifying_garments, campaign_webflow_ids, designer_webflow_ids):
    if not qualifying_garments:
        print("No garments to process this run — skipping slug preload and garment sync.")
        return [], []

    print("Loading existing Webflow garment slugs for collision checking...")
    existing_items = webflow_all_items(GARMENTS_COLLECTION_ID)
    existing_slugs = {}
    for item in existing_items:
        s = item.get("fieldData", {}).get(WF_FIELD_SLUG)
        if s:
            existing_slugs[s] = item["id"]
    print(f"  {len(existing_slugs)} existing slugs loaded from {len(existing_items)} live items")

    reconcile_orphaned_items(existing_items)

    synced = []
    failures = []

    for r in qualifying_garments:
        airtable_id = r["id"]
        f = r["fields"]
        name_formula = f.get(FLD_NAME_FORMULA, "")
        garment_name = f.get(FLD_GARMENT_NAME, "")
        display_name = name_formula or garment_name or "(unnamed)"

        try:
            existing_wf_id = f.get(FLD_WEBFLOW_ITEM_ID)
            existing_item = None

            if not existing_wf_id:
                candidate_base = base_slug_for_garment(name_formula, garment_name)
                adopted_wf_id = existing_slugs.get(candidate_base)
                if adopted_wf_id:
                    fetched = webflow_get_item(GARMENTS_COLLECTION_ID, adopted_wf_id)
                    if fetched:
                        existing_wf_id = adopted_wf_id
                        existing_item = fetched
                        print(f"  RECOVERED: garment {airtable_id} ({display_name}) had blank "
                              f"Webflow Item ID but slug '{candidate_base}' already exists as "
                              f"{adopted_wf_id} — adopting instead of creating a duplicate.")
                        airtable_update(GARMENTS_TABLE, airtable_id, {FLD_WEBFLOW_ITEM_ID: adopted_wf_id})

            new_slug = build_slug(
                airtable_id, name_formula, garment_name, f.get(FLD_PRODUCT_CODE, ""),
                existing_slugs, self_wf_id=existing_wf_id,
            )

            if existing_wf_id and existing_item is None:
                existing_item = webflow_get_item(GARMENTS_COLLECTION_ID, existing_wf_id)

            if not existing_item and new_slug in existing_slugs:
                adopted_wf_id = existing_slugs[new_slug]
                fetched = webflow_get_item(GARMENTS_COLLECTION_ID, adopted_wf_id)
                if fetched:
                    existing_wf_id = adopted_wf_id
                    existing_item = fetched
                    print(f"  RECOVERED (secondary check): garment {airtable_id} ({display_name}) — "
                          f"slug '{new_slug}' already exists as {adopted_wf_id} — adopting.")
                    airtable_update(GARMENTS_TABLE, airtable_id, {FLD_WEBFLOW_ITEM_ID: adopted_wf_id})

            supabase_uuid = get_supabase_garment_uuid(airtable_id)
            if supabase_uuid:
                update_supabase_garment_slug(supabase_uuid, new_slug)

            campaign_wf_id = None
            collection_links = f.get(FLD_COLLECTION) or []
            if collection_links:
                campaign_wf_id = campaign_webflow_ids.get(collection_links[0])

            designer_name = f.get(FLD_DESIGNER, "")
            designer_wf_id = designer_webflow_ids.get(designer_name)
            if designer_name and not designer_wf_id:
                print(f"  WARNING: designer '{designer_name}' has no Webflow item — "
                      f"garment {airtable_id} will save with no Designer reference. "
                      f"Check spelling/whitespace match against the Designers table.")

            field_data = {
                WF_FIELD_NAME: name_formula or garment_name,
                WF_FIELD_SLUG: new_slug,
                WF_FIELD_PRODUCT_CODE: f.get(FLD_PRODUCT_CODE, ""),
                WF_FIELD_RRP: f.get(FLD_RRP),
                WF_FIELD_PRODUCT_COLOUR: f.get(FLD_PRODUCT_COLOUR, ""),
                WF_FIELD_CATEGORY: f.get(FLD_CATEGORY, ""),
                WF_FIELD_AIRTABLE_ID: airtable_id,
            }
            if supabase_uuid:
                field_data[WF_FIELD_SUPABASE_ID] = supabase_uuid
            if campaign_wf_id:
                field_data[WF_FIELD_CAMPAIGN_REF] = campaign_wf_id
            if designer_wf_id:
                field_data[WF_FIELD_DESIGNER_REF] = designer_wf_id

            # 2026-08 FIX (revision note 9): loop over every Airtable image
            # field -> Webflow slug pair, instead of only ever sending
            # Image 1. image_urls collects what was actually sent, keyed
            # by Airtable field name, so the post-write CDN-URL swap below
            # can do the same "prefer Webflow's own hosted URL" fix for
            # every image slot, not just the first.
            image_urls = {}
            for at_field, wf_slug in IMAGE_FIELD_MAP.items():
                img_field = f.get(at_field)
                if img_field and isinstance(img_field, list) and img_field[0].get("url"):
                    url = img_field[0]["url"]
                    image_urls[at_field] = url
                    field_data[wf_slug] = {"url": url}

            if existing_item:
                old_slug = existing_item.get("fieldData", {}).get(WF_FIELD_SLUG)
                update_res = webflow_update_item(GARMENTS_COLLECTION_ID, existing_wf_id, field_data)
                wf_item_id = existing_wf_id
                if old_slug and old_slug != new_slug:
                    kv_write_redirect(old_slug, new_slug)
                    print(f"  Slug changed: {old_slug} -> {new_slug} (redirect written)")
            else:
                update_res = webflow_create_item(GARMENTS_COLLECTION_ID, field_data)
                wf_item_id = update_res["id"]
                airtable_update(GARMENTS_TABLE, airtable_id, {FLD_WEBFLOW_ITEM_ID: wf_item_id})
                webflow_undraft_item(GARMENTS_COLLECTION_ID, wf_item_id)

            # Prefer Webflow's own hosted CDN URL over Airtable's signed,
            # expiring attachment URL — for EVERY image slot that was
            # sent, not just main-photo. Webflow echoes the resolved CDN
            # URL back in fieldData for each image field once attached.
            response_field_data = (update_res or {}).get("fieldData", {})
            for at_field, wf_slug in IMAGE_FIELD_MAP.items():
                if at_field not in image_urls:
                    continue
                echoed = response_field_data.get(wf_slug, {})
                if isinstance(echoed, dict) and echoed.get("url"):
                    image_urls[at_field] = echoed["url"]

            # Keep the existing single "image_url" as Image 1's URL (main-
            # photo) for backward compatibility with garments.json/
            # Supabase consumers that only ever expected one image.
            image_url = image_urls.get(FLD_IMAGE_1)

            if supabase_uuid:
                update_supabase_garment_image(supabase_uuid, image_url)
                update_supabase_garment_webflow_id(supabase_uuid, wf_item_id)

            existing_slugs[new_slug] = wf_item_id

            synced.append({
                "airtable_id": airtable_id,
                "webflow_item_id": wf_item_id,
                "supabase_garment_id": supabase_uuid,
                "name": name_formula or garment_name,
                "slug": new_slug,
                "product_code": f.get(FLD_PRODUCT_CODE, ""),
                "designer": f.get(FLD_DESIGNER, ""),
                "category": f.get(FLD_CATEGORY, ""),
                "colour": f.get(FLD_PRODUCT_COLOUR, ""),
                "rrp": f.get(FLD_RRP),
                "image_url": image_url,
                # 2026-08 (revision note 9): every synced image, in
                # Airtable-field-name order, for any future consumer
                # (garments.json, garment page gallery) that wants more
                # than just the first photo.
                "image_urls": [image_urls[k] for k in IMAGE_FIELD_MAP if k in image_urls],
            })

        except Exception as e:
            failures.append({"airtable_id": airtable_id, "name": display_name, "error": str(e)})
            print(f"  FAILED garment {airtable_id} ({display_name}): {e}")

        time.sleep(0.15)

    print(f"Garments synced: {len(synced)}")
    if failures:
        print(f"Garments FAILED: {len(failures)}")
        for fail in failures[:20]:
            print(f"  - {fail['airtable_id']} ({fail['name']}): {fail['error'][:200]}")
        if len(failures) > 20:
            print(f"  ...and {len(failures) - 20} more")
    return synced, failures


# ── Step 5: single site-wide publish ──────────────────────────────────────
def publish_site():
    print("Publishing site (single site-wide call)...")
    webflow_site_publish()
    print("Publish complete.")


# ── Step 6: garments.json export (merge, not overwrite) ──────────────────
def export_garments_json(synced_garments):
    existing = []
    try:
        with open(GARMENTS_JSON_PATH) as fp:
            existing = json.load(fp)
        if not isinstance(existing, list):
            print(f"WARNING: {GARMENTS_JSON_PATH} wasn't a list — starting fresh.")
            existing = []
    except FileNotFoundError:
        pass
    except (json.JSONDecodeError, OSError) as e:
        print(f"WARNING: couldn't read existing {GARMENTS_JSON_PATH} ({e}) — starting fresh.")
        existing = []

    merged = {g["id"]: g for g in existing if isinstance(g, dict) and g.get("id")}

    for g in synced_garments:
        merged[g["airtable_id"]] = {
            "id": g["airtable_id"],
            "webflow_item_id": g["webflow_item_id"],
            "name": g["name"],
            "slug": g["slug"],
            "designer": g["designer"],
            "category": g["category"],
            "colour": g["colour"],
            "product_code": g["product_code"],
            "rrp": g["rrp"],
            "image": g["image_url"],
            "images": g.get("image_urls", []),
            "search": " ".join(filter(None, [g["designer"], g["name"], g["colour"], g["product_code"]])).lower(),
        }

    payload = list(merged.values())
    with open(GARMENTS_JSON_PATH, "w") as fp:
        json.dump(payload, fp, indent=2)

    print(f"Wrote {len(payload)} garments to {GARMENTS_JSON_PATH} "
          f"({len(synced_garments)} updated this run, {len(payload) - len(synced_garments)} carried over)")


# ── Main ───────────────────────────────────────────────────────────────────
def main():
    print("=== webflow_sync.py — starting ===")
    if DRY_RUN:
        print("*** DRY RUN MODE — no Webflow/Airtable/KV writes will actually happen ***")

    run_started_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    last_sync_iso = read_last_sync_timestamp()

    in_feed_designers = get_in_feed_designers()
    if not in_feed_designers:
        print("No in-feed designers found — aborting to avoid wiping scope unintentionally.")
        sys.exit(1)

    qualifying_garments = get_qualifying_garments(in_feed_designers)
    if not qualifying_garments:
        print("No qualifying garments found — aborting, this looks wrong.")
        sys.exit(1)

    campaign_webflow_ids = sync_campaigns(qualifying_garments)
    designer_webflow_ids = sync_designers()

    if last_sync_iso:
        modified_ids = get_modified_garment_ids(last_sync_iso)
        garments_to_process = [r for r in qualifying_garments if r["id"] in modified_ids]
        print(f"Incremental mode (since {last_sync_iso}): "
              f"{len(garments_to_process)} of {len(qualifying_garments)} qualifying garments "
              f"modified/new — the rest are skipped.")
    else:
        garments_to_process = qualifying_garments
        print(f"Full-sync mode: all {len(garments_to_process)} qualifying garments will be processed.")

    synced_garments, garment_failures = sync_garments(
        garments_to_process, campaign_webflow_ids, designer_webflow_ids
    )

    export_garments_json(synced_garments)

    if not garment_failures:
        write_last_sync_timestamp(run_started_iso)
    else:
        print(f"NOT advancing {LAST_SYNC_PATH}: {len(garment_failures)} garment failure(s) — "
              f"this window will be retried in full on the next run.")

    try:
        publish_site()
    except Exception as e:
        print(f"WARNING: site publish failed, but garments.json was already exported successfully: {e}")
        print("Garments/Campaigns/Designers were still created/updated in Webflow — they just haven't")
        print("been published live yet. Re-run publish manually, or it'll retry on the next scheduled sync.")

    print("=== webflow_sync.py — done ===")


if __name__ == "__main__":
    main()
