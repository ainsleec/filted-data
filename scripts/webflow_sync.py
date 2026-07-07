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
"""

import os
import re
import sys
import time
import json
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


# ── Airtable helpers ─────────────────────────────────────────────────────
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
    return records


def airtable_update(table, record_id, fields):
    if DRY_RUN:
        print(f"  [DRY RUN] would update Airtable {table}/{record_id} with {fields}")
        return {"id": record_id, "fields": fields}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{requests.utils.quote(table)}/{record_id}"
    res = requests.patch(url, headers=AIRTABLE_HEADERS, json={"fields": fields}, timeout=30)
    res.raise_for_status()
    return res.json()


# ── Supabase helper ──────────────────────────────────────────────────────
def get_supabase_garment_uuid(airtable_id):
    """Look up the Supabase garments.id (uuid) for a given Airtable record ID.
    Returns None if no matching row exists yet (expected for garments that
    only qualify via Product Code and have never had a sighting synced)."""
    url = f"{SUPABASE_URL}/rest/v1/garments?airtable_id=eq.{airtable_id}&select=id"
    res = requests.get(url, headers=SUPABASE_HEADERS, timeout=15)
    res.raise_for_status()
    rows = res.json()
    return rows[0]["id"] if rows else None


# ── Cloudflare KV helper (redirects) ─────────────────────────────────────
def kv_write_redirect(old_slug, new_slug):
    """Writes redirect:/garments/{old_slug} -> /garments/{new_slug} so any
    already-indexed/bookmarked URL 301s instead of 404ing. Uses the same
    key scheme worker.js already reads on every /garments/* request."""
    key = f"redirect:/garments/{old_slug}"
    value = f"/garments/{new_slug}"
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
def slugify(text):
    text = (text or "").lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    text = text.strip("-")
    return text[:80]


def build_slug(record_id, name_formula, fallback_name):
    """Single source of truth: slugify the Name (Formula) field.

    Deliberately does NOT append Product Colour or Product Code
    separately — Name (Formula) is trusted as-is, whatever it produces
    (with colour appended when present, bare name when not — e.g. for
    Alemais, which has no Product Colour data at all).

    IMPORTANT PRE-CONDITION, verified manually before each run rather
    than enforced in code: this assumes no two garments from the same
    designer share an identical Garment Name where colour is also blank
    (which would produce a genuine slug collision, since nothing else
    would disambiguate them). Checked via an Airtable extension before
    running — if that check ever turns up a duplicate, this function
    will need a fallback again (e.g. Product Code) for that specific
    case rather than reintroducing one globally.

    If Name (Formula) is somehow blank, falls back to the raw Garment
    Name plus a short id suffix so slugs never collide outright."""
    base = name_formula or fallback_name or ""
    slug = slugify(base)
    if not slug:
        slug = slugify(fallback_name or "garment")
    if len(slug) < 3:
        slug = f"{slug}-{record_id[-8:].lower()}"
    return slug


# ── Webflow helpers ───────────────────────────────────────────────────────
def webflow_create_item(collection_id, field_data):
    if DRY_RUN:
        fake_id = f"dryrun-{abs(hash((collection_id, field_data.get('name'), field_data.get('slug')))) % 10**8}"
        print(f"  [DRY RUN] would CREATE item in {collection_id}: {field_data}")
        return {"id": fake_id, "fieldData": field_data}
    url = f"https://api.webflow.com/v2/collections/{collection_id}/items"
    res = requests.post(url, headers=WEBFLOW_HEADERS, json={"fieldData": field_data}, timeout=30)
    if not res.ok:
        raise requests.exceptions.HTTPError(
            f"{res.status_code} on CREATE in {collection_id}: {res.text[:500]} | payload: {field_data}",
            response=res,
        )
    return res.json()


def webflow_update_item(collection_id, item_id, field_data):
    if DRY_RUN:
        print(f"  [DRY RUN] would UPDATE item {item_id} in {collection_id}: {field_data}")
        return {"id": item_id, "fieldData": field_data}
    url = f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}"
    res = requests.patch(url, headers=WEBFLOW_HEADERS, json={"fieldData": field_data}, timeout=30)
    if not res.ok:
        raise requests.exceptions.HTTPError(
            f"{res.status_code} on UPDATE {item_id} in {collection_id}: {res.text[:500]} | payload: {field_data}",
            response=res,
        )
    return res.json()


def webflow_get_item(collection_id, item_id):
    url = f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}"
    res = requests.get(url, headers=WEBFLOW_HEADERS, timeout=30)
    if res.status_code == 404:
        return None
    res.raise_for_status()
    return res.json()


def webflow_undraft_item(collection_id, item_id):
    """Un-draft a single item. Uses the individual item PATCH endpoint —
    NEVER the per-collection /items/publish batch endpoint, which is what
    caused the ~2,862-failure rate-limit incident during the last rebuild
    attempt."""
    if DRY_RUN:
        print(f"  [DRY RUN] would un-draft item {item_id} in {collection_id}")
        return {"id": item_id, "isDraft": False}
    url = f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}"
    res = requests.patch(url, headers=WEBFLOW_HEADERS, json={"isDraft": False}, timeout=30)
    res.raise_for_status()
    return res.json()


def webflow_site_publish():
    """ONE single site-wide publish call at the very end of the run —
    not per-item, not per-collection batch."""
    if DRY_RUN:
        print("  [DRY RUN] would trigger single site-wide publish")
        return {"queued": True, "dryRun": True}
    url = f"https://api.webflow.com/v2/sites/{WEBFLOW_SITE_ID}/publish"
    res = requests.post(url, headers=WEBFLOW_HEADERS, json={"publishToWebflowSubdomain": False}, timeout=60)
    res.raise_for_status()
    return res.json()


def webflow_all_items(collection_id):
    items, offset, limit = [], 0, 100
    while True:
        url = f"https://api.webflow.com/v2/collections/{collection_id}/items"
        res = requests.get(url, headers=WEBFLOW_HEADERS, params={"limit": limit, "offset": offset}, timeout=30)
        res.raise_for_status()
        data = res.json()
        items.extend(data.get("items", []))
        total = data.get("pagination", {}).get("total", 0)
        offset += len(data.get("items", []))
        if offset >= total or not data.get("items"):
            break
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
    """Returns the set of Airtable Garment record IDs that currently have
    an Active or Sold sighting — used to combine with the Product Code
    condition below."""
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
            FLD_IMAGE_1, FLD_AIRTABLE_SLUG, FLD_WEBFLOW_ITEM_ID,
        ],
    )

    qualifying = []
    for r in all_garments:
        f = r["fields"]
        designer = f.get(FLD_DESIGNER)
        if designer not in in_feed_designers:
            continue
        has_sighting = r["id"] in sighted_ids
        has_code = bool((f.get(FLD_PRODUCT_CODE) or "").strip())
        has_image = bool(f.get(FLD_IMAGE_1))  # Airtable attachment field: empty list/None if no image
        # Sighting history alone qualifies regardless of image (that data's
        # rarer/more valuable than a photo). Product Code alone is NOT
        # enough on its own anymore — a page created off code alone with
        # no image would be a thin, imageless result. Requiring both for
        # that path avoids publishing pages with nothing to show.
        if has_sighting or (has_code and has_image):
            qualifying.append(r)

    print(f"Qualifying garments (Active/Sold sighting, OR Product Code + Image 1, in-feed designers): {len(qualifying)}")

    if DESIGNER_FILTER:
        qualifying = [r for r in qualifying if r["fields"].get(FLD_DESIGNER) in DESIGNER_FILTER]
        print(f"  Narrowed by DESIGNER_FILTER={sorted(DESIGNER_FILTER)}: {len(qualifying)} remain")

    if LIMIT:
        qualifying = qualifying[:LIMIT]
        print(f"  Capped by LIMIT={LIMIT}: {len(qualifying)} will actually be processed")

    return qualifying


# ── Step 3: sync Designers (no drafting, create-once) ────────────────────
def sync_designers():
    """Ensures every in-feed designer has a Webflow item. Returns a map of
    designer NAME (string) -> Webflow item ID.

    Keyed by name rather than Airtable record ID because Garments.Designer
    is a plain text field, NOT a linked record to the Designers table —
    so name is the only join key available when building each garment's
    single-reference value below."""
    records = airtable_fetch_all(
        DESIGNERS_TABLE,
        filter_formula="{In Feed}=1",
        fields=[FLD_DESIGNER_NAME, FLD_DESIGNER_WEBFLOW_ID],
    )

    designer_webflow_ids = {}

    for record in records:
        f = record["fields"]
        name = f.get(FLD_DESIGNER_NAME)
        if not name:
            continue

        airtable_id = record["id"]
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
            # stale Webflow Item ID (deleted on Webflow side) — recreate below

        created = webflow_create_item(DESIGNERS_COLLECTION_ID, field_data)
        new_id = created["id"]
        airtable_update(DESIGNERS_TABLE, airtable_id, {FLD_DESIGNER_WEBFLOW_ID: new_id})
        designer_webflow_ids[name] = new_id
        webflow_undraft_item(DESIGNERS_COLLECTION_ID, new_id)
        time.sleep(0.2)

    print(f"Designers synced: {len(designer_webflow_ids)}")
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

    campaign_webflow_ids = {}  # airtable collection record id -> webflow item id

    for airtable_id, record in campaign_records.items():
        f = record["fields"]
        existing_wf_id = f.get(FLD_WEBFLOW_ITEM_ID)

        # Airtable Lookup fields always return an array, even for a single
        # linked value (e.g. ['Aje']) — but Webflow's Designer Name field is
        # Plain Text, not a list. Flatten before sending, or every campaign
        # update fails validation on a real (non-dry) run.
        designer_raw = f.get("Designer Name") or f.get("Designer") or ""
        designer_name = ", ".join(designer_raw) if isinstance(designer_raw, list) else designer_raw

        field_data = {
            WF_CAMPAIGN_FIELD_NAME: f.get("Collection Name", ""),
            WF_CAMPAIGN_FIELD_SLUG: slugify(f.get("Collection Name", "")),
            WF_CAMPAIGN_FIELD_AIRTABLE_ID: airtable_id,
            WF_CAMPAIGN_FIELD_DESIGNER_NAME: designer_name,
            WF_CAMPAIGN_FIELD_SEASON_CODE: f.get("Season Code", ""),
        }

        if existing_wf_id:
            existing = webflow_get_item(CAMPAIGNS_COLLECTION_ID, existing_wf_id)
            if existing:
                webflow_update_item(CAMPAIGNS_COLLECTION_ID, existing_wf_id, field_data)
                campaign_webflow_ids[airtable_id] = existing_wf_id
                continue
            # Webflow Item ID was stale (item deleted on Webflow side) — recreate below

        created = webflow_create_item(CAMPAIGNS_COLLECTION_ID, field_data)
        new_id = created["id"]
        airtable_update(COLLECTIONS_TABLE, airtable_id, {FLD_WEBFLOW_ITEM_ID: new_id})
        campaign_webflow_ids[airtable_id] = new_id
        webflow_undraft_item(CAMPAIGNS_COLLECTION_ID, new_id)
        time.sleep(0.2)

    print(f"Campaigns synced: {len(campaign_webflow_ids)}")
    return campaign_webflow_ids


# ── Step 5: sync Garments ─────────────────────────────────────────────────
def sync_garments(qualifying_garments, campaign_webflow_ids, designer_webflow_ids):
    synced = []
    failures = []

    for r in qualifying_garments:
        airtable_id = r["id"]
        f = r["fields"]
        name_formula = f.get(FLD_NAME_FORMULA, "")
        garment_name = f.get(FLD_GARMENT_NAME, "")
        display_name = name_formula or garment_name or "(unnamed)"

        try:
            new_slug = build_slug(airtable_id, name_formula, garment_name)

            existing_wf_id = f.get(FLD_WEBFLOW_ITEM_ID)
            existing_item = webflow_get_item(GARMENTS_COLLECTION_ID, existing_wf_id) if existing_wf_id else None

            supabase_uuid = get_supabase_garment_uuid(airtable_id)  # may be None — see module docstring

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
                field_data[WF_FIELD_DESIGNER_REF] = designer_wf_id  # single-reference — plain item ID string

            image_url = None
            img_field = f.get(FLD_IMAGE_1)
            if img_field and isinstance(img_field, list) and img_field[0].get("url"):
                image_url = img_field[0]["url"]
                field_data[WF_FIELD_IMAGE_1] = {"url": image_url}

            if existing_item:
                old_slug = existing_item.get("fieldData", {}).get(WF_FIELD_SLUG)
                webflow_update_item(GARMENTS_COLLECTION_ID, existing_wf_id, field_data)
                wf_item_id = existing_wf_id
                if old_slug and old_slug != new_slug:
                    kv_write_redirect(old_slug, new_slug)
                    print(f"  Slug changed: {old_slug} -> {new_slug} (redirect written)")
            else:
                created = webflow_create_item(GARMENTS_COLLECTION_ID, field_data)
                wf_item_id = created["id"]
                airtable_update(GARMENTS_TABLE, airtable_id, {FLD_WEBFLOW_ITEM_ID: wf_item_id})
                webflow_undraft_item(GARMENTS_COLLECTION_ID, wf_item_id)

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
                # Webflow's own asset URL — permanent, no expiry, no Supabase Storage needed
                "image_url": image_url,
            })

        except Exception as e:
            failures.append({"airtable_id": airtable_id, "name": display_name, "error": str(e)})
            print(f"  FAILED garment {airtable_id} ({display_name}): {e}")
            # Deliberately no re-raise — one bad record should never take
            # down the other ~5,000+ in the same run. See failures summary
            # at the end for what needs manual attention.

        time.sleep(0.15)

    print(f"Garments synced: {len(synced)}")
    if failures:
        print(f"Garments FAILED: {len(failures)}")
        for fail in failures[:20]:
            print(f"  - {fail['airtable_id']} ({fail['name']}): {fail['error'][:200]}")
        if len(failures) > 20:
            print(f"  ...and {len(failures) - 20} more")
    return synced


# ── Step 5: single site-wide publish ──────────────────────────────────────
def publish_site():
    print("Publishing site (single site-wide call)...")
    webflow_site_publish()
    print("Publish complete.")


# ── Step 6: garments.json export ──────────────────────────────────────────
def export_garments_json(synced_garments):
    payload = []
    for g in synced_garments:
        payload.append({
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
            "search": " ".join(filter(None, [g["designer"], g["name"], g["colour"], g["product_code"]])).lower(),
        })

    with open(GARMENTS_JSON_PATH, "w") as fp:
        json.dump(payload, fp, indent=2)

    print(f"Wrote {len(payload)} garments to {GARMENTS_JSON_PATH}")


# ── Main ───────────────────────────────────────────────────────────────────
def main():
    print("=== webflow_sync.py — starting ===")
    if DRY_RUN:
        print("*** DRY RUN MODE — no Webflow/Airtable/KV writes will actually happen ***")

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
    synced_garments = sync_garments(qualifying_garments, campaign_webflow_ids, designer_webflow_ids)

    publish_site()
    export_garments_json(synced_garments)

    print("=== webflow_sync.py — done ===")


if __name__ == "__main__":
    main()
