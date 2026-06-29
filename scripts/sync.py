"""
Filted — Airtable → Webflow Sync
Runs via GitHub Actions on a schedule.
All secrets are injected as environment variables.

Sync order:
  1. Build Designer lookup
  2. Sync Campaigns         (Airtable Collections → Webflow Campaigns)
  3. Identify garments to keep (active OR sold sightings)
  4. Sync Garments          (active + sold sightings)
  5. Delete garments with no active AND no sold sightings
  6. Sync Sightings         (Active/Sold → create/update; Expired/Removed → delete)
  7. Upload images to Supabase Storage (permanent URLs)
  8. Save last sync timestamp to GitHub

Required GitHub Secrets:
  AIRTABLE_API_KEY     — Airtable personal access token
  WEBFLOW_API_TOKEN    — Webflow API v2 token
  GH_PAT               — GitHub personal access token
  SUPABASE_URL         — Supabase project URL
  SUPABASE_SERVICE_KEY — Supabase service role key
"""

import os
import re
import json
import time
import base64
import unicodedata
import requests
from datetime import datetime, timezone
from pyairtable import Api

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_API_KEY        = os.environ["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID        = "appUk1ThnHvWwFDHG"
DESIGNERS_TABLE_ID      = "tbltYGtfVV574EOcw"
CAMPAIGNS_TABLE_ID      = "tblyVgpD1SSCrRSgQ"
GARMENTS_TABLE_ID       = "tblmqjU4WqgCzP7cR"
SIGHTINGS_TABLE_ID      = "tblw4aByS8d5djekv"

WEBFLOW_API_TOKEN       = os.environ["WEBFLOW_API_TOKEN"]
DESIGNERS_COLLECTION_ID = "687a1d0eeb0f06f63aef724f"
CAMPAIGNS_COLLECTION_ID = "68944133543f0cbb26b4aeb9"
GARMENTS_COLLECTION_ID  = "68774f3e850c7a30ebc3a0aa"
SIGHTINGS_COLLECTION_ID = "69aba8c1e497e6345c288657"

GITHUB_TOKEN            = os.environ["GH_PAT"]
GITHUB_USERNAME         = "ainsleec"
GITHUB_REPO             = "filted-data"

WEBFLOW_ID_FIELD        = "Webflow Item ID"
DESIGNER_FILTER         = ("Alemais", "Aje", "Sir.", "Roame")

WEBFLOW_HEADERS = {
    "Authorization": f"Bearer {WEBFLOW_API_TOKEN}",
    "Content-Type":  "application/json",
    "accept":        "application/json",
}

ACTIVE_STATUS  = "Active"
SOLD_STATUS    = "Sold"
KEEP_STATUSES  = {"Active", "Sold"}


# ── Helpers ───────────────────────────────────────────────────────────────────
def slugify(text: str, fallback: str = "item") -> str:
    """
    Produce a Webflow-safe slug matching ^[_a-zA-Z0-9][-_a-zA-Z0-9]*$.

    Transliterates accents (é → e) and drops anything non-ASCII (emoji ✨,
    curly quotes “ ”, en-dashes –) so they can never reach Webflow. Always
    returns a non-empty string with no leading/trailing hyphen.
    """
    if not text:
        return fallback
    # NFKD splits accented chars into base + combining mark; ascii-ignore then
    # drops the marks and any fully non-ASCII char (emoji, smart quotes, dashes).
    text = unicodedata.normalize("NFKD", str(text))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s-]", "", text)   # ascii-only now, safe to strip rest
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    text = text.strip("-")[:80].strip("-")      # re-strip in case truncation cut a hyphen
    return text or fallback


def _compose_slug(*parts: str) -> str:
    """Join slug parts, dropping empties so there's never a leading/double hyphen."""
    slug = "-".join(p for p in parts if p)
    slug = re.sub(r"-+", "-", slug).strip("-")[:80].strip("-")
    return slug


def build_garment_slug(name: str, colour: str, product_code: str, airtable_id: str) -> str:
    """Colour-based slug with product code fallback, then Airtable ID last resort."""
    name_slug   = slugify(name, fallback="")
    colour_slug = slugify(colour, fallback="")
    code_slug   = slugify(product_code, fallback="")
    id_suffix   = slugify(airtable_id[-8:], fallback="") if airtable_id else ""

    if colour_slug:
        slug = _compose_slug(name_slug, colour_slug)
    elif code_slug:
        slug = _compose_slug(name_slug, code_slug)
    else:
        slug = _compose_slug(name_slug, id_suffix)

    # Final guarantee: never empty, always pattern-valid.
    return slug or id_suffix or "garment"


def get_str(value, default=""):
    if value is None:
        return default
    if isinstance(value, list):
        return str(value[0]).strip() if value else default
    return str(value).strip()


# ── Airtable helpers ──────────────────────────────────────────────────────────
def get_all_airtable_records(table_id: str, filter_formula: str = None, fields: list = None) -> list:
    api    = Api(AIRTABLE_API_KEY)
    table  = api.table(AIRTABLE_BASE_ID, table_id)
    kwargs = {}
    if filter_formula:
        kwargs["formula"] = filter_formula
    if fields:
        kwargs["fields"] = fields
    return table.all(**kwargs)


def write_webflow_id_to_airtable(table_id: str, record_id: str, webflow_id: str):
    api   = Api(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE_ID, table_id)
    table.update(record_id, {WEBFLOW_ID_FIELD: webflow_id})


# ── Webflow helpers ───────────────────────────────────────────────────────────
def webflow_create_item(collection_id: str, fields: dict) -> str | None:
    url  = f"https://api.webflow.com/v2/collections/{collection_id}/items/live"
    resp = requests.post(url, headers=WEBFLOW_HEADERS, json={"fieldData": fields})
    if resp.status_code in (200, 201, 202):
        return resp.json().get("id")
    print(f"    ⚠️  Create failed [{resp.status_code}]: {resp.text[:300]}")
    return None


def webflow_update_item(collection_id: str, item_id: str, fields: dict) -> bool:
    url  = f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}/live"
    resp = requests.patch(url, headers=WEBFLOW_HEADERS, json={"fieldData": fields})
    if resp.status_code in (200, 202):
        return True
    print(f"    ⚠️  Update failed [{resp.status_code}]: {resp.text[:200]}")
    return False


def webflow_delete_item(collection_id: str, item_id: str) -> bool:
    url  = f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}"
    resp = requests.delete(url, headers=WEBFLOW_HEADERS)
    if resp.status_code in (200, 202, 204):
        return True
    print(f"    ⚠️  Delete failed [{resp.status_code}]: {resp.text[:200]}")
    return False


# ── GitHub last sync ──────────────────────────────────────────────────────────
LAST_SYNC_PATH = "last_sync.json"
LAST_SYNC_KEY  = "webflow_sync"

def load_last_sync() -> str | None:
    url  = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{LAST_SYNC_PATH}"
    resp = requests.get(url, headers={"Authorization": f"token {GITHUB_TOKEN}"})
    if resp.status_code == 200:
        data    = resp.json()
        content = json.loads(base64.b64decode(data["content"]).decode())
        return content.get(LAST_SYNC_KEY)
    return None


def save_last_sync(ts: datetime):
    url  = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{LAST_SYNC_PATH}"
    resp = requests.get(url, headers={"Authorization": f"token {GITHUB_TOKEN}"})
    existing = {}
    sha = None
    if resp.status_code == 200:
        data     = resp.json()
        sha      = data["sha"]
        existing = json.loads(base64.b64decode(data["content"]).decode())
    existing[LAST_SYNC_KEY] = ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    payload = {
        "message": "chore: update last sync timestamp",
        "content": base64.b64encode(json.dumps(existing).encode()).decode(),
    }
    if sha:
        payload["sha"] = sha
    requests.put(
        url,
        headers={"Authorization": f"token {GITHUB_TOKEN}", "Content-Type": "application/json"},
        json=payload,
    )


# ── Supabase image upload ─────────────────────────────────────────────────────
def upload_images_to_supabase(all_garments: list):
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not supabase_url or not supabase_key:
        print("  ⚠️  Supabase credentials not found, skipping image upload")
        return

    print("\n" + "=" * 64)
    print("  Uploading images to Supabase Storage")
    print("=" * 64)

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
    }

    uploaded = skipped = errors = 0

    for record in all_garments:
        airtable_id = record["id"]
        at_fields   = record.get("fields", {})

        img1 = at_fields.get("Image 1", [])
        if not img1:
            skipped += 1
            continue

        image_url = img1[0].get("url") if isinstance(img1[0], dict) else img1[0]
        if not image_url:
            skipped += 1
            continue

        path = f"{airtable_id}.jpg"

        # Check if already uploaded
        check = requests.get(
            f"{supabase_url}/rest/v1/garments?airtable_id=eq.{airtable_id}&select=image_url",
            headers={**headers, "Content-Type": "application/json"},
        )
        if check.status_code == 200:
            rows = check.json()
            if rows and (rows[0].get("image_url") or "").startswith(f"{supabase_url}/storage"):
                skipped += 1
                continue

        # Download from Airtable
        try:
            img_res = requests.get(image_url, timeout=15)
            if img_res.status_code != 200:
                errors += 1
                continue
        except Exception as e:
            print(f"    ⚠️  Download failed {airtable_id}: {e}")
            errors += 1
            continue

        # Upload to Supabase Storage
        upload = requests.post(
            f"{supabase_url}/storage/v1/object/garment-images/{path}",
            headers={**headers, "Content-Type": "image/jpeg", "x-upsert": "true"},
            data=img_res.content,
        )

        if upload.status_code in (200, 201):
            permanent_url = f"{supabase_url}/storage/v1/object/public/garment-images/{path}"
            requests.patch(
                f"{supabase_url}/rest/v1/garments?airtable_id=eq.{airtable_id}",
                headers={**headers, "Content-Type": "application/json", "Prefer": "return=minimal"},
                json={"image_url": permanent_url},
            )
            uploaded += 1
            if uploaded % 100 == 0:
                print(f"  {uploaded} uploaded, {skipped} skipped, {errors} errors...", flush=True)
        else:
            print(f"    ⚠️  Upload failed {airtable_id}: {upload.status_code} {upload.text[:100]}")
            errors += 1

        time.sleep(0.05)

    print(f"  Images: {uploaded} uploaded | {skipped} skipped | {errors} errors")


# ── Content generation ────────────────────────────────────────────────────────
def generate_garment_html(garments: list, designer_name: str, collection_name: str) -> str:
    if not garments:
        return ""
    rows = "\n".join(
        "<tr>"
        f"<td>{get_str(g['fields'].get('Product Code'))}</td>"
        f"<td>{get_str(g['fields'].get('Garment Name'))}</td>"
        f"<td>{get_str(g['fields'].get('Product Colour'))}</td>"
        f"<td>{get_str(g['fields'].get('Category'))}</td>"
        "</tr>"
        for g in garments
    )
    count = len(garments)
    return (
        f"<h2>{designer_name} {collection_name} — Full Garment List</h2>"
        f"<p>Complete garment reference for the {designer_name} {collection_name} collection. "
        f"{count} pieces with product codes, colourways and categories.</p>"
        f"<table>"
        f"<thead><tr><th>Code</th><th>Garment</th><th>Colourway</th><th>Category</th></tr></thead>"
        f"<tbody>{rows}</tbody>"
        f"</table>"
    )


def generate_meta_description(garments: list, designer_name: str, collection_name: str) -> str:
    count  = len(garments)
    sample = [
        get_str(g["fields"].get("Garment Name"))
        for g in garments[:4]
        if g["fields"].get("Garment Name")
    ]
    names = ", ".join(sample)
    if count > 4:
        names += " and more"
    return (
        f"Complete garment reference for {designer_name} {collection_name}. "
        f"{count} pieces with product codes and colourways — {names}."
    )


# ── Field builders ────────────────────────────────────────────────────────────
def build_campaign_fields(
    at_fields: dict,
    airtable_record_id: str,
    designer_webflow_id: str | None,
    garments: list,
    designer_name: str,
) -> dict:
    name = get_str(at_fields.get("Collection Name"))
    if not name:
        return {}

    fields = {
        "name":                   name,
        "slug":                   slugify(get_str(at_fields.get("Slug")) or name),
        "season-code":            get_str(at_fields.get("Season Code")),
        "airtable-collection-id": airtable_record_id,
        "designer-name":          designer_name,
        "editorial-content":      generate_garment_html(garments, designer_name, name),
        "meta-description":       generate_meta_description(garments, designer_name, name),
    }

    if designer_webflow_id:
        fields["designer"] = designer_webflow_id

    return fields


def build_garment_fields(
    at_fields: dict,
    airtable_id: str,
    campaign_webflow_id: str | None,
    designer_webflow_id: str | None,
) -> dict:
    name = get_str(at_fields.get("Garment Name"))
    if not name:
        return {}

    slug_base    = get_str(at_fields.get("Webflow Slug")) or name
    product_code = get_str(at_fields.get("Product Code"))
    colour       = get_str(at_fields.get("Product Colour"))

    fields = {
        "name":           name,
        "slug":           build_garment_slug(slug_base, colour, product_code, airtable_id),
        "style-id":       product_code,
        "product-colour": colour,
        "category-3":     get_str(at_fields.get("Category")),
        "rrp":            at_fields.get("RRP") or 0,
    }

    if campaign_webflow_id:
        fields["campaign-2"] = campaign_webflow_id
    if designer_webflow_id:
        fields["designers"] = designer_webflow_id

    img1 = at_fields.get("Image 1", [])
    if img1:
        fields["main-photo"] = {"url": img1[0].get("url"), "alt": name}

    img2 = at_fields.get("Image 2", [])
    if img2:
        fields["image-2"] = {"url": img2[0].get("url"), "alt": name}

    return fields


def build_sighting_fields(at_fields: dict, garment_webflow_id: str | None) -> dict:
    title   = get_str(at_fields.get("eBay Title"))
    item_id = get_str(at_fields.get("eBay Item ID") or at_fields.get("Item Number", ""))

    if not title:
        url   = get_str(at_fields.get("Listing URL", ""))
        title = url.split("/")[-1].split("?")[0] if url else ""
    if not title:
        return {}

    # ASCII-safe slug: title stem + a stable suffix (eBay item id, or title hash).
    raw_suffix  = item_id[-10:] if item_id else title[:20]
    slug_suffix = slugify(raw_suffix, fallback="")
    base        = slugify(title[:40], fallback="")
    slug        = _compose_slug(base, slug_suffix) or "sighting"

    fields = {
        "name":          title,
        "slug":          slug,
        "listing-url":   get_str(at_fields.get("Listing URL")),
        "listing-price": at_fields.get("Listed Price") or 0,
        "status":        get_str(at_fields.get("Status")),
        # condition-2 is the new PLAIN TEXT field (old Option field "condition"
        # was deleted; Webflow reserved its slug, so the replacement is condition-2).
        # Plain text → send the raw string straight from Airtable, no option-id lookup.
        "condition-2":   get_str(at_fields.get("Condition")),
        "date-listed":   get_str(at_fields.get("Date Listed")),
        "size":          get_str(at_fields.get("Size")),
    }

    # Sold date (slug confirmed from collection details) — only sent when present.
    date_sold = get_str(at_fields.get("Date Sold"))
    if date_sold:
        fields["date-sold"] = date_sold

    if garment_webflow_id:
        fields["garment"] = garment_webflow_id

    return fields


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    sync_started = datetime.now(timezone.utc)
    print("=" * 64)
    print(f"  Filted — Airtable → Webflow Sync")
    print(f"  {sync_started.strftime('%d %b %Y, %I:%M %p')}")
    print(f"  Designer filter: {DESIGNER_FILTER}")
    print("=" * 64)

    # ── Step 1: Build Designer lookup ────────────────────────────────────────
    print("\n  Building Designer lookup...")
    designer_records = get_all_airtable_records(DESIGNERS_TABLE_ID)
    designer_lookup  = {
        r["id"]: r["fields"].get(WEBFLOW_ID_FIELD)
        for r in designer_records
        if r["fields"].get(WEBFLOW_ID_FIELD)
    }
    designer_name_to_wf = {}
    for r in designer_records:
        name  = get_str(r["fields"].get("Designer Name"))
        wf_id = r["fields"].get(WEBFLOW_ID_FIELD)
        if name and wf_id:
            designer_name_to_wf[name.lower()] = wf_id
    print(f"  {len(designer_lookup)} designer(s) with Webflow IDs.")

    # ── Step 2: Sync Campaigns ────────────────────────────────────────────────
    print("\n" + "=" * 64)
    print("  Syncing: Campaigns")
    print("=" * 64)

    all_campaigns = get_all_airtable_records(
        CAMPAIGNS_TABLE_ID,
        filter_formula="{Published}=TRUE()",
        fields=[
            "Collection Name", "Designer Name", "Designer", "Season Code",
            "Slug", "Webflow Item ID",
        ],
    )

    campaigns = [
        c for c in all_campaigns
        if any(d in get_str(c["fields"].get("Designer Name")) for d in DESIGNER_FILTER)
    ]
    print(f"  {len(campaigns)} campaign(s) for {DESIGNER_FILTER}")

    created = updated = skipped = errors = 0
    campaign_lookup = {}
    for record in all_campaigns:
        wf_id = record["fields"].get(WEBFLOW_ID_FIELD)
        if wf_id:
            campaign_lookup[record["id"]] = wf_id

    for record in campaigns:
        airtable_id = record["id"]
        at_fields   = record.get("fields", {})
        col_name    = get_str(at_fields.get("Collection Name"))
        designer    = get_str(at_fields.get("Designer Name"))

        linked_designers   = at_fields.get("Designer", [])
        linked_designer_id = linked_designers[0] if linked_designers else None
        designer_wf_id     = (
            designer_lookup.get(linked_designer_id)
            if linked_designer_id
            else designer_name_to_wf.get(designer.lower())
        )

        print(f"\n  {designer} — {col_name}")

        garments = []
        if col_name:
            try:
                garments = get_all_airtable_records(
                    GARMENTS_TABLE_ID,
                    filter_formula=f'AND({{Collection}}="{col_name}", {{Image 1}}!="")',
                    fields=["Garment Name", "Product Code", "Product Colour", "Category"],
                )
            except Exception as e:
                print(f"    ⚠️  Could not fetch garments: {e}")

        print(f"    {len(garments)} garments with images")

        wf_fields = build_campaign_fields(at_fields, airtable_id, designer_wf_id, garments, designer)
        if not wf_fields:
            skipped += 1
            continue

        existing_id = at_fields.get(WEBFLOW_ID_FIELD)
        if existing_id:
            wf_fields.pop("slug", None)  # Freeze: never change a live URL
            if webflow_update_item(CAMPAIGNS_COLLECTION_ID, existing_id, wf_fields):
                updated += 1
                print("    ✓ Updated")
            else:
                errors += 1
        else:
            new_id = webflow_create_item(CAMPAIGNS_COLLECTION_ID, wf_fields)
            if new_id:
                write_webflow_id_to_airtable(CAMPAIGNS_TABLE_ID, airtable_id, new_id)
                campaign_lookup[airtable_id] = new_id
                created += 1
                print(f"    ✓ Created (ID: {new_id})")
            else:
                errors += 1
        time.sleep(0.3)

    print(f"\n  Campaigns: {created} created | {updated} updated | {skipped} skipped | {errors} errors")

    # ── Step 3: Identify garments to keep (active OR sold) ───────────────────
    print("\n" + "=" * 64)
    print("  Loading all sightings...")
    all_sightings = get_all_airtable_records(SIGHTINGS_TABLE_ID)

    def in_filter(record):
        designer = get_str(record["fields"].get("Brand") or record["fields"].get("Designer Name"))
        return any(d in designer for d in DESIGNER_FILTER)

    active_sightings  = [r for r in all_sightings if r["fields"].get("Status") == ACTIVE_STATUS and r["fields"].get("Verified") and in_filter(r)]
    sold_sightings    = [r for r in all_sightings if r["fields"].get("Status") == SOLD_STATUS and in_filter(r)]
    cleanup_sightings = [r for r in all_sightings if r["fields"].get("Status") not in KEEP_STATUSES and r["fields"].get(WEBFLOW_ID_FIELD)]

    print(f"  {len(active_sightings)} verified active | {len(sold_sightings)} sold | {len(cleanup_sightings)} to clean up")

    def garment_ids_from(sightings):
        ids = set()
        for r in sightings:
            linked = r["fields"].get("Garment", [])
            if linked:
                ids.add(linked[0])
        return ids

    active_garment_ids = garment_ids_from(active_sightings)
    sold_garment_ids   = garment_ids_from(sold_sightings)
    # A garment is kept if anything (active OR sold) still references it.
    keep_garment_ids   = active_garment_ids | sold_garment_ids

    # ── Step 4: Sync Garments (active + sold) ────────────────────────────────
    print("\n" + "=" * 64)
    print("  Syncing: Garments")
    print("=" * 64)

    all_garments = get_all_airtable_records(GARMENTS_TABLE_ID)
    priority_garments = [
        g for g in all_garments
        if g["id"] in keep_garment_ids and g["fields"].get("Image 1")
    ]
    print(f"  {len(priority_garments)} garments with active/sold sightings + image")

    garment_lookup = {}
    for g in all_garments:
        wf_id = g["fields"].get(WEBFLOW_ID_FIELD)
        if wf_id:
            garment_lookup[g["id"]] = wf_id

    campaign_to_designer = {}
    for c in all_campaigns:
        linked_designers   = c["fields"].get("Designer", [])
        linked_designer_id = linked_designers[0] if linked_designers else None
        if linked_designer_id and designer_lookup.get(linked_designer_id):
            campaign_to_designer[c["id"]] = designer_lookup[linked_designer_id]

    created = updated = skipped = errors = 0
    for record in priority_garments:
        airtable_id        = record["id"]
        at_fields          = record.get("fields", {})
        linked_campaigns   = at_fields.get("Collection", []) or at_fields.get("Campaign", [])
        linked_campaign_id = linked_campaigns[0] if linked_campaigns else None
        campaign_wf_id     = campaign_lookup.get(linked_campaign_id) if linked_campaign_id else None
        designer_wf_id     = campaign_to_designer.get(linked_campaign_id) if linked_campaign_id else None
        wf_fields          = build_garment_fields(at_fields, airtable_id, campaign_wf_id, designer_wf_id)

        if not wf_fields:
            skipped += 1
            continue

        existing_id = at_fields.get(WEBFLOW_ID_FIELD)
        if existing_id:
            wf_fields.pop("slug", None)  # Freeze: never change a live URL
            if webflow_update_item(GARMENTS_COLLECTION_ID, existing_id, wf_fields):
                updated += 1
                garment_lookup[airtable_id] = existing_id
            else:
                errors += 1
        else:
            new_id = webflow_create_item(GARMENTS_COLLECTION_ID, wf_fields)
            if new_id:
                write_webflow_id_to_airtable(GARMENTS_TABLE_ID, airtable_id, new_id)
                garment_lookup[airtable_id] = new_id
                created += 1
            else:
                errors += 1
        time.sleep(0.3)

    print(f"  Garments: {created} created | {updated} updated | {skipped} skipped | {errors} errors")

    # ── Step 5: Delete garments with no active AND no sold sightings ─────────
    print("\n  Cleaning up garments with no active or sold sightings...")
    deleted = errors_d = 0
    for record in all_garments:
        existing_id = record["fields"].get(WEBFLOW_ID_FIELD)
        if not existing_id:
            continue
        # Keep anything still referenced by an active OR sold sighting — deleting
        # a garment that holds a sold sighting both loses price history and 409s.
        if record["id"] not in keep_garment_ids:
            if webflow_delete_item(GARMENTS_COLLECTION_ID, existing_id):
                write_webflow_id_to_airtable(GARMENTS_TABLE_ID, record["id"], "")
                deleted += 1
            else:
                errors_d += 1
            time.sleep(0.2)
    print(f"  Garments cleaned up: {deleted} deleted | {errors_d} errors")

    # ── Step 6: Sync Sightings ────────────────────────────────────────────────
    print("\n" + "=" * 64)
    print("  Syncing: Resale Sightings")
    print("=" * 64)

    print("  Cleaning up expired/removed sightings...")
    deleted = errors_d = 0
    for record in cleanup_sightings:
        existing_id = record["fields"].get(WEBFLOW_ID_FIELD)
        if webflow_delete_item(SIGHTINGS_COLLECTION_ID, existing_id):
            write_webflow_id_to_airtable(SIGHTINGS_TABLE_ID, record["id"], "")
            deleted += 1
        else:
            errors_d += 1
        time.sleep(0.2)
    print(f"  Cleaned up: {deleted} deleted | {errors_d} errors")

    sightings_to_sync = active_sightings + sold_sightings
    print(f"  Syncing {len(sightings_to_sync)} sightings (active + sold)...")
    created = updated = skipped = errors = 0
    for record in sightings_to_sync:
        airtable_id       = record["id"]
        at_fields         = record.get("fields", {})
        linked_garments   = at_fields.get("Garment", [])
        linked_garment_id = linked_garments[0] if linked_garments else None
        if not linked_garment_id:
            skipped += 1
            continue
        garment_webflow_id = garment_lookup.get(linked_garment_id)
        wf_fields          = build_sighting_fields(at_fields, garment_webflow_id)
        if not wf_fields.get("name"):
            skipped += 1
            continue
        existing_id = at_fields.get(WEBFLOW_ID_FIELD)
        if existing_id:
            wf_fields.pop("slug", None)  # Freeze: never change a live URL
            if webflow_update_item(SIGHTINGS_COLLECTION_ID, existing_id, wf_fields):
                updated += 1
            else:
                errors += 1
        else:
            new_id = webflow_create_item(SIGHTINGS_COLLECTION_ID, wf_fields)
            if new_id:
                write_webflow_id_to_airtable(SIGHTINGS_TABLE_ID, airtable_id, new_id)
                created += 1
            else:
                errors += 1
        time.sleep(0.2)
    print(f"  Sightings: {created} created | {updated} updated | {skipped} skipped | {errors} errors")

    # ── Step 7: Upload images to Supabase Storage ─────────────────────────────
    upload_images_to_supabase(all_garments)

    # ── Step 8: Save timestamp ────────────────────────────────────────────────
    save_last_sync(sync_started)

    print("\n" + "=" * 64)
    print(f"  ■ Sync complete — {datetime.now().strftime('%d %b %Y, %I:%M %p')}")
    print("=" * 64)


if __name__ == "__main__":
    main()
