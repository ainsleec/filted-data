"""
Filted — Airtable → Webflow Sync
Runs via GitHub Actions on a schedule.
All secrets are injected as environment variables.

Sync order:
  1. Build Designer lookup  (Webflow IDs manually set in Airtable)
  2. Sync Campaigns         (Airtable Collections → Webflow Campaigns)
  3. Sync Garments          (verified active sightings only)
  4. Step 7b: Delete garments no longer with active sightings
  5. Sync Sightings         (Verified Active → create/update; Expired/Removed → delete)
  6. Save last sync timestamp to GitHub
"""

import os
import re
import json
import time
import base64
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
def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")[:80]


def get_str(value, default=""):
    if value is None:
        return default
    if isinstance(value, list):
        return str(value[0]).strip() if value else default
    return str(value).strip()


# ── Airtable helpers ──────────────────────────────────────────────────────────
def get_all_airtable_records(table_id: str, filter_formula: str = None, fields: list = None) -> list:
    api    = Api(AIRTABLE_API_KEY)
    table  = api.table(AIRTABLE_BASE, table_id)
    kwargs = {}
    if filter_formula:
        kwargs["formula"] = filter_formula
    if fields:
        kwargs["fields"] = fields
    return table.all(**kwargs)


def write_webflow_id_to_airtable(table_id: str, record_id: str, webflow_id: str):
    api   = Api(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE, table_id)
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
    # No /live suffix — catches both draft and published items
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
        data = resp.json()
        content = json.loads(base64.b64decode(data["content"]).decode())
        return content.get(LAST_SYNC_KEY)
    return None


def save_last_sync(ts: datetime):
    url  = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{LAST_SYNC_PATH}"
    resp = requests.get(url, headers={"Authorization": f"token {GITHUB_TOKEN}"})
    existing = {}
    sha = None
    if resp.status_code == 200:
        data = resp.json()
        sha  = data["sha"]
        existing = json.loads(base64.b64decode(data["content"]).decode())
    existing[LAST_SYNC_KEY] = ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    payload = {
        "message": f"chore: update last sync timestamp",
        "content": base64.b64encode(json.dumps(existing).encode()).decode(),
    }
    if sha:
        payload["sha"] = sha
    requests.put(url, headers={"Authorization": f"token {GITHUB_TOKEN}", "Content-Type": "application/json"}, json=payload)


# ── Field builders ────────────────────────────────────────────────────────────
def build_campaign_fields(at_fields: dict, designer_webflow_id: str | None) -> dict:
    name = get_str(at_fields.get("Collection Name"))
    if not name:
        return {}
    fields = {
        "name":        name,
        "slug":        slugify(get_str(at_fields.get("Slug")) or name),
        "season-code": get_str(at_fields.get("Season Code")),
    }
    if designer_webflow_id:
        fields["designer"] = designer_webflow_id
    hero = at_fields.get("Hero Image", [])
    if hero:
        fields["hero-image"] = {"url": hero[0].get("url"), "alt": name}
    return fields


def build_garment_fields(at_fields: dict, campaign_webflow_id: str | None, designer_webflow_id: str | None) -> dict:
    name = get_str(at_fields.get("Garment Name"))
    if not name:
        return {}
    slug_base = get_str(at_fields.get("Webflow Slug")) or name
    fields = {
        "name":             name,
        "slug":             slugify(slug_base),
        "product-code":     get_str(at_fields.get("Product Code")),
        "product-colour":   get_str(at_fields.get("Product Colour")),
        "category":         get_str(at_fields.get("Category")),
        "rrp":              at_fields.get("RRP") or 0,
    }
    if campaign_webflow_id:
        fields["campaign"] = campaign_webflow_id
    if designer_webflow_id:
        fields["designer"] = designer_webflow_id
    img1 = at_fields.get("Image 1", [])
    if img1:
        fields["image-1"] = {"url": img1[0].get("url"), "alt": name}
    img2 = at_fields.get("Image 2", [])
    if img2:
        fields["image-2"] = {"url": img2[0].get("url"), "alt": name}
    return fields


def build_sighting_fields(at_fields: dict, garment_webflow_id: str | None) -> dict:
    title   = get_str(at_fields.get("eBay Title"))
    item_id = get_str(at_fields.get("eBay Item ID") or at_fields.get("Item Number", ""))
    if not title:
        url = get_str(at_fields.get("Listing URL", ""))
        title = url.split("/")[-1].split("?")[0] if url else ""
    if not title:
        return {}
    # Use last 10 digits of item ID for slug to avoid collisions
    slug_suffix = item_id[-10:] if item_id else re.sub(r"[^\w]", "", title[:20])
    fields = {
        "name":          title,
        "slug":          f"{slugify(title[:40])}-{slug_suffix}",
        "ebay-title":    title,
        "listing-url":   get_str(at_fields.get("Listing URL")),
        "listed-price":  at_fields.get("Listed Price") or 0,
        "status":        get_str(at_fields.get("Status")),
        "ebay-item-id":  item_id,
        "date-listed":   get_str(at_fields.get("Date Listed")),
        "seller-name":   get_str(at_fields.get("Seller Name")),
        "condition":     get_str(at_fields.get("Condition")),
    }
    if garment_webflow_id:
        fields["garment"] = garment_webflow_id
    img = at_fields.get("eBay Image", [])
    if img:
        fields["ebay-image"] = {"url": img[0].get("url") if isinstance(img[0], dict) else img[0], "alt": title}
    sold_price = at_fields.get("Sold Price")
    if sold_price:
        fields["sold-price"] = sold_price
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
    print(f"  {len(designer_lookup)} designer(s) with Webflow IDs.")

    # ── Step 2: Sync Campaigns ────────────────────────────────────────────────
    print("\n" + "=" * 64)
    print(f"  Syncing: Campaigns")
    print("=" * 64)
    all_campaigns = get_all_airtable_records(CAMPAIGNS_TABLE_ID)
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
        linked_designers   = at_fields.get("Designer", [])
        linked_designer_id = linked_designers[0] if linked_designers else None
        designer_wf_id     = designer_lookup.get(linked_designer_id) if linked_designer_id else None
        wf_fields          = build_campaign_fields(at_fields, designer_wf_id)
        if not wf_fields:
            skipped += 1
            continue
        existing_id = at_fields.get(WEBFLOW_ID_FIELD)
        if existing_id:
            if webflow_update_item(CAMPAIGNS_COLLECTION_ID, existing_id, wf_fields):
                updated += 1
            else:
                errors += 1
        else:
            new_id = webflow_create_item(CAMPAIGNS_COLLECTION_ID, wf_fields)
            if new_id:
                write_webflow_id_to_airtable(CAMPAIGNS_TABLE_ID, airtable_id, new_id)
                campaign_lookup[airtable_id] = new_id
                created += 1
            else:
                errors += 1
        time.sleep(0.3)
    print(f"  Campaigns: {created} created | {updated} updated | {skipped} skipped | {errors} errors\n")

    # ── Step 3: Identify active garments ─────────────────────────────────────
    print("  Loading all sightings...")
    all_sightings = get_all_airtable_records(SIGHTINGS_TABLE_ID)

    # Filter to target designers
    def in_filter(record):
        designer = get_str(record["fields"].get("Brand") or record["fields"].get("Designer Name"))
        return any(d in designer for d in DESIGNER_FILTER)

    active_sightings  = [r for r in all_sightings if r["fields"].get("Status") == ACTIVE_STATUS and r["fields"].get("Verified") and in_filter(r)]
    sold_sightings    = [r for r in all_sightings if r["fields"].get("Status") == SOLD_STATUS and in_filter(r)]
    cleanup_sightings = [r for r in all_sightings if r["fields"].get("Status") not in KEEP_STATUSES and r["fields"].get(WEBFLOW_ID_FIELD)]

    print(f"  {len(active_sightings)} verified active | {len(sold_sightings)} sold | {len(cleanup_sightings)} to clean up")

    # Garment IDs that have verified active sightings
    active_garment_ids = set()
    for r in active_sightings:
        linked = r["fields"].get("Garment", [])
        if linked:
            active_garment_ids.add(linked[0])

    # ── Step 4: Sync Garments ─────────────────────────────────────────────────
    print("\n" + "=" * 64)
    print(f"  Syncing: Garments")
    print("=" * 64)
    all_garments = get_all_airtable_records(GARMENTS_TABLE_ID)
    priority_garments = [
        g for g in all_garments
        if g["id"] in active_garment_ids and g["fields"].get("Image 1")
    ]
    print(f"  {len(priority_garments)} garments with verified active sightings + image")

    garment_lookup = {}
    for g in all_garments:
        wf_id = g["fields"].get(WEBFLOW_ID_FIELD)
        if wf_id:
            garment_lookup[g["id"]] = wf_id

    # Campaign → Designer lookup for garments
    campaign_to_designer = {}
    for c in all_campaigns:
        linked_designers   = c["fields"].get("Designer", [])
        linked_designer_id = linked_designers[0] if linked_designers else None
        if linked_designer_id and designer_lookup.get(linked_designer_id):
            campaign_to_designer[c["id"]] = designer_lookup[linked_designer_id]

    created = updated = skipped = errors = 0
    for record in priority_garments:
        airtable_id = record["id"]
        at_fields   = record.get("fields", {})
        linked_campaigns   = at_fields.get("Collection", []) or at_fields.get("Campaign", [])
        linked_campaign_id = linked_campaigns[0] if linked_campaigns else None
        campaign_wf_id     = campaign_lookup.get(linked_campaign_id) if linked_campaign_id else None
        designer_wf_id     = campaign_to_designer.get(linked_campaign_id) if linked_campaign_id else None
        wf_fields          = build_garment_fields(at_fields, campaign_wf_id, designer_wf_id)
        if not wf_fields:
            skipped += 1
            continue
        existing_id = at_fields.get(WEBFLOW_ID_FIELD)
        if existing_id:
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
    print(f"  Garments: {created} created | {updated} updated | {skipped} skipped | {errors} errors\n")

    # ── Step 7b: Delete garments no longer with active sightings ─────────────
    print("  Cleaning up garments with no active sightings...")
    deleted = errors_d = 0
    for record in all_garments:
        existing_id = record["fields"].get(WEBFLOW_ID_FIELD)
        if not existing_id:
            continue
        if record["id"] not in active_garment_ids:
            if webflow_delete_item(GARMENTS_COLLECTION_ID, existing_id):
                write_webflow_id_to_airtable(GARMENTS_TABLE_ID, record["id"], "")
                deleted += 1
            else:
                errors_d += 1
            time.sleep(0.2)
    print(f"  Garments cleaned up: {deleted} deleted | {errors_d} errors\n")

    # ── Step 8: Sync Sightings ────────────────────────────────────────────────
    print("=" * 64)
    print(f"  Syncing: Resale Sightings")
    print("=" * 64)

    # First clean up non-keep statuses
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

    # Sync active + sold
    sightings_to_sync = active_sightings + sold_sightings
    print(f"  Syncing {len(sightings_to_sync)} sightings (active + sold)...")
    created = updated = skipped = errors = 0
    for record in sightings_to_sync:
        airtable_id = record["id"]
        at_fields   = record.get("fields", {})
        linked_garments    = at_fields.get("Garment", [])
        linked_garment_id  = linked_garments[0] if linked_garments else None
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
    print(f"  Sightings: {created} created | {updated} updated | {skipped} skipped | {errors} errors\n")

    # ── Save timestamp ────────────────────────────────────────────────────────
    save_last_sync(sync_started)

    print("=" * 64)
    print(f"  ■ Sync complete — {datetime.now().strftime('%d %b %Y, %I:%M %p')}")
    print("=" * 64)


if __name__ == "__main__":
    main()
