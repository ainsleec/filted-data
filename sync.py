"""
Filted — Airtable → Webflow Sync
Runs via GitHub Actions on a schedule.
All secrets are injected as environment variables.
"""

import os
import re
import json
import time
import socket
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
DESIGNER_FILTER         = ("Alemais", "Aje", "Sir.")

ACTIVE_STATUS           = "Active"
SOLD_STATUS             = "Sold"
EXPIRED_STATUS          = "Expired"

# ── Webflow helpers ───────────────────────────────────────────────────────────
socket.setdefaulttimeout(30)
_session = requests.Session()

WEBFLOW_HEADERS = {
    "Authorization": f"Bearer {WEBFLOW_API_TOKEN}",
    "Content-Type":  "application/json",
    "accept":        "application/json",
}
WEBFLOW_TIMEOUT     = (10, 30)
WEBFLOW_RETRIES     = 3
WEBFLOW_RETRY_DELAY = 5


def _webflow_request(method, url, **kwargs):
    for attempt in range(1, WEBFLOW_RETRIES + 1):
        try:
            resp = _session.request(method, url, headers=WEBFLOW_HEADERS,
                                    timeout=WEBFLOW_TIMEOUT, **kwargs)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                print(f"    ⏳ Rate limited — waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                print(f"    ⚠️  Server error {resp.status_code} (attempt {attempt}/{WEBFLOW_RETRIES})")
                time.sleep(WEBFLOW_RETRY_DELAY)
                continue
            return resp
        except (requests.exceptions.Timeout, socket.timeout):
            print(f"    ⚠️  Timeout (attempt {attempt}/{WEBFLOW_RETRIES})")
            _session.close()
            if attempt < WEBFLOW_RETRIES:
                time.sleep(WEBFLOW_RETRY_DELAY)
        except requests.exceptions.ConnectionError as e:
            print(f"    ⚠️  Connection error (attempt {attempt}/{WEBFLOW_RETRIES}): {e}")
            _session.close()
            if attempt < WEBFLOW_RETRIES:
                time.sleep(WEBFLOW_RETRY_DELAY)
    return None


def webflow_create_item(collection_id, fields):
    url  = f"https://api.webflow.com/v2/collections/{collection_id}/items/live"
    resp = _webflow_request("POST", url, json={"fieldData": fields})
    if resp is None:
        print(f"    ■■ Create failed: no response after {WEBFLOW_RETRIES} attempts")
        return None
    if resp.status_code in (200, 201, 202):
        return resp.json().get("id")
    print(f"    ■■ Create failed [{resp.status_code}]: {resp.text[:300]}")
    return None


def webflow_update_item(collection_id, item_id, fields):
    url  = f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}/live"
    resp = _webflow_request("PATCH", url, json={"fieldData": fields})
    if resp is None:
        print(f"    ■■ Update failed: no response after {WEBFLOW_RETRIES} attempts")
        return False
    if resp.status_code in (200, 202):
        return True
    if resp.status_code == 404:
        print(f"    ⚠️  Item not found in Webflow ({item_id}) — will recreate on next run")
        return False
    print(f"    ■■ Update failed [{resp.status_code}]: {resp.text[:300]}")
    return False


def webflow_delete_item(collection_id, item_id):
    # No /live suffix — deletes both draft and published items
    url  = f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}"
    resp = _webflow_request("DELETE", url)
    if resp is None:
        print(f"  ■■ Delete failed: no response after {WEBFLOW_RETRIES} attempts")
        return False
    if resp.status_code in (200, 202, 204):
        return True
    print(f"  ■■ Delete failed [{resp.status_code}]: {resp.text[:200]}")
    return False


# ── Airtable helpers ──────────────────────────────────────────────────────────
def get_all_airtable_records(table_id):
    api   = Api(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE_ID, table_id)
    return table.all()


def write_webflow_id_to_airtable(table_id, record_id, webflow_item_id):
    api   = Api(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE_ID, table_id)
    table.update(record_id, {WEBFLOW_ID_FIELD: webflow_item_id})


# ── Field extraction helpers ──────────────────────────────────────────────────
def extract_image_url(attachment_field):
    if isinstance(attachment_field, list) and len(attachment_field) > 0:
        url = attachment_field[0].get("url")
        if url:
            return {"url": url, "alt": ""}
    return None


def format_date(date_str):
    if not date_str:
        return None
    if "T" not in date_str:
        return f"{date_str}T00:00:00.000Z"
    return date_str


def clean_name(value):
    return str(value).strip("[]'\"") if value else ""


# ── Field builders ────────────────────────────────────────────────────────────
def build_campaign_fields(at_fields, designer_webflow_id):
    fields = {}
    name = at_fields.get("Collection Name")
    if name:
        fields["name"] = clean_name(name)
    season_code = at_fields.get("Season Code")
    if season_code:
        fields["season-code"] = str(season_code)
    release_date = format_date(at_fields.get("Release Date"))
    if release_date:
        fields["released-2"] = release_date
    if designer_webflow_id:
        fields["designer"] = designer_webflow_id
    return fields


def build_garment_fields(at_fields, campaign_webflow_id, airtable_record_id,
                         active_garment_ids, designer_webflow_id=None):
    fields = {}
    for at_key, wf_slug in {
        "Name":                "name",
        "Product Code":        "style-id",
        "Fabric":              "fabric",
        "Product Colour":      "product-colour",
        "Designer Finsweet":   "designer-finsweet",
        "Last Sold Date":      "last-sold-date",
        "First Sighting Date": "first-sighting-date",
    }.items():
        val = at_fields.get(at_key)
        if val is not None and val != "":
            fields[wf_slug] = str(val)

    rrp_val = at_fields.get("RRP")
    if rrp_val is not None:
        try:
            rrp_num = float(rrp_val)
            if rrp_num > 0:
                fields["rrp-display-webflow"] = f"${int(rrp_num)}"
        except (ValueError, TypeError):
            pass

    fields["has-active-listings-text"] = "1" if airtable_record_id in active_garment_ids else ""

    for at_key, wf_slug in {
        "RRP":                    "rrp",
        "Resale From":            "resale-from",
        "Discount Amount":        "discount-amount-2",
        "Average Resale Price":   "average-resale-price",
        "Lowest Resale Price":    "lowest-resale-price",
        "Highest Resale Price":   "highest-resale-price",
        "Total Times Listed":     "total-times-listed",
        "Sold Count":             "sold-count",
    }.items():
        val = at_fields.get(at_key)
        if val is not None:
            try:
                fields[wf_slug] = float(val)
            except (ValueError, TypeError):
                pass

    description = at_fields.get("Description")
    if description:
        fields["description"] = description

    origin_url = at_fields.get("Origin URL")
    if origin_url:
        fields["origin-url"] = origin_url

    if campaign_webflow_id:
        fields["campaign-2"] = campaign_webflow_id
    if designer_webflow_id:
        fields["designers"] = designer_webflow_id

    last_listing_date = format_date(at_fields.get("Last Listing Date"))
    if last_listing_date:
        fields["last-listing-date"] = last_listing_date

    for at_key, wf_slug in {
        "Image 1": "main-photo",
        "Image 2": "image-2",
        "Image 3": "model-back",
        "Image 4": "image-3",
        "Image 5": "image-4",
        "Image 6": "image-5",
        "Image 7": "image-7",
    }.items():
        img = extract_image_url(at_fields.get(at_key))
        if img:
            fields[wf_slug] = img

    return fields


def build_sighting_fields(at_fields, garment_webflow_id):
    fields = {}
    name = at_fields.get("Name") or at_fields.get("eBay Title", "")
    if name:
        fields["name"] = clean_name(name)

    size = at_fields.get("Size")
    if size:
        fields["size"] = str(size)

    price = at_fields.get("Listed Price")
    if price is not None:
        try:
            fields["listing-price"] = float(price)
        except (ValueError, TypeError):
            pass

    listing_url = at_fields.get("Listing URL")
    if listing_url:
        fields["listing-url"] = listing_url
        item_id = listing_url.split("?")[0].rstrip("/").split("/")[-1]
        if item_id and name:
            clean = re.sub(r'[^a-z0-9]+', '-', clean_name(name).lower()).strip('-')
            fields["slug"] = f"{clean}-{item_id[-10:]}"

    for at_key, wf_slug in {
        "Platform":  "platform",
        "Status":    "status",
        "Condition": "condition",
    }.items():
        val = at_fields.get(at_key)
        if val:
            fields[wf_slug] = val

    date_listed = format_date(at_fields.get("Date Listed"))
    if date_listed:
        fields["date-listed"] = date_listed

    date_sold = format_date(at_fields.get("Date Sold"))
    if date_sold:
        fields["date-sold"] = date_sold

    if garment_webflow_id:
        fields["garment"] = garment_webflow_id

    return fields


# ── Lookup builders ───────────────────────────────────────────────────────────
def build_campaign_to_designer_lookup(designer_lookup):
    records = get_all_airtable_records(CAMPAIGNS_TABLE_ID)
    lookup  = {}
    for r in records:
        linked_designers   = r["fields"].get("Designer", [])
        linked_designer_id = linked_designers[0] if linked_designers else None
        if linked_designer_id:
            designer_wf_id = designer_lookup.get(linked_designer_id)
            if designer_wf_id:
                lookup[r["id"]] = designer_wf_id
    return lookup


# ── Incremental sync timestamp ────────────────────────────────────────────────
def load_last_sync():
    try:
        url = f"https://raw.githubusercontent.com/{GITHUB_USERNAME}/{GITHUB_REPO}/main/last_sync.json"
        r   = requests.get(url)
        if r.status_code == 200:
            return r.json().get("last_sync", "")
    except Exception:
        pass
    return ""


def save_last_sync(ts):
    try:
        content = json.dumps({"last_sync": ts})
        url     = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/last_sync.json"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        r       = requests.get(url, headers=headers)
        sha     = r.json().get("sha") if r.status_code == 200 else None
        payload = {
            "message": "Update sync timestamp",
            "content": base64.b64encode(content.encode()).decode(),
        }
        if sha:
            payload["sha"] = sha
        requests.put(url, headers=headers, json=payload)
        print("  ✓ Sync timestamp saved to GitHub")
    except Exception as e:
        print(f"  ⚠️  Could not save sync timestamp: {e}")


def is_modified_since(record, since_iso):
    if not since_iso:
        return True
    modified = record.get("fields", {}).get("Last Modified", "") or record.get("modifiedTime", "")
    if not modified:
        return True
    return modified > since_iso


# ── Main sync ─────────────────────────────────────────────────────────────────
def main():
    last_sync    = load_last_sync()
    sync_started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    is_full_sync = not last_sync

    print(f"\n{'=' * 64}")
    print(f"  Filted × Priority Sync")
    print(f"  {datetime.now().strftime('%d %b %Y, %I:%M %p')}")
    print(f"  Designer filter : {DESIGNER_FILTER}")
    print(f"  Mode            : {'FULL SYNC' if is_full_sync else f'INCREMENTAL (since {last_sync})'}")
    print(f"{'=' * 64}\n")

    # ── Step 1: Fetch all tables ──────────────────────────────────────────────
    print("  Fetching all Airtable tables...")
    designer_records = get_all_airtable_records(DESIGNERS_TABLE_ID)
    all_sightings    = get_all_airtable_records(SIGHTINGS_TABLE_ID)
    all_garments     = get_all_airtable_records(GARMENTS_TABLE_ID)
    all_campaigns    = get_all_airtable_records(CAMPAIGNS_TABLE_ID)
    print(f"  Loaded: {len(designer_records)} designers | {len(all_sightings)} sightings | "
          f"{len(all_garments)} garments | {len(all_campaigns)} campaigns\n")

    # ── Step 2: Designer lookups ──────────────────────────────────────────────
    designer_lookup      = {}
    designer_name_lookup = {}
    for r in designer_records:
        f    = r.get("fields", {})
        wfid = f.get(WEBFLOW_ID_FIELD)
        name = f.get("Name") or ""
        if wfid:
            designer_lookup[r["id"]] = wfid
        designer_name_lookup[r["id"]] = name
    print(f"  {len(designer_lookup)} designer(s) with Webflow IDs.\n")
    campaign_to_designer_lookup = build_campaign_to_designer_lookup(designer_lookup)

    # ── Step 3: Filter sightings ──────────────────────────────────────────────
    def get_sighting_designer(fields):
        val = fields.get("Designer (from Garment) 2", [])
        return val[0] if isinstance(val, list) and val else str(val)

    def has_image(record):
        img = record["fields"].get("Image 1")
        return isinstance(img, list) and len(img) > 0

    if DESIGNER_FILTER:
        df_lower      = [d.lower() for d in DESIGNER_FILTER]
        all_sightings = [
            r for r in all_sightings
            if any(d in get_sighting_designer(r["fields"]).lower() for d in df_lower)
        ]
        print(f"  Sightings filtered to '{DESIGNER_FILTER}': {len(all_sightings)}")

    active_sightings  = []
    sold_sightings    = []
    expired_sightings = []
    for r in all_sightings:
        status   = r["fields"].get("Status")
        verified = r["fields"].get("Verified", False)
        if status == ACTIVE_STATUS and r["fields"].get("Garment") and verified:
            active_sightings.append(r)
        elif status == SOLD_STATUS and r["fields"].get("Garment"):
            sold_sightings.append(r)
        elif status == EXPIRED_STATUS and r["fields"].get("Garment"):
            expired_sightings.append(r)

    print(f"  {len(active_sightings)} Active (verified) | {len(sold_sightings)} Sold | "
          f"{len(expired_sightings)} Expired sightings\n")

    # ── Step 4: Priority garments — only those with verified active sightings ─
    sighting_garment_ids = set()
    for r in active_sightings:
        linked = r["fields"].get("Garment", [])
        if linked:
            sighting_garment_ids.add(linked[0])

    active_garment_ids = set(sighting_garment_ids)

    recently_modified_sighting_garment_ids = set()
    for r in active_sightings + sold_sightings + expired_sightings:
        if is_modified_since(r, last_sync):
            linked = r["fields"].get("Garment", [])
            if linked:
                recently_modified_sighting_garment_ids.add(linked[0])

    priority_garments = [
        r for r in all_garments
        if has_image(r) and r["id"] in sighting_garment_ids
    ]
    priority_garment_ids = {r["id"] for r in priority_garments}
    print(f"  {len(priority_garments)} priority garments (with image + verified sighting)\n")

    if is_full_sync:
        garments_to_sync = priority_garments
    else:
        garments_to_sync = [
            r for r in priority_garments
            if is_modified_since(r, last_sync) or r["id"] in recently_modified_sighting_garment_ids
        ]
    print(f"  {'Full sync' if is_full_sync else 'Incremental'} — "
          f"{len(garments_to_sync)} garments to sync\n")

    # ── Step 5: Trim sightings to priority garments ───────────────────────────
    active_sightings = [r for r in active_sightings
                        if r["fields"].get("Garment", [None])[0] in priority_garment_ids]
    sold_sightings   = [r for r in sold_sightings
                        if r["fields"].get("Garment", [None])[0] in priority_garment_ids]
    sightings_to_sync = active_sightings if is_full_sync else [
        r for r in active_sightings if is_modified_since(r, last_sync)
    ]

    # ── Step 6: Priority campaigns ────────────────────────────────────────────
    priority_campaign_ids = set()
    for r in priority_garments:
        linked = r["fields"].get("Collection", [])
        if linked:
            priority_campaign_ids.add(linked[0])
    priority_campaigns = [r for r in all_campaigns if r["id"] in priority_campaign_ids]

    print(f"{'=' * 64}")
    print(f"  Campaigns : {len(priority_campaigns)}")
    print(f"  Garments  : {len(garments_to_sync)}")
    print(f"  Sightings : {len(sightings_to_sync)}")
    print(f"{'=' * 64}\n")

    # ── Step 7: Clean up sold/expired sightings ───────────────────────────────
    print("  Cleaning up sold/expired sightings from Webflow...")
    deleted = skipped = errors = 0
    sightings_table = Api(AIRTABLE_API_KEY).table(AIRTABLE_BASE_ID, SIGHTINGS_TABLE_ID)
    for record in sold_sightings + expired_sightings:
        airtable_id         = record["id"]
        existing_webflow_id = record["fields"].get(WEBFLOW_ID_FIELD)
        if existing_webflow_id:
            success = webflow_delete_item(SIGHTINGS_COLLECTION_ID, existing_webflow_id)
            if success:
                try:
                    sightings_table.update(airtable_id, {WEBFLOW_ID_FIELD: ""})
                    deleted += 1
                except Exception as e:
                    print(f"  ■■ Failed to clear Webflow ID ({airtable_id}): {e}")
                    errors += 1
            else:
                errors += 1
        else:
            skipped += 1
        time.sleep(0.15)
    print(f"  Sightings: {deleted} deleted | {skipped} already clean | {errors} errors\n")

    # ── Step 7b: Clean up garments with no active sightings ───────────────────
    print("  Cleaning up garments with no active sightings from Webflow...")
    deleted = skipped = errors = 0
    garments_table = Api(AIRTABLE_API_KEY).table(AIRTABLE_BASE_ID, GARMENTS_TABLE_ID)
    for record in all_garments:
        airtable_id         = record["id"]
        existing_webflow_id = record["fields"].get(WEBFLOW_ID_FIELD)
        if not existing_webflow_id:
            skipped += 1
            continue
        if airtable_id not in active_garment_ids:
            success = webflow_delete_item(GARMENTS_COLLECTION_ID, existing_webflow_id)
            if success:
                try:
                    garments_table.update(airtable_id, {WEBFLOW_ID_FIELD: ""})
                    deleted += 1
                except Exception as e:
                    print(f"  ■■ Failed to clear Webflow ID ({airtable_id}): {e}")
                    errors += 1
            else:
                errors += 1
        else:
            skipped += 1
        time.sleep(0.15)
    print(f"  Garments: {deleted} deleted | {skipped} skipped | {errors} errors\n")

    # ── Step 8: Sync campaigns ────────────────────────────────────────────────
    print("  Syncing campaigns...")
    created = updated = skipped = errors = 0
    for record in priority_campaigns:
        airtable_id = record["id"]
        at_fields   = record.get("fields", {})
        if not at_fields.get("Collection Name"):
            skipped += 1
            continue
        linked_designers    = at_fields.get("Designer", [])
        linked_designer_id  = linked_designers[0] if linked_designers else None
        designer_webflow_id = designer_lookup.get(linked_designer_id) if linked_designer_id else None
        webflow_fields      = build_campaign_fields(at_fields, designer_webflow_id)
        existing_webflow_id = at_fields.get(WEBFLOW_ID_FIELD)
        if existing_webflow_id:
            success = webflow_update_item(CAMPAIGNS_COLLECTION_ID, existing_webflow_id, webflow_fields)
            if success: updated += 1
            else: errors += 1
        else:
            new_id = webflow_create_item(CAMPAIGNS_COLLECTION_ID, webflow_fields)
            if new_id:
                write_webflow_id_to_airtable(CAMPAIGNS_TABLE_ID, airtable_id, new_id)
                created += 1
            else:
                errors += 1
        time.sleep(0.2)
    print(f"  Campaigns: {created} created | {updated} updated | {skipped} skipped | {errors} errors\n")

    campaign_lookup = {
        r["id"]: r["fields"].get(WEBFLOW_ID_FIELD)
        for r in all_campaigns
        if r["fields"].get(WEBFLOW_ID_FIELD)
    }

    # ── Step 9: Sync garments ─────────────────────────────────────────────────
    print("  Syncing garments...")
    created = updated = skipped = errors = 0
    for record in garments_to_sync:
        airtable_id = record["id"]
        at_fields   = record.get("fields", {})
        if not at_fields.get("Name"):
            skipped += 1
            continue
        linked_campaigns    = at_fields.get("Collection", [])
        linked_campaign_id  = linked_campaigns[0] if linked_campaigns else None
        campaign_webflow_id = campaign_lookup.get(linked_campaign_id) if linked_campaign_id else None
        designer_webflow_id = campaign_to_designer_lookup.get(linked_campaign_id) if linked_campaign_id else None
        webflow_fields      = build_garment_fields(at_fields, campaign_webflow_id, airtable_id,
                                                   active_garment_ids, designer_webflow_id)
        existing_webflow_id = at_fields.get(WEBFLOW_ID_FIELD)
        if existing_webflow_id:
            success = webflow_update_item(GARMENTS_COLLECTION_ID, existing_webflow_id, webflow_fields)
            if success: updated += 1
            else: errors += 1
        else:
            new_id = webflow_create_item(GARMENTS_COLLECTION_ID, webflow_fields)
            if new_id:
                write_webflow_id_to_airtable(GARMENTS_TABLE_ID, airtable_id, new_id)
                created += 1
            else:
                errors += 1
        time.sleep(0.2)
    print(f"  Garments: {created} created | {updated} updated | {skipped} skipped | {errors} errors\n")

    # ── Step 10: Rebuild garment lookup then sync sightings ───────────────────
    print("  Rebuilding garment lookup...")
    fresh_garments = get_all_airtable_records(GARMENTS_TABLE_ID)
    garment_lookup = {
        r["id"]: r["fields"].get(WEBFLOW_ID_FIELD)
        for r in fresh_garments
        if r["fields"].get(WEBFLOW_ID_FIELD)
    }
    print(f"  {len(garment_lookup)} garments in lookup\n")

    print("  Syncing active sightings...")
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
        webflow_fields     = build_sighting_fields(at_fields, garment_webflow_id)
        if not webflow_fields.get("name"):
            skipped += 1
            continue
        existing_webflow_id = at_fields.get(WEBFLOW_ID_FIELD)
        if existing_webflow_id:
            success = webflow_update_item(SIGHTINGS_COLLECTION_ID, existing_webflow_id, webflow_fields)
            if success: updated += 1
            else: errors += 1
        else:
            new_id = webflow_create_item(SIGHTINGS_COLLECTION_ID, webflow_fields)
            if new_id:
                write_webflow_id_to_airtable(SIGHTINGS_TABLE_ID, airtable_id, new_id)
                created += 1
            else:
                errors += 1
        time.sleep(0.2)
    print(f"  Sightings: {created} created | {updated} updated | {skipped} skipped | {errors} errors\n")

    # ── Save timestamp ────────────────────────────────────────────────────────
    save_last_sync(sync_started)

    print(f"{'=' * 64}")
    print(f"  ■ Sync complete — {datetime.now().strftime('%d %b %Y, %I:%M %p')}")
    print(f"{'=' * 64}")


if __name__ == "__main__":
    main()
