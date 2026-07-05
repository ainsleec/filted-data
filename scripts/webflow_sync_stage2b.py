#!/usr/bin/env python3
"""
Filted — Webflow Sync (consolidated) — STAGE 2b: Sightings + Publish

Runs AFTER Stage 2a (Campaigns + Garments) each cycle.

Sighting lifecycle (per decision: sightings need individual pages for SEO,
unlike garments they are NOT drafted — they are deleted outright the moment
they're no longer Active/Sold):
  - Active or Sold sighting, no Webflow item yet  -> CREATE
  - Active or Sold sighting, already has a Webflow item -> UPDATE
  - Any other status (Expired/Removed/etc.) with a Webflow item -> DELETE,
    then clear the Webflow Item ID field back in Airtable.

Publish step:
  Stage 2a only staged changes (isDraft flips, field updates) — nothing
  was pushed live. This stage publishes:
    - All in-scope Campaigns
    - All Garments that currently qualify as live (active/sold sighting)
    - All Sightings just created/updated this run
  This is what actually makes drafted/live changes visible on the site,
  and is what clears stale expired-listing displays.

Flags:
  --dry-run   Preview counts only. No writes, no publish calls.

Required env vars:
  AIRTABLE_API_KEY
  WEBFLOW_API_TOKEN
"""

import os, sys, re, time, unicodedata, requests
from datetime import datetime, timezone

DRY_RUN = "--dry-run" in sys.argv

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_API_KEY   = os.environ["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID   = "appUk1ThnHvWwFDHG"
DESIGNERS_TABLE_ID = "tbltYGtfVV574EOcw"
CAMPAIGNS_TABLE_ID = "tblyVgpD1SSCrRSgQ"
GARMENTS_TABLE_ID  = "tblmqjU4WqgCzP7cR"
SIGHTINGS_TABLE_ID = "tblw4aByS8d5djekv"

WEBFLOW_API_TOKEN       = os.environ["WEBFLOW_API_TOKEN"]
CAMPAIGNS_COLLECTION_ID = "68944133543f0cbb26b4aeb9"
GARMENTS_COLLECTION_ID  = "68774f3e850c7a30ebc3a0aa"
SIGHTINGS_COLLECTION_ID = "69aba8c1e497e6345c288657"

WEBFLOW_ID_FIELD = "Webflow Item ID"

HEADERS_AT = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type":  "application/json",
}
WEBFLOW_HEADERS = {
    "Authorization": f"Bearer {WEBFLOW_API_TOKEN}",
    "Content-Type":  "application/json",
    "accept":        "application/json",
}

ACTIVE_STATUS = "Active"
SOLD_STATUS   = "Sold"
KEEP_STATUSES = {"Active", "Sold"}


# ── Airtable helpers ────────────────────────────────────────────────────────────
def get_all_airtable_records(table_id, filter_formula=None, fields=None):
    records = []
    params = {"pageSize": 100}
    if filter_formula:
        params["filterByFormula"] = filter_formula
    if fields:
        params["fields[]"] = fields
    while True:
        resp = requests.get(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_id}",
            headers=HEADERS_AT, params=params
        )
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
        if not data.get("offset"):
            break
        params["offset"] = data["offset"]
    return records


def write_webflow_id_to_airtable(table_id, record_id, webflow_id):
    if DRY_RUN:
        return
    resp = requests.patch(
        f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_id}/{record_id}",
        headers=HEADERS_AT,
        json={"fields": {WEBFLOW_ID_FIELD: webflow_id}},
    )
    if not resp.ok:
        print(f"    ⚠️  Failed to write Webflow ID back to Airtable {record_id}: {resp.status_code}")


def get_str(value, default=""):
    if value is None:
        return default
    if isinstance(value, list):
        return str(value[0]).strip() if value else default
    return str(value).strip()


# ── Slug helpers (same as Stage 2a) ────────────────────────────────────────────
def slugify(text, fallback="item"):
    if not text:
        return fallback
    text = unicodedata.normalize("NFKD", str(text))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    text = text.strip("-")[:80].strip("-")
    return text or fallback


def _compose_slug(*parts):
    slug = "-".join(p for p in parts if p)
    slug = re.sub(r"-+", "-", slug).strip("-")[:80].strip("-")
    return slug


# ── Webflow helpers (staged, not auto-live) ────────────────────────────────────
def webflow_create_item(collection_id, fields, is_draft=False):
    if DRY_RUN:
        return "DRY_RUN_FAKE_ID"
    url  = f"https://api.webflow.com/v2/collections/{collection_id}/items"
    resp = requests.post(url, headers=WEBFLOW_HEADERS, json={"isDraft": is_draft, "fieldData": fields})
    if resp.status_code in (200, 201, 202):
        return resp.json().get("id")
    print(f"    ⚠️  Create failed [{resp.status_code}]: {resp.text[:300]}")
    return None


def webflow_update_item(collection_id, item_id, fields, is_draft=False):
    if DRY_RUN:
        return True
    url  = f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}"
    resp = requests.patch(url, headers=WEBFLOW_HEADERS, json={"isDraft": is_draft, "fieldData": fields})
    if resp.status_code in (200, 202):
        return True
    print(f"    ⚠️  Update failed [{resp.status_code}]: {resp.text[:200]}")
    return False


def webflow_delete_item(collection_id, item_id):
    if DRY_RUN:
        return True
    url  = f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}"
    resp = requests.delete(url, headers=WEBFLOW_HEADERS)
    if resp.status_code in (200, 202, 204):
        return True
    print(f"    ⚠️  Delete failed [{resp.status_code}]: {resp.text[:200]}")
    return False


def webflow_publish_items(collection_id, item_ids, label):
    if not item_ids:
        return
    if DRY_RUN:
        print(f"   🧪 Would publish {len(item_ids)} {label} item(s)")
        return
    for i in range(0, len(item_ids), 100):
        batch = item_ids[i:i + 100]
        url  = f"https://api.webflow.com/v2/collections/{collection_id}/items/publish"
        resp = requests.post(url, headers=WEBFLOW_HEADERS, json={"itemIds": batch})
        if resp.ok:
            print(f"   ✓ Published {len(batch)} {label} item(s)")
        else:
            print(f"    ⚠️  Publish batch failed [{resp.status_code}]: {resp.text[:200]}")
        time.sleep(0.3)


# ── Sighting field builder ──────────────────────────────────────────────────
def build_sighting_fields(at_fields, garment_webflow_id):
    title   = get_str(at_fields.get("eBay Title"))
    item_id = get_str(at_fields.get("eBay Item ID"))

    if not title:
        url   = get_str(at_fields.get("Listing URL", ""))
        title = url.split("/")[-1].split("?")[0] if url else ""
    if not title:
        return {}

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
        "condition-2":   get_str(at_fields.get("Condition")),
        "date-listed":   get_str(at_fields.get("Date Listed")),
        "size":          get_str(at_fields.get("Size")),
    }
    date_sold = get_str(at_fields.get("Date Sold"))
    if date_sold:
        fields["date-sold"] = date_sold
    if garment_webflow_id:
        fields["garment"] = garment_webflow_id

    return fields


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 64)
    print("  Filted — Webflow Sync — STAGE 2b: Sightings + Publish")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    if DRY_RUN:
        print("  🧪 DRY RUN — no writes or publishes will be made")
    print("=" * 64)

    # ── Designer scope ─────────────────────────────────────────────────────────
    print("\n📋 Loading in-feed designers...")
    designer_records = get_all_airtable_records(
        DESIGNERS_TABLE_ID, fields=["Designer Name", "Webflow Item ID", "In Feed"]
    )
    in_feed_designers = [
        r for r in designer_records
        if r["fields"].get("In Feed") and r["fields"].get(WEBFLOW_ID_FIELD)
    ]
    designer_lookup = {r["id"]: r["fields"][WEBFLOW_ID_FIELD] for r in in_feed_designers}
    in_feed_designer_ids = set(designer_lookup.keys())
    print(f"   {len(in_feed_designers)} in-feed designer(s)")

    # ── Campaigns in scope (for publish list) ─────────────────────────────────
    print("\n📦 Loading campaigns...")
    all_campaigns = get_all_airtable_records(
        CAMPAIGNS_TABLE_ID, fields=["Designer", WEBFLOW_ID_FIELD]
    )
    campaign_to_designer_id = {
        c["id"]: c["fields"]["Designer"][0]
        for c in all_campaigns if c["fields"].get("Designer")
    }
    in_scope_campaign_webflow_ids = [
        c["fields"][WEBFLOW_ID_FIELD] for c in all_campaigns
        if campaign_to_designer_id.get(c["id"]) in in_feed_designer_ids
        and c["fields"].get(WEBFLOW_ID_FIELD)
    ]
    print(f"   {len(in_scope_campaign_webflow_ids)} in-scope campaigns with Webflow items")

    # ── Garments in scope ──────────────────────────────────────────────────────
    print("\n👗 Loading garments...")
    all_garments = get_all_airtable_records(
        GARMENTS_TABLE_ID, fields=["Collection", WEBFLOW_ID_FIELD]
    )

    def garment_campaign_id(fields):
        linked = fields.get("Collection", [])
        return linked[0] if linked else None

    in_scope_campaign_ids = {
        c["id"] for c in all_campaigns
        if campaign_to_designer_id.get(c["id"]) in in_feed_designer_ids
    }
    in_scope_garments = {
        g["id"]: g["fields"].get(WEBFLOW_ID_FIELD)
        for g in all_garments
        if garment_campaign_id(g["fields"]) in in_scope_campaign_ids
    }
    print(f"   {len(in_scope_garments)} garments in scope")

    # ── Sightings — unrestricted fields (avoiding another 422 guessing game) ──
    print("\n🔍 Loading sightings...")
    all_sightings = get_all_airtable_records(SIGHTINGS_TABLE_ID)
    print(f"   {len(all_sightings)} total sightings")

    def sighting_garment_id(fields):
        linked = fields.get("Garment", [])
        return linked[0] if linked else None

    in_scope_sightings = [
        s for s in all_sightings
        if sighting_garment_id(s["fields"]) in in_scope_garments
    ]
    print(f"   {len(in_scope_sightings)} belong to an in-scope garment")

    # ── Sync sightings: create/update active+sold, delete everything else ─────
    print("\n📝 Syncing sightings to Webflow...")
    s_created = s_updated = s_deleted = s_skipped = s_errors = 0
    published_sighting_ids = []
    keep_garment_ids = set()

    for s in in_scope_sightings:
        at_fields   = s.get("fields", {})
        status      = at_fields.get("Status")
        existing_id = at_fields.get(WEBFLOW_ID_FIELD)
        garment_id  = sighting_garment_id(at_fields)

        if status in KEEP_STATUSES:
            keep_garment_ids.add(garment_id)
            garment_webflow_id = in_scope_garments.get(garment_id)
            wf_fields = build_sighting_fields(at_fields, garment_webflow_id)
            if not wf_fields:
                s_skipped += 1
                continue

            if existing_id:
                wf_fields.pop("slug", None)  # freeze — never change a live URL
                if webflow_update_item(SIGHTINGS_COLLECTION_ID, existing_id, wf_fields):
                    s_updated += 1
                    published_sighting_ids.append(existing_id)
                else:
                    s_errors += 1
            else:
                new_id = webflow_create_item(SIGHTINGS_COLLECTION_ID, wf_fields)
                if new_id:
                    write_webflow_id_to_airtable(SIGHTINGS_TABLE_ID, s["id"], new_id)
                    s_created += 1
                    published_sighting_ids.append(new_id)
                else:
                    s_errors += 1
        else:
            # Not active/sold — delete its individual page if one exists
            if existing_id:
                if webflow_delete_item(SIGHTINGS_COLLECTION_ID, existing_id):
                    write_webflow_id_to_airtable(SIGHTINGS_TABLE_ID, s["id"], "")
                    s_deleted += 1
                else:
                    s_errors += 1
            else:
                s_skipped += 1
        if not DRY_RUN:
            time.sleep(0.15)

    print(f"\n   Sightings: {s_created} created | {s_updated} updated | "
          f"{s_deleted} deleted | {s_skipped} skipped | {s_errors} errors")

    # ── Publish everything ────────────────────────────────────────────────────
    print("\n🚀 Publishing staged changes...")
    live_garment_webflow_ids = [
        wf_id for gid, wf_id in in_scope_garments.items()
        if gid in keep_garment_ids and wf_id
    ]

    webflow_publish_items(CAMPAIGNS_COLLECTION_ID, in_scope_campaign_webflow_ids, "campaign")
    webflow_publish_items(GARMENTS_COLLECTION_ID, live_garment_webflow_ids, "garment")
    webflow_publish_items(SIGHTINGS_COLLECTION_ID, published_sighting_ids, "sighting")

    if DRY_RUN:
        print("\n🧪 DRY RUN — no items were actually created/updated/deleted/published.")

    print("\n" + "=" * 64)
    print("  Stage 2b complete. Consolidated sync is now fully wired end-to-end.")
    print("=" * 64)


if __name__ == "__main__":
    main()
