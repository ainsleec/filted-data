#!/usr/bin/env python3
"""
Filted — One-time Rebuild: Wipe + Resync Resale Sightings (with affiliate URLs)

Solves the orphan-sighting problem discovered on 2026-07-06: the old Colab
pipeline created many Webflow Sighting items but never reliably wrote the
resulting Webflow Item ID back into Airtable — so those items are invisible
to the normal sync's delete logic (which only knows to delete IDs it can
see). This script sidesteps that entirely by:

  1. Deleting EVERY item currently in the Webflow Sightings collection,
     regardless of what Airtable's Webflow Item ID field says — a clean
     wipe rather than a reconciliation.
  2. Rebuilding it from scratch: every Airtable sighting with
     Status = Active or Sold, in scope (belongs to an in-feed-designer
     garment), gets a brand new Webflow item created — with its
     Listing URL wrapped in EPN affiliate tracking params if not already
     tagged.
  3. Publishing everything newly created.

This is destructive and irreversible for step 1 — there is no "undo" for
deleting Webflow items other than recreating them (which is exactly what
step 2 does, but any Sighting page that existed ONLY as an orphan with no
matching Active/Sold Airtable record will be gone for good, which is the
intended outcome).

SAFETY: two separate flags are required to actually run the destructive
version. Running with no flags, or just --dry-run, does nothing but report
counts.

Flags:
  --dry-run          Preview counts only. No deletes, no writes, no creates.
  --confirm-wipe      Required (in addition to omitting --dry-run) to
                       actually delete + rebuild. Without this flag, the
                       script refuses to run destructively even if
                       --dry-run is also omitted.

Required env vars:
  AIRTABLE_API_KEY (or AIRTABLE_TOKEN — both are checked)
  AIRTABLE_BASE (or AIRTABLE_BASE_ID)
  RESALE_SIGHTINGS_TABLE
  WEBFLOW_API_TOKEN
"""

import os, sys, re, time, unicodedata, requests
from datetime import datetime, timezone

DRY_RUN      = "--dry-run" in sys.argv
CONFIRM_WIPE = "--confirm-wipe" in sys.argv

LIMIT = None
for i, arg in enumerate(sys.argv):
    if arg == "--limit" and i + 1 < len(sys.argv):
        try:
            LIMIT = int(sys.argv[i + 1])
        except ValueError:
            print(f"⚠️  Invalid --limit value: {sys.argv[i + 1]!r} — ignoring, running unlimited.")

if not DRY_RUN and not CONFIRM_WIPE:
    print("=" * 64)
    print("  STOP: REFUSING TO RUN")
    print("  This script deletes every item in the Webflow Sightings")
    print("  collection. To actually run it, you must pass BOTH:")
    print("    (no --dry-run flag)  AND  --confirm-wipe")
    print("  Run with --dry-run first to preview what would happen.")
    print("=" * 64)
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY") or os.environ.get("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE") or os.environ.get("AIRTABLE_BASE_ID")
RESALE_SIGHTINGS_TABLE = os.environ["RESALE_SIGHTINGS_TABLE"]

DESIGNERS_TABLE_ID = "tbltYGtfVV574EOcw"
CAMPAIGNS_TABLE_ID = "tblyVgpD1SSCrRSgQ"
GARMENTS_TABLE_ID  = "tblmqjU4WqgCzP7cR"

WEBFLOW_API_TOKEN       = os.environ["WEBFLOW_API_TOKEN"]
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

EPN_CAMPAIGN_ID = "5339108963"
EPN_PARAMS      = "mkcid=1&mkrid=705-53470-19255-0&siteid=15&toolid=10001&mkevt=1"


def make_affiliate_url(listing_url, campaign_id=EPN_CAMPAIGN_ID):
    if not listing_url:
        return listing_url
    if "campid=" in listing_url:
        return listing_url  # already tagged — don't double-wrap
    return f"{listing_url.split('?')[0]}?campid={campaign_id}&{EPN_PARAMS}"


# ── Airtable helpers ────────────────────────────────────────────────────────────
def get_all_airtable_records(table_id, fields=None):
    records = []
    params = {"pageSize": 100}
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
        print(f"    Warning: failed to write Webflow ID back to Airtable {record_id}: {resp.status_code}")


def get_str(value, default=""):
    if value is None:
        return default
    if isinstance(value, list):
        return str(value[0]).strip() if value else default
    return str(value).strip()


# ── Slug helpers ────────────────────────────────────────────────────────────────
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


# ── Webflow helpers ─────────────────────────────────────────────────────────────
def fetch_all_webflow_items(collection_id):
    """List every item currently in a Webflow collection, regardless of Airtable."""
    items = []
    offset, limit = 0, 100
    while True:
        resp = requests.get(
            f"https://api.webflow.com/v2/collections/{collection_id}/items",
            headers=WEBFLOW_HEADERS,
            params={"limit": limit, "offset": offset},
        )
        if not resp.ok:
            print(f"   Warning: failed to list items at offset {offset}: {resp.status_code} {resp.text[:200]}")
            break
        data  = resp.json()
        batch = data.get("items", [])
        items.extend(batch)
        total = data.get("pagination", {}).get("total", 0)
        offset += limit
        if offset >= total or not batch:
            break
    return items


def webflow_delete_item(collection_id, item_id):
    if DRY_RUN:
        return True
    resp = requests.delete(
        f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}",
        headers=WEBFLOW_HEADERS,
    )
    return resp.status_code in (200, 202, 204)


def webflow_create_item(collection_id, fields, is_draft=False):
    if DRY_RUN:
        return "DRY_RUN_FAKE_ID"
    resp = requests.post(
        f"https://api.webflow.com/v2/collections/{collection_id}/items",
        headers=WEBFLOW_HEADERS,
        json={"isDraft": is_draft, "fieldData": fields},
    )
    if resp.status_code in (200, 201, 202):
        return resp.json().get("id")
    print(f"    Warning: create failed [{resp.status_code}]: {resp.text[:300]}")
    return None


def webflow_publish_items(collection_id, item_ids):
    if not item_ids:
        return
    if DRY_RUN:
        print(f"   Would publish {len(item_ids)} sighting item(s)")
        return
    for i in range(0, len(item_ids), 100):
        batch = item_ids[i:i + 100]
        resp = requests.post(
            f"https://api.webflow.com/v2/collections/{collection_id}/items/publish",
            headers=WEBFLOW_HEADERS, json={"itemIds": batch}
        )
        if resp.ok:
            print(f"   Published {len(batch)} sighting item(s)")
        else:
            print(f"    Warning: publish batch failed [{resp.status_code}]: {resp.text[:200]}")
        time.sleep(0.3)


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
        "listing-url":   make_affiliate_url(get_str(at_fields.get("Listing URL"))),
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
    print("  Filted — Wipe + Rebuild Resale Sightings")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    if DRY_RUN:
        print("  DRY RUN — no deletes, writes, or publishes will be made")
    else:
        print("  LIVE MODE — this WILL delete every current Sighting item")
    if LIMIT:
        print(f"  🔬 TEST MODE — limited to first {LIMIT} item(s) for delete and rebuild")
    print("=" * 64)

    # ── Scope (same logic as Stage 2a/2b) ─────────────────────────────────────
    print("\nLoading in-feed designers...")
    designer_records = get_all_airtable_records(
        DESIGNERS_TABLE_ID, fields=["Designer Name", "Webflow Item ID", "In Feed"]
    )
    in_feed_designers = [
        r for r in designer_records
        if r["fields"].get("In Feed") and r["fields"].get(WEBFLOW_ID_FIELD)
    ]
    in_feed_designer_ids = {r["id"] for r in in_feed_designers}
    print(f"   {len(in_feed_designers)} in-feed designer(s)")

    print("\nLoading campaigns...")
    all_campaigns = get_all_airtable_records(CAMPAIGNS_TABLE_ID, fields=["Designer"])
    campaign_to_designer_id = {
        c["id"]: c["fields"]["Designer"][0]
        for c in all_campaigns if c["fields"].get("Designer")
    }
    in_scope_campaign_ids = {
        c["id"] for c in all_campaigns
        if campaign_to_designer_id.get(c["id"]) in in_feed_designer_ids
    }

    print("\nLoading garments...")
    all_garments = get_all_airtable_records(
        GARMENTS_TABLE_ID, fields=["Collection", WEBFLOW_ID_FIELD]
    )

    def garment_campaign_id(fields):
        linked = fields.get("Collection", [])
        return linked[0] if linked else None

    in_scope_garments = {
        g["id"]: g["fields"].get(WEBFLOW_ID_FIELD)
        for g in all_garments
        if garment_campaign_id(g["fields"]) in in_scope_campaign_ids
    }
    print(f"   {len(in_scope_garments)} garments in scope")

    # ── Step 1: Wipe everything currently in the Webflow Sightings collection ─
    print("\nFetching ALL current items in Webflow Sightings collection...")
    current_items = fetch_all_webflow_items(SIGHTINGS_COLLECTION_ID)
    print(f"   {len(current_items)} items currently exist in Webflow")

    if LIMIT:
        current_items = current_items[:LIMIT]
        print(f"   🔬 Limited to first {len(current_items)} for this test run")

    print("\nDeleting all current Sighting items...")
    deleted = errors = 0
    for item in current_items:
        if webflow_delete_item(SIGHTINGS_COLLECTION_ID, item["id"]):
            deleted += 1
        else:
            errors += 1
        if not DRY_RUN:
            time.sleep(0.1)
        if (deleted + errors) % 200 == 0:
            print(f"   ... {deleted + errors}/{len(current_items)} processed")

    print(f"   Deleted: {deleted} | Errors: {errors}")

    # ── Step 2: Load Active/Sold sightings from Airtable, in scope ────────────
    print("\nLoading Active/Sold sightings from Airtable...")
    all_sightings = get_all_airtable_records(RESALE_SIGHTINGS_TABLE)

    def sighting_garment_id(fields):
        linked = fields.get("Garment", [])
        return linked[0] if linked else None

    to_rebuild = [
        s for s in all_sightings
        if s["fields"].get("Status") in KEEP_STATUSES
        and sighting_garment_id(s["fields"]) in in_scope_garments
    ]
    print(f"   {len(all_sightings)} total sightings")
    print(f"   {len(to_rebuild)} Active/Sold and in scope — will be rebuilt")

    if LIMIT:
        to_rebuild = to_rebuild[:LIMIT]
        print(f"   🔬 Limited to first {len(to_rebuild)} for this test run")

    # ── Step 3: Recreate fresh, with affiliate URLs ────────────────────────────
    print("\nRecreating sightings with affiliate URLs applied...")
    created = skipped = create_errors = 0
    new_ids = []

    for s in to_rebuild:
        at_fields   = s.get("fields", {})
        garment_id  = sighting_garment_id(at_fields)
        garment_wf  = in_scope_garments.get(garment_id)

        wf_fields = build_sighting_fields(at_fields, garment_wf)
        if not wf_fields:
            skipped += 1
            continue

        new_id = webflow_create_item(SIGHTINGS_COLLECTION_ID, wf_fields)
        if new_id:
            write_webflow_id_to_airtable(RESALE_SIGHTINGS_TABLE, s["id"], new_id)
            created += 1
            new_ids.append(new_id)
        else:
            create_errors += 1

        if not DRY_RUN:
            time.sleep(0.15)
        if created % 200 == 0 and created > 0:
            print(f"   ... {created}/{len(to_rebuild)} created")

    print(f"\n   Created: {created} | Skipped: {skipped} | Errors: {create_errors}")

    # ── Step 4: Publish everything newly created ───────────────────────────────
    print("\nPublishing rebuilt sightings...")
    webflow_publish_items(SIGHTINGS_COLLECTION_ID, new_ids)

    if DRY_RUN:
        print("\nDRY RUN — nothing was actually deleted, created, or published.")

    print("\n" + "=" * 64)
    print("  Rebuild complete.")
    print("=" * 64)


if __name__ == "__main__":
    main()
