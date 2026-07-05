#!/usr/bin/env python3
"""
Filted — Webflow Sync (consolidated) — STAGE 2a: Campaigns + Garments

Syncs Campaigns and Garments from Airtable to Webflow, using the dynamic
designer filter validated in Stage 1 (Designers table, In Feed = true).

Garment lifecycle:
  - A garment is only CREATED in Webflow once it has an active or sold
    sighting (no pre-provisioning the full catalogue — protects CMS item
    limits).
  - A garment that already has a Webflow item, but no longer has any
    active/sold sighting, gets DRAFTED (isDraft=true) — never deleted.
    This preserves its slug/SEO groundwork for if it's relisted later.
  - Publishing (making draft->live changes actually visible on the site)
    happens in Stage 2b, alongside Sightings — not in this stage.

This is a FULL sync (not incremental yet) to keep this stage simple and
verifiable. Incremental mode can be layered in once this is confirmed
working correctly.

Flags:
  --dry-run   Preview counts and a sample of planned actions. No writes.

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


# ── Slug helpers (unchanged from sync.py — already correct) ───────────────────
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


def build_garment_slug(name, colour, product_code, airtable_id):
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
    return slug or id_suffix or "garment"


# ── Webflow helpers (v2 API — staged, not auto-live) ───────────────────────────
def webflow_create_item(collection_id, fields, is_draft):
    if DRY_RUN:
        return "DRY_RUN_FAKE_ID"
    url  = f"https://api.webflow.com/v2/collections/{collection_id}/items"
    resp = requests.post(url, headers=WEBFLOW_HEADERS, json={"isDraft": is_draft, "fieldData": fields})
    if resp.status_code in (200, 201, 202):
        return resp.json().get("id")
    print(f"    ⚠️  Create failed [{resp.status_code}]: {resp.text[:300]}")
    return None


def webflow_update_item(collection_id, item_id, fields, is_draft):
    if DRY_RUN:
        return True
    url  = f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}"
    resp = requests.patch(url, headers=WEBFLOW_HEADERS, json={"isDraft": is_draft, "fieldData": fields})
    if resp.status_code in (200, 202):
        return True
    print(f"    ⚠️  Update failed [{resp.status_code}]: {resp.text[:200]}")
    return False


def webflow_publish_items(collection_id, item_ids):
    """Publish a batch of staged items so they go live on the site."""
    if DRY_RUN or not item_ids:
        return
    for i in range(0, len(item_ids), 100):
        batch = item_ids[i:i + 100]
        url  = f"https://api.webflow.com/v2/collections/{collection_id}/items/publish"
        resp = requests.post(url, headers=WEBFLOW_HEADERS, json={"itemIds": batch})
        if not resp.ok:
            print(f"    ⚠️  Publish batch failed [{resp.status_code}]: {resp.text[:200]}")
        time.sleep(0.3)


# ── Field builders ────────────────────────────────────────────────────────────
def build_campaign_fields(at_fields, airtable_record_id, designer_webflow_id):
    name = get_str(at_fields.get("Collection Name"))
    if not name:
        return {}
    fields = {
        "name":                   name,
        "slug":                   slugify(get_str(at_fields.get("Slug")) or name),
        "season-code":            get_str(at_fields.get("Season Code")),
        "airtable-collection-id": airtable_record_id,
    }
    if designer_webflow_id:
        fields["designer"] = designer_webflow_id
    return fields


def build_garment_fields(at_fields, airtable_id, campaign_webflow_id, designer_webflow_id):
    name = get_str(at_fields.get("Garment Name"))
    if not name:
        return {}

    slug_base    = get_str(at_fields.get("Slug")) or name
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


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 64)
    print("  Filted — Webflow Sync — STAGE 2a: Campaigns + Garments")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    if DRY_RUN:
        print("  🧪 DRY RUN — no writes will be made")
    print("=" * 64)

    # ── Designer scope (dynamic, from Stage 1) ────────────────────────────────
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
    print(f"   {len(in_feed_designers)} in-feed designer(s): "
          f"{', '.join(get_str(r['fields'].get('Designer Name')) for r in in_feed_designers)}")

    # ── Campaigns in scope ─────────────────────────────────────────────────────
    print("\n📦 Loading campaigns...")
    all_campaigns = get_all_airtable_records(
        CAMPAIGNS_TABLE_ID,
        fields=["Collection Name", "Designer", "Season Code", "Slug", WEBFLOW_ID_FIELD]
    )
    campaign_to_designer_id = {
        c["id"]: c["fields"]["Designer"][0]
        for c in all_campaigns if c["fields"].get("Designer")
    }
    in_scope_campaigns = [
        c for c in all_campaigns
        if campaign_to_designer_id.get(c["id"]) in in_feed_designer_ids
    ]
    print(f"   {len(in_scope_campaigns)} of {len(all_campaigns)} campaigns in scope")

    campaign_created = campaign_updated = campaign_skipped = campaign_errors = 0
    campaign_lookup = {c["id"]: c["fields"].get(WEBFLOW_ID_FIELD) for c in all_campaigns if c["fields"].get(WEBFLOW_ID_FIELD)}

    for c in in_scope_campaigns:
        airtable_id = c["id"]
        at_fields   = c.get("fields", {})
        designer_id = campaign_to_designer_id.get(airtable_id)
        designer_wf_id = designer_lookup.get(designer_id)

        wf_fields = build_campaign_fields(at_fields, airtable_id, designer_wf_id)
        if not wf_fields:
            campaign_skipped += 1
            continue

        existing_id = at_fields.get(WEBFLOW_ID_FIELD)
        if existing_id:
            wf_fields.pop("slug", None)  # freeze — never change a live URL
            if webflow_update_item(CAMPAIGNS_COLLECTION_ID, existing_id, wf_fields, is_draft=False):
                campaign_updated += 1
            else:
                campaign_errors += 1
        else:
            new_id = webflow_create_item(CAMPAIGNS_COLLECTION_ID, wf_fields, is_draft=False)
            if new_id:
                write_webflow_id_to_airtable(CAMPAIGNS_TABLE_ID, airtable_id, new_id)
                campaign_lookup[airtable_id] = new_id
                campaign_created += 1
            else:
                campaign_errors += 1
        if not DRY_RUN:
            time.sleep(0.2)

    print(f"   Campaigns: {campaign_created} created | {campaign_updated} updated | "
          f"{campaign_skipped} skipped | {campaign_errors} errors")

    # ── Garments in scope ──────────────────────────────────────────────────────
    print("\n👗 Loading garments...")
    all_garments = get_all_airtable_records(
        GARMENTS_TABLE_ID,
        fields=["Garment Name", "Collection", "Product Code", "Product Colour",
                "Category", "RRP", "Image 1", "Image 2", "Slug", WEBFLOW_ID_FIELD]
    )

    def garment_campaign_id(fields):
        linked = fields.get("Collection", []) or fields.get("Campaign", [])
        return linked[0] if linked else None

    in_scope_campaign_ids = {c["id"] for c in in_scope_campaigns}
    in_scope_garments = [
        g for g in all_garments
        if garment_campaign_id(g["fields"]) in in_scope_campaign_ids
    ]
    print(f"   {len(in_scope_garments)} of {len(all_garments)} garments in scope")

    # ── Determine which garments should be live (have active/sold sighting) ──
    print("\n🔍 Loading sightings to determine live/draft status...")
    all_sightings = get_all_airtable_records(SIGHTINGS_TABLE_ID, fields=["Status", "Verified", "Garment"])

    def sighting_garment_id(fields):
        linked = fields.get("Garment", [])
        return linked[0] if linked else None

    keep_garment_ids = set()
    for s in all_sightings:
        status = s["fields"].get("Status")
        if status == ACTIVE_STATUS and s["fields"].get("Verified"):
            gid = sighting_garment_id(s["fields"])
            if gid:
                keep_garment_ids.add(gid)
        elif status == SOLD_STATUS:
            gid = sighting_garment_id(s["fields"])
            if gid:
                keep_garment_ids.add(gid)

    print(f"   {len(keep_garment_ids)} garments have an active/sold sighting (should be LIVE)")

    # ── Sync garments ──────────────────────────────────────────────────────────
    print("\n📝 Syncing garments to Webflow...")
    g_created = g_updated = g_drafted = g_skipped = g_errors = 0
    published_ids = []

    for g in in_scope_garments:
        airtable_id = g["id"]
        at_fields   = g.get("fields", {})
        existing_id = at_fields.get(WEBFLOW_ID_FIELD)
        should_be_live = airtable_id in keep_garment_ids

        # Never create a brand-new Webflow item for a garment with no active/sold
        # sighting — only garments that have earned a listing get a page.
        if not existing_id and not should_be_live:
            g_skipped += 1
            continue

        linked_campaign_id = garment_campaign_id(at_fields)
        campaign_wf_id     = campaign_lookup.get(linked_campaign_id)
        designer_id        = campaign_to_designer_id.get(linked_campaign_id)
        designer_wf_id      = designer_lookup.get(designer_id)

        wf_fields = build_garment_fields(at_fields, airtable_id, campaign_wf_id, designer_wf_id)
        if not wf_fields:
            g_skipped += 1
            continue

        is_draft = not should_be_live

        if existing_id:
            wf_fields.pop("slug", None)  # freeze — never change a live URL
            if webflow_update_item(GARMENTS_COLLECTION_ID, existing_id, wf_fields, is_draft=is_draft):
                g_updated += 1
                if should_be_live:
                    published_ids.append(existing_id)
                if is_draft:
                    g_drafted += 1
            else:
                g_errors += 1
        else:
            new_id = webflow_create_item(GARMENTS_COLLECTION_ID, wf_fields, is_draft=is_draft)
            if new_id:
                write_webflow_id_to_airtable(GARMENTS_TABLE_ID, airtable_id, new_id)
                g_created += 1
                if should_be_live:
                    published_ids.append(new_id)
            else:
                g_errors += 1
        if not DRY_RUN:
            time.sleep(0.2)

    print(f"\n   Garments: {g_created} created | {g_updated} updated | "
          f"{g_drafted} newly drafted | {g_skipped} skipped | {g_errors} errors")
    print(f"   {len(published_ids)} garment(s) queued for publish (deferred to Stage 2b)")

    if DRY_RUN:
        print("\n🧪 DRY RUN — no items were actually created/updated/published.")

    print("\n" + "=" * 64)
    print("  Stage 2a complete.")
    print("  Publishing (making draft/live changes visible) happens in Stage 2b,")
    print("  alongside Sightings sync.")
    print("=" * 64)


if __name__ == "__main__":
    main()
