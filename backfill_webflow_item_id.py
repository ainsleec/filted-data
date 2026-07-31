#!/usr/bin/env python3
"""
backfill_webflow_item_id.py — one-off fix for Supabase garments.webflow_item_id

BACKGROUND: webflow_sync.py has never written garments.webflow_item_id
back to Supabase — it writes slug (update_supabase_garment_slug) and
image_url (update_supabase_garment_image) on every run, but the
equivalent write for webflow_item_id was simply missing. Since
search_garments() filters to `webflow_item_id is not null`, any garment
whose Supabase row was created with this column blank has been
permanently invisible in site search ever since, regardless of being
live and published on Webflow. Confirmed via
  SELECT COUNT(*) FROM garments WHERE webflow_item_id IS NULL OR webflow_item_id = '';
-> 6,819 of ~7,000 garments affected (2026-08).

This script is the one-time cleanup for the rows already broken.
webflow_sync.py has a separate patch (update_supabase_garment_webflow_id,
called in sync_garments()) so this gap does not reopen on future runs —
that patch is NOT part of this script and must be applied separately.

SOURCE OF TRUTH FOR THIS BACKFILL: garments.json, not a fresh Airtable/
Webflow API call. webflow_sync.py's export_garments_json() has always
written a correct webflow_item_id for every garment it has ever synced,
keyed by Airtable record ID — so the data needed to fix every affected
row already exists, committed in the repo, and reading it avoids ~7,000
additional Airtable/Webflow API calls entirely.

Matching: garments.json entries are keyed by Airtable record ID (the
"id" field in each entry, per export_garments_json()). Supabase's
garments.airtable_id is the join key on the other side.

Required env vars:
  SUPABASE_URL, SUPABASE_SERVICE_KEY

Usage:
  python backfill_webflow_item_id.py --dry-run   # preview only, no writes
  python backfill_webflow_item_id.py             # apply the backfill
"""

import os
import sys
import json
import time
import requests

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true" or "--dry-run" in sys.argv

SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

GARMENTS_JSON_PATH = "garments.json"


def get_supabase_headers():
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }


def load_all_supabase_garments():
    """Paginated fetch of every garments row's id, airtable_id, and
    current webflow_item_id — enough to decide which rows need fixing
    without pulling the entire table's other columns."""
    page_size = 1000
    offset    = 0
    rows      = []
    while True:
        headers = get_supabase_headers()
        headers["Range-Unit"] = "items"
        headers["Range"]      = f"{offset}-{offset + page_size - 1}"
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/garments",
            headers=headers,
            params={"select": "id,airtable_id,webflow_item_id"},
            timeout=30,
        )
        if not resp.ok:
            print(f"   ⚠️  Supabase load failed at offset {offset}: {resp.status_code} {resp.text[:200]}")
            break
        batch = resp.json()
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def load_garments_json():
    try:
        with open(GARMENTS_JSON_PATH) as fp:
            data = json.load(fp)
        if not isinstance(data, list):
            raise ValueError("garments.json is not a list at its top level")
        return data
    except FileNotFoundError:
        print(f"❌ {GARMENTS_JSON_PATH} not found in the current directory. "
              f"Run this from the repo root (or wherever webflow_sync.py's "
              f"committed garments.json lives), or pull the latest version first.")
        sys.exit(1)
    except (json.JSONDecodeError, ValueError) as e:
        print(f"❌ Couldn't parse {GARMENTS_JSON_PATH}: {e}")
        sys.exit(1)


def patch_webflow_item_id(supabase_uuid, webflow_item_id):
    if DRY_RUN:
        return True
    resp = requests.patch(
        f"{SUPABASE_URL}/rest/v1/garments",
        headers=get_supabase_headers(),
        params={"id": f"eq.{supabase_uuid}"},
        json={"webflow_item_id": webflow_item_id},
        timeout=15,
    )
    if not resp.ok:
        print(f"   ⚠️  FAILED for {supabase_uuid}: {resp.status_code} {resp.text[:150]}")
    return resp.ok


def main():
    print("🔄 backfill_webflow_item_id.py")
    if DRY_RUN:
        print("   🧪 DRY RUN — no writes will be made\n")

    print(f"📦 Loading {GARMENTS_JSON_PATH}...")
    garments_data = load_garments_json()
    # Keyed by Airtable record ID (the "id" field per export_garments_json()).
    json_by_airtable_id = {
        g["id"]: g.get("webflow_item_id")
        for g in garments_data
        if isinstance(g, dict) and g.get("id") and g.get("webflow_item_id")
    }
    print(f"   {len(json_by_airtable_id)} entries with a populated webflow_item_id")

    print("\n📊 Loading Supabase garments (id, airtable_id, webflow_item_id)...")
    supabase_rows = load_all_supabase_garments()
    print(f"   {len(supabase_rows)} rows loaded")

    to_fix = []
    already_correct = missing_from_json = no_airtable_id = 0

    for row in supabase_rows:
        airtable_id = row.get("airtable_id")
        current_wf_id = row.get("webflow_item_id")

        if not airtable_id:
            no_airtable_id += 1
            continue

        correct_wf_id = json_by_airtable_id.get(airtable_id)
        if not correct_wf_id:
            missing_from_json += 1
            continue

        if current_wf_id == correct_wf_id:
            already_correct += 1
            continue

        to_fix.append({
            "supabase_id": row["id"],
            "airtable_id": airtable_id,
            "old_value": current_wf_id,
            "new_value": correct_wf_id,
        })

    print(f"\n   Already correct:                     {already_correct}")
    print(f"   No airtable_id on Supabase row:        {no_airtable_id}")
    print(f"   No matching entry in garments.json:    {missing_from_json}  "
          f"(these need a fresh sync run, not this backfill — garments.json "
          f"has never synced them either)")
    print(f"   🔧 Rows to fix:                        {len(to_fix)}")

    if not to_fix:
        print("\n   Nothing to do.")
        return

    if DRY_RUN:
        print(f"\n🧪 DRY RUN — would patch {len(to_fix)} rows. Sample:")
        for item in to_fix[:10]:
            print(f"   • supabase_id={item['supabase_id']} airtable_id={item['airtable_id']} "
                  f"'{item['old_value']}' -> '{item['new_value']}'")
        if len(to_fix) > 10:
            print(f"   ...and {len(to_fix) - 10} more")
        return

    print(f"\n📝 Patching {len(to_fix)} rows in Supabase...")
    succeeded, failed = 0, 0
    for i, item in enumerate(to_fix):
        ok = patch_webflow_item_id(item["supabase_id"], item["new_value"])
        if ok:
            succeeded += 1
        else:
            failed += 1
        time.sleep(0.05)
        if (i + 1) % 500 == 0:
            print(f"   ... {i + 1}/{len(to_fix)} processed")

    print(f"\n✅ Done — {succeeded} succeeded | {failed} FAILED")
    if failed:
        print("   Re-run this script to retry the failed rows — it's safe to "
              "run repeatedly since already-correct rows are skipped.")


if __name__ == "__main__":
    main()
