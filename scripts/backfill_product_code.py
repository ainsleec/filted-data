"""
backfill_product_code.py

Fixes Supabase garments where `product_code` is empty but Airtable has
a non-empty Product Code for the same garment.

Originally this matched records by Airtable's internal record ID
(`airtable_id`), but those values in Supabase are stale (likely from
before a table rebuild), causing 100% 404s. Instead this matches by
`webflow_item_id`, which is confirmed reliably synced on both sides
and doesn't drift the way internal Airtable record IDs do.

This also fetches the whole Airtable table once instead of doing one
API call per record — faster and avoids per-record errors entirely.

Usage:
    python backfill_product_code.py            # dry run, prints what would change
    python backfill_product_code.py --apply    # actually writes to Supabase

Env vars required (matches repo secrets):
    AIRTABLE_TOKEN
    AIRTABLE_BASE_ID
    AIRTABLE_TABLE_NAME     defaults to "All Garments"
    SUPABASE_URL
    SUPABASE_SERVICE_KEY
"""

import os
import argparse
from pyairtable import Table
from supabase import create_client

# --- Config -----------------------------------------------------------

AIRTABLE_API_KEY = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]
AIRTABLE_TABLE_NAME = os.environ.get("AIRTABLE_TABLE_NAME", "All Garments")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

# Confirmed exact Airtable field names
AIRTABLE_WEBFLOW_ID_FIELD = "Webflow Item ID"
AIRTABLE_PRODUCT_CODE_FIELD = "Product Code"

BATCH_SIZE = 500


def get_supabase_rows_missing_code(supabase):
    """Fetch all garments in Supabase where product_code is empty/null,
    paginated, along with their webflow_item_id."""
    rows = []
    start = 0
    while True:
        resp = (
            supabase.table("garments")
            .select("id, webflow_item_id, product_code, designer_name")
            .or_("product_code.is.null,product_code.eq.")
            .range(start, start + BATCH_SIZE - 1)
            .execute()
        )
        batch = resp.data
        if not batch:
            break
        rows.extend(batch)
        start += BATCH_SIZE
    return rows


def get_airtable_codes_by_webflow_id():
    """Fetch the entire Airtable table once and build a lookup of
    webflow_item_id -> product_code for every record with a non-empty code."""
    table = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    codes = {}
    for record in table.all(fields=[AIRTABLE_WEBFLOW_ID_FIELD, AIRTABLE_PRODUCT_CODE_FIELD]):
        fields = record.get("fields", {}) or {}
        webflow_id = (fields.get(AIRTABLE_WEBFLOW_ID_FIELD) or "").strip()
        code = (fields.get(AIRTABLE_PRODUCT_CODE_FIELD) or "").strip()
        if webflow_id and code:
            codes[webflow_id] = code
    return codes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                         help="Actually write updates. Without this flag, dry-run only.")
    args = parser.parse_args()

    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    print("Fetching Supabase rows with empty product_code...")
    missing_rows = get_supabase_rows_missing_code(supabase)
    print(f"  found {len(missing_rows)} rows in Supabase with empty product_code")

    print("Fetching all Airtable records (single bulk fetch)...")
    airtable_codes = get_airtable_codes_by_webflow_id()
    print(f"  Airtable has {len(airtable_codes)} records with a non-empty Webflow Item ID + Product Code")

    to_update = []
    skipped_no_webflow_id = 0
    for r in missing_rows:
        wid = (r.get("webflow_item_id") or "").strip()
        if not wid:
            skipped_no_webflow_id += 1
            continue
        if wid in airtable_codes:
            to_update.append((r, airtable_codes[wid]))

    print(f"\n{len(to_update)} rows can be backfilled (matched by Webflow Item ID, Airtable has a code):")
    if skipped_no_webflow_id:
        print(f"  ({skipped_no_webflow_id} Supabase rows skipped — no webflow_item_id to match on)")

    by_designer = {}
    for r, _ in to_update:
        by_designer[r["designer_name"]] = by_designer.get(r["designer_name"], 0) + 1
    for designer, count in sorted(by_designer.items(), key=lambda x: -x[1]):
        print(f"  {designer}: {count}")

    if not args.apply:
        print("\nDry run only — no changes made. Re-run with --apply to write updates.")
        return

    print("\nApplying updates...")
    updated = 0
    for r, code in to_update:
        try:
            supabase.table("garments").update({
                "product_code": code,
            }).eq("id", r["id"]).execute()
            updated += 1
        except Exception as e:
            print(f"  ! failed to update {r['id']} (webflow_item_id={r['webflow_item_id']}): {e}")

    print(f"\nUpdated {updated} rows.")
    print(
        "NOTE: if search_vector is populated by a DB trigger, it should have "
        "regenerated automatically on update. If it's NOT trigger-based, run "
        "a manual search_vector rebuild for these rows next."
    )


if __name__ == "__main__":
    main()
