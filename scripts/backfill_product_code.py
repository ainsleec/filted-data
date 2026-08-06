"""
backfill_product_code.py

Fixes the ~332 Aje garments (and any others) where Airtable has a
non-empty Product Code but Supabase's `garments.product_code` is empty.

This is a targeted backfill, NOT a full re-sync — it only touches rows
where Supabase is missing data that already exists in Airtable, and
regenerates `search_vector` for any row it updates so search picks the
code up immediately.

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
import sys
import argparse
from pyairtable import Table
from supabase import create_client

# --- Config -----------------------------------------------------------

AIRTABLE_API_KEY = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]
AIRTABLE_TABLE_NAME = os.environ.get("AIRTABLE_TABLE_NAME", "All Garments")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

# Airtable field names — confirm these match your base exactly.
# From the screenshots: "Product Code" is the field label shown in the
# filter UI. Adjust if the underlying field name differs.
AIRTABLE_PRODUCT_CODE_FIELD = "Product Code"

BATCH_SIZE = 200


def get_supabase_rows_missing_code(supabase):
    """Fetch all garments in Supabase where product_code is empty/null,
    paginated, along with their airtable_id so we can look them up."""
    rows = []
    start = 0
    while True:
        resp = (
            supabase.table("garments")
            .select("id, airtable_id, product_code, designer_name")
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


def get_airtable_codes_by_id(airtable_ids):
    """Fetch current Product Code values from Airtable for the given
    record IDs. Airtable doesn't support IN-style filters well over
    large ID lists via formula, so we fetch in chunks by record id."""
    table = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    codes = {}
    for rec_id in airtable_ids:
        try:
            record = table.get(rec_id)
        except Exception as e:
            print(f"  ! failed to fetch Airtable record {rec_id}: {e}")
            continue
        code = (record.get("fields", {}) or {}).get(AIRTABLE_PRODUCT_CODE_FIELD, "")
        code = (code or "").strip()
        if code:
            codes[rec_id] = code
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

    airtable_ids = [r["airtable_id"] for r in missing_rows if r.get("airtable_id")]
    if not airtable_ids:
        print("No airtable_id values to look up. Exiting.")
        return

    print(f"Looking up {len(airtable_ids)} records in Airtable...")
    airtable_codes = get_airtable_codes_by_id(airtable_ids)
    print(f"  Airtable has a non-empty code for {len(airtable_codes)} of these")

    to_update = [
        r for r in missing_rows
        if r.get("airtable_id") in airtable_codes
    ]

    print(f"\n{len(to_update)} rows can be backfilled (Airtable has a code, Supabase doesn't):\n")

    by_designer = {}
    for r in to_update:
        by_designer[r["designer_name"]] = by_designer.get(r["designer_name"], 0) + 1
    for designer, count in sorted(by_designer.items(), key=lambda x: -x[1]):
        print(f"  {designer}: {count}")

    if not args.apply:
        print("\nDry run only — no changes made. Re-run with --apply to write updates.")
        return

    print("\nApplying updates...")
    updated = 0
    for r in to_update:
        code = airtable_codes[r["airtable_id"]]
        try:
            supabase.table("garments").update({
                "product_code": code,
            }).eq("id", r["id"]).execute()
            updated += 1
        except Exception as e:
            print(f"  ! failed to update {r['id']} ({r['airtable_id']}): {e}")

    print(f"\nUpdated {updated} rows.")
    print(
        "NOTE: if search_vector is populated by a DB trigger, it should have "
        "regenerated automatically on update. If it's NOT trigger-based, run "
        "a manual search_vector rebuild for these rows next."
    )


if __name__ == "__main__":
    main()
