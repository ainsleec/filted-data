"""
backfill_supabase_slugs.py — one-off correction pass

Problem: Supabase `garments.slug` was seeded once (2026-06-10, ~700+ rows,
confirmed via SQL: every row has created_at == updated_at, i.e. never
touched since) using an old code-suffixed slug convention
(e.g. "contina-day-dress-6416d"). webflow_sync.py's build_slug() has
never written to Supabase at all — it only READS a Supabase uuid via
get_supabase_garment_uuid(). So Webflow has the correct, current
colour-based slug (e.g. "contina-day-dress"), but Supabase was never
told about it.

This script is the fix: for every Supabase garments row with a
webflow_item_id, fetch the live Webflow item, compare its real slug
against Supabase's stored slug, and update Supabase wherever they
differ. Webflow is treated as the source of truth — this script never
generates or guesses a slug itself.

Run with DRY_RUN=true first (default) to see what would change without
writing anything. Set DRY_RUN=false to actually patch Supabase.

Requires the same env vars as webflow_sync.py: SUPABASE_URL,
SUPABASE_ANON_KEY (or a service-role key if RLS blocks anon writes),
WEBFLOW_API_TOKEN.
"""

import os
import sys
import time
import json
import requests

SUPABASE_URL      = os.environ["SUPABASE_URL"]
SUPABASE_KEY      = os.environ["SUPABASE_ANON_KEY"]  # use service-role key if RLS blocks writes
WEBFLOW_API_TOKEN = os.environ["WEBFLOW_API_TOKEN"]
GARMENTS_COLLECTION_ID = "68774f3e850c7a30ebc3a0aa"

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}
WEBFLOW_HEADERS = {
    "Authorization": f"Bearer {WEBFLOW_API_TOKEN}",
    "accept": "application/json",
}

REPORT_PATH = "slug_backfill_report.json"


def supabase_fetch_all_garments():
    """Paginate through every garments row that has a webflow_item_id."""
    rows, offset, page_size = [], 0, 1000
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/garments"
            f"?select=id,airtable_id,name,slug,webflow_item_id"
            f"&webflow_item_id=not.is.null"
            f"&order=id"
            f"&offset={offset}&limit={page_size}"
        )
        res = requests.get(url, headers=SUPABASE_HEADERS, timeout=30)
        res.raise_for_status()
        batch = res.json()
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def webflow_get_item(item_id, max_retries=5):
    url = f"https://api.webflow.com/v2/collections/{GARMENTS_COLLECTION_ID}/items/{item_id}"
    for attempt in range(max_retries):
        res = requests.get(url, headers=WEBFLOW_HEADERS, timeout=30)
        if res.status_code == 404:
            return None
        if res.ok:
            return res.json()
        if res.status_code == 429 or res.status_code >= 500:
            wait = int(res.headers.get("Retry-After", 5 * (attempt + 1)))
            print(f"  Webflow {res.status_code} on GET {item_id} — waiting {wait}s, retry {attempt + 1}/{max_retries}")
            time.sleep(wait)
            continue
        res.raise_for_status()
    raise requests.exceptions.HTTPError(f"Exhausted retries on GET {item_id}")


def supabase_update_slug(row_id, new_slug):
    if DRY_RUN:
        return True
    url = f"{SUPABASE_URL}/rest/v1/garments?id=eq.{row_id}"
    res = requests.patch(url, headers=SUPABASE_HEADERS, json={"slug": new_slug}, timeout=15)
    if not res.ok:
        print(f"  FAILED to update {row_id}: {res.status_code} {res.text[:200]}")
        return False
    return True


def main():
    print("=== backfill_supabase_slugs.py — starting ===")
    if DRY_RUN:
        print("*** DRY RUN MODE — no Supabase writes will actually happen ***")

    rows = supabase_fetch_all_garments()
    print(f"Fetched {len(rows)} Supabase garments rows with a webflow_item_id")

    changes = []
    missing_webflow = []
    unchanged = 0
    failures = []

    for i, row in enumerate(rows, 1):
        row_id = row["id"]
        old_slug = row.get("slug")
        wf_item_id = row["webflow_item_id"]

        try:
            item = webflow_get_item(wf_item_id)
        except Exception as e:
            failures.append({"id": row_id, "name": row.get("name"), "error": str(e)})
            print(f"  FAILED fetching Webflow item for {row_id} ({row.get('name')}): {e}")
            continue

        if item is None:
            # Webflow item no longer exists (deleted on Webflow side) — nothing to reconcile against
            missing_webflow.append({"id": row_id, "name": row.get("name"), "webflow_item_id": wf_item_id})
            continue

        real_slug = item.get("fieldData", {}).get("slug")

        if not real_slug:
            failures.append({"id": row_id, "name": row.get("name"), "error": "Webflow item has no slug field"})
            continue

        if real_slug != old_slug:
            ok = supabase_update_slug(row_id, real_slug)
            changes.append({
                "id": row_id,
                "airtable_id": row.get("airtable_id"),
                "name": row.get("name"),
                "old_slug": old_slug,
                "new_slug": real_slug,
                "applied": ok and not DRY_RUN,
            })
            if DRY_RUN:
                print(f"  [DRY RUN] would update {row_id} ({row.get('name')}): '{old_slug}' -> '{real_slug}'")
        else:
            unchanged += 1

        if i % 200 == 0:
            print(f"  ...processed {i}/{len(rows)}")

        time.sleep(0.1)  # gentle on Webflow's rate limit

    print("\n=== Summary ===")
    print(f"Total rows checked:      {len(rows)}")
    print(f"Already correct:         {unchanged}")
    print(f"Slug mismatches found:   {len(changes)}")
    print(f"Webflow item missing:    {len(missing_webflow)}")
    print(f"Failures:                {len(failures)}")

    report = {
        "dry_run": DRY_RUN,
        "total_checked": len(rows),
        "unchanged": unchanged,
        "changes": changes,
        "missing_webflow_items": missing_webflow,
        "failures": failures,
    }
    with open(REPORT_PATH, "w") as fp:
        json.dump(report, fp, indent=2)
    print(f"\nFull report written to {REPORT_PATH}")

    if DRY_RUN:
        print("\nThis was a DRY RUN — no Supabase rows were actually changed.")
        print("Review slug_backfill_report.json, then re-run with DRY_RUN=false to apply.")

    print("=== backfill_supabase_slugs.py — done ===")


if __name__ == "__main__":
    main()
