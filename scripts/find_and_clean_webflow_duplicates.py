#!/usr/bin/env python3
"""
Filted — Webflow Garment Duplicate Finder & Cleaner
Standalone script. Safe by default: running it with no flags only REPORTS
duplicates, it never deletes or modifies anything.

Background: a gap in webflow_sync.py's Airtable write-back (see the
airtable_update patch) could leave a garment's "Webflow Item ID" field
blank in Airtable even after a Webflow item was successfully created for
it. The next sync run would then see a blank ID, assume the garment had
never been created, and create a second Webflow item for it — same Name,
different slug.

This script:
  1. Pulls every live item from the Webflow Garments collection.
  2. Groups items by normalized Name (case-insensitive, whitespace-
     collapsed) — a duplicate group is any Name with 2+ items.
  3. For each group, cross-references Airtable's "All Garments" table to
     find which (if any) Webflow Item ID Airtable currently considers
     canonical for that garment.
  4. Prints a full report: every duplicate group, every item ID/slug/
     created-date in it, and which one (if any) is Airtable's canonical
     reference.
  5. Only with --delete does it act — and even then, only after you
     confirm the plan looks right, and only ever deletes the non-
     canonical duplicate(s), never the one Airtable references. Also
     writes a Cloudflare KV redirect from each deleted item's slug to the
     surviving item's slug, so no live-indexed URL 404s.

USAGE:
  python find_and_clean_webflow_duplicates.py                 # report only
  python find_and_clean_webflow_duplicates.py --delete         # report, then prompt to confirm and delete
  python find_and_clean_webflow_duplicates.py --delete --yes   # skip confirmation (use with care)

Required env vars (same as webflow_sync.py):
  AIRTABLE_TOKEN, SUPABASE_URL, SUPABASE_ANON_KEY (or SERVICE_KEY),
  WEBFLOW_API_TOKEN
Optional:
  CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_KV_NAMESPACE_ID, CLOUDFLARE_API_TOKEN
  (only needed if --delete is used — redirects are skipped with a warning
  if these aren't set)
"""

import os
import sys
import time
import json
import argparse
import requests
from collections import defaultdict

# ── Config — match webflow_sync.py's real values ────────────────────────────
AIRTABLE_TOKEN  = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE   = "appUk1ThnHvWwFDHG"
GARMENTS_TABLE  = "All Garments"
FLD_WEBFLOW_ITEM_ID = "Webflow Item ID"
FLD_GARMENT_NAME    = "Garment Name"
FLD_NAME_FORMULA    = "Name"

WEBFLOW_API_TOKEN      = os.environ["WEBFLOW_API_TOKEN"]
GARMENTS_COLLECTION_ID = "68774f3e850c7a30ebc3a0aa"

CLOUDFLARE_ACCOUNT_ID      = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
CLOUDFLARE_KV_NAMESPACE_ID = os.environ.get("CLOUDFLARE_KV_NAMESPACE_ID")
CLOUDFLARE_API_TOKEN       = os.environ.get("CLOUDFLARE_API_TOKEN")

AIRTABLE_HEADERS = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
WEBFLOW_HEADERS  = {
    "Authorization": f"Bearer {WEBFLOW_API_TOKEN}",
    "accept": "application/json",
    "content-type": "application/json",
}

WEBFLOW_PAGINATION_SLEEP = 1.1
REPORT_PATH = "webflow_duplicate_report.json"


# ── Retry wrapper (same pattern as webflow_sync.py) ─────────────────────────
def webflow_request(method, url, json_body=None, params=None, max_retries=5):
    for attempt in range(max_retries):
        res = requests.request(method, url, headers=WEBFLOW_HEADERS,
                               json=json_body, params=params, timeout=30)
        if res.ok:
            return res
        if res.status_code == 429 or res.status_code >= 500:
            wait = int(res.headers.get("Retry-After", 5 * (attempt + 1)))
            print(f"  Webflow {res.status_code} on {method} {url} — waiting {wait}s, retry {attempt + 1}/{max_retries}")
            time.sleep(wait + 1)
            continue
        raise requests.exceptions.HTTPError(f"{res.status_code} on {method} {url}: {res.text[:500]}")
    raise requests.exceptions.HTTPError(f"Exhausted {max_retries} retries on {method} {url}")


def webflow_all_items(collection_id):
    items, offset, limit = [], 0, 100
    while True:
        res = webflow_request(
            "GET",
            f"https://api.webflow.com/v2/collections/{collection_id}/items",
            params={"limit": limit, "offset": offset},
        )
        data = res.json()
        page_items = data.get("items", [])
        items.extend(page_items)
        total = data.get("pagination", {}).get("total", 0)
        offset += len(page_items)
        if offset >= total or not page_items:
            break
        time.sleep(WEBFLOW_PAGINATION_SLEEP)
    return items


def webflow_delete_item(collection_id, item_id):
    return webflow_request("DELETE", f"https://api.webflow.com/v2/collections/{collection_id}/items/{item_id}")


# ── Airtable ─────────────────────────────────────────────────────────────
def airtable_fetch_all(table, fields=None):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{requests.utils.quote(table)}"
    records, offset = [], None
    while True:
        params = {}
        if offset:
            params["offset"] = offset
        if fields:
            params["fields[]"] = fields
        res = requests.get(url, headers=AIRTABLE_HEADERS, params=params, timeout=30)
        res.raise_for_status()
        data = res.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return records


# ── Cloudflare KV redirect ──────────────────────────────────────────────────
def kv_write_redirect(old_slug, new_slug, path_prefix="/garments/"):
    if not (CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_KV_NAMESPACE_ID and CLOUDFLARE_API_TOKEN):
        print(f"  WARNING: Cloudflare env vars not set — skipping redirect {old_slug} -> {new_slug}")
        return
    key = f"redirect:{path_prefix}{old_slug}"
    value = f"{path_prefix}{new_slug}"
    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}"
        f"/storage/kv/namespaces/{CLOUDFLARE_KV_NAMESPACE_ID}/values/{key}"
    )
    res = requests.put(url, headers={"Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}"}, data=value, timeout=15)
    if not res.ok:
        print(f"  WARNING: failed to write redirect {old_slug} -> {new_slug}: {res.status_code} {res.text}")
    else:
        print(f"  Redirect written: {path_prefix}{old_slug} -> {value}")


# ── Core logic ───────────────────────────────────────────────────────────
def normalize_name(name):
    return " ".join((name or "").strip().lower().split())


def build_report():
    print("Loading all Webflow Garments items...")
    items = webflow_all_items(GARMENTS_COLLECTION_ID)
    print(f"  {len(items)} live items loaded")

    print("Loading Airtable garments (for canonical Webflow Item ID lookup)...")
    at_records = airtable_fetch_all(GARMENTS_TABLE, fields=[FLD_GARMENT_NAME, FLD_NAME_FORMULA, FLD_WEBFLOW_ITEM_ID])
    # Map: webflow_item_id -> airtable record id (so we know which live
    # Webflow item Airtable currently considers canonical)
    canonical_wf_ids = {
        r["fields"][FLD_WEBFLOW_ITEM_ID]: r["id"]
        for r in at_records
        if r["fields"].get(FLD_WEBFLOW_ITEM_ID)
    }
    print(f"  {len(canonical_wf_ids)} garments have a Webflow Item ID recorded in Airtable")

    groups = defaultdict(list)
    for item in items:
        fd = item.get("fieldData", {})
        name = fd.get("name", "")
        groups[normalize_name(name)].append({
            "item_id": item["id"],
            "name": name,
            "slug": fd.get("slug"),
            "created_on": item.get("createdOn"),
            "last_updated": item.get("lastUpdated"),
            "is_canonical_in_airtable": item["id"] in canonical_wf_ids,
        })

    duplicate_groups = {name: entries for name, entries in groups.items() if len(entries) > 1}
    return duplicate_groups


def print_report(duplicate_groups):
    if not duplicate_groups:
        print("\n✅ No duplicate garment names found.")
        return

    print(f"\n⚠️  {len(duplicate_groups)} garment name(s) with duplicates "
          f"({sum(len(v) for v in duplicate_groups.values())} total items involved):\n")

    for name, entries in sorted(duplicate_groups.items()):
        entries_sorted = sorted(entries, key=lambda e: e["created_on"] or "")
        print(f"— {entries_sorted[0]['name']!r}  ({len(entries_sorted)} copies)")
        for e in entries_sorted:
            marker = "  [AIRTABLE CANONICAL]" if e["is_canonical_in_airtable"] else ""
            print(f"    {e['item_id']}  slug={e['slug']!r}  created={e['created_on']}{marker}")
        print()


def choose_survivor(entries):
    """Survivor priority: (1) the one Airtable references as canonical,
    (2) if none/multiple are referenced, the earliest-created item."""
    canonical = [e for e in entries if e["is_canonical_in_airtable"]]
    if len(canonical) == 1:
        return canonical[0]
    if len(canonical) > 1:
        # Shouldn't normally happen, but if it does, prefer the earliest
        # of the ones Airtable actually points to.
        return sorted(canonical, key=lambda e: e["created_on"] or "")[0]
    return sorted(entries, key=lambda e: e["created_on"] or "")[0]


def run_cleanup(duplicate_groups, auto_confirm=False):
    plan = []
    for name, entries in duplicate_groups.items():
        survivor = choose_survivor(entries)
        for e in entries:
            if e["item_id"] != survivor["item_id"]:
                plan.append({"name": name, "delete": e, "keep": survivor})

    print(f"\nCleanup plan: {len(plan)} duplicate item(s) will be DELETED, "
          f"{len(duplicate_groups)} survivor(s) kept.\n")
    for p in plan[:30]:
        print(f"  DELETE {p['delete']['item_id']} (slug={p['delete']['slug']!r}) "
              f"-> redirect to KEEP {p['keep']['item_id']} (slug={p['keep']['slug']!r})")
    if len(plan) > 30:
        print(f"  ...and {len(plan) - 30} more")

    if not auto_confirm:
        confirm = input(f"\nProceed with deleting these {len(plan)} items? Type 'yes' to continue: ").strip().lower()
        if confirm != "yes":
            print("Aborted — nothing was deleted.")
            return

    deleted, failed = 0, []
    for p in plan:
        try:
            webflow_delete_item(GARMENTS_COLLECTION_ID, p["delete"]["item_id"])
            deleted += 1
            if p["delete"]["slug"] and p["keep"]["slug"]:
                kv_write_redirect(p["delete"]["slug"], p["keep"]["slug"])
        except Exception as e:
            failed.append((p["delete"]["item_id"], str(e)))
            print(f"  FAILED to delete {p['delete']['item_id']}: {e}")
        time.sleep(0.3)

    print(f"\nDone. Deleted {deleted}/{len(plan)}. {len(failed)} failure(s).")
    if failed:
        for item_id, err in failed:
            print(f"  - {item_id}: {err[:200]}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delete", action="store_true", help="Actually delete duplicates after review")
    parser.add_argument("--yes", action="store_true", help="Skip the confirmation prompt (use with --delete)")
    args = parser.parse_args()

    duplicate_groups = build_report()
    print_report(duplicate_groups)

    with open(REPORT_PATH, "w") as fp:
        json.dump(duplicate_groups, fp, indent=2)
    print(f"Full report written to {REPORT_PATH}")

    if duplicate_groups and args.delete:
        run_cleanup(duplicate_groups, auto_confirm=args.yes)
    elif duplicate_groups:
        print("\nRun again with --delete to clean these up (you'll be asked to confirm).")


if __name__ == "__main__":
    main()
