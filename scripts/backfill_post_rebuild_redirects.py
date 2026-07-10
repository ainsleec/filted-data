"""
backfill_post_rebuild_redirects.py — one-off

The missing piece from the wipe-and-rebuild plan: before wiping Webflow's
Garments collection, snapshot_current_slugs.py captured every garment's
Airtable Record ID -> live slug at that moment (current_slugs_snapshot.csv).
After the wipe and the real webflow_sync.py run rebuilt everything from
scratch, some garments' new slugs will differ from what they were before
(different formula output, fixed bugs, etc.) — exactly like the
Consonance Dress / Valerie Broderie Shirt cases found earlier, just
potentially at much wider scale now that literally everything was
recreated.

This script closes that gap: for every garment present in BOTH the
pre-wipe snapshot AND the post-rebuild garments.json, compare old slug
vs new slug. Where they differ, write a KV redirect (same scheme
worker.js already reads on every /garments/* request) so any
already-indexed or bookmarked old URL 301s instead of 404ing.

Garments that were in the snapshot but are NOT in the new garments.json
(i.e. they no longer qualify for a Webflow page after the rebuild) can't
be redirected to anything — there's no live page to point at. These are
written to a separate report for manual review rather than guessed at.

Reads two local files (expects to run from the repo root, after
checkout, with both files present):
  current_slugs_snapshot.csv   — from snapshot_current_slugs.py
  garments.json                — from the real webflow_sync.py run

Required env vars:
  CLOUDFLARE_ACCOUNT_ID
  CLOUDFLARE_KV_NAMESPACE_ID
  CLOUDFLARE_API_TOKEN
"""

import os
import csv
import json
import time
import requests

SNAPSHOT_PATH = "current_slugs_snapshot.csv"
GARMENTS_JSON_PATH = "garments.json"
NO_LONGER_LIVE_REPORT_PATH = "redirects_no_longer_live.csv"

CLOUDFLARE_ACCOUNT_ID      = os.environ["CLOUDFLARE_ACCOUNT_ID"]
CLOUDFLARE_KV_NAMESPACE_ID = os.environ["CLOUDFLARE_KV_NAMESPACE_ID"]
CLOUDFLARE_API_TOKEN       = os.environ["CLOUDFLARE_API_TOKEN"]

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"  # defaults SAFE


def load_snapshot():
    """airtable_record_id -> (old_slug, name)"""
    rows = {}
    with open(SNAPSHOT_PATH, newline="", encoding="utf-8-sig") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            aid = row.get("airtable_record_id")
            slug = row.get("current_slug")
            if aid and slug:
                rows[aid] = (slug, row.get("name", ""))
    return rows


def load_new_garments():
    """airtable_id -> (new_slug, name)"""
    with open(GARMENTS_JSON_PATH) as fp:
        garments = json.load(fp)
    return {g["id"]: (g["slug"], g.get("name", "")) for g in garments if g.get("id") and g.get("slug")}


def kv_write_redirect(old_slug, new_slug):
    key = f"redirect:/garments/{old_slug}"
    value = f"/garments/{new_slug}"
    if DRY_RUN:
        print(f"  [DRY RUN] would write: {key} -> {value}")
        return True
    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}"
        f"/storage/kv/namespaces/{CLOUDFLARE_KV_NAMESPACE_ID}/values/{key}"
    )
    res = requests.put(url, headers={"Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}"}, data=value, timeout=15)
    if not res.ok:
        print(f"  WARNING: failed to write redirect {old_slug} -> {new_slug}: {res.status_code} {res.text[:200]}")
        return False
    return True


def main():
    print("=== backfill_post_rebuild_redirects.py — starting ===")
    if DRY_RUN:
        print("*** DRY RUN — no KV writes will happen (set DRY_RUN=false to write for real) ***")

    print(f"Loading pre-wipe snapshot from {SNAPSHOT_PATH}...")
    snapshot = load_snapshot()
    print(f"  {len(snapshot)} garments in the pre-wipe snapshot")

    print(f"Loading post-rebuild garments from {GARMENTS_JSON_PATH}...")
    new_garments = load_new_garments()
    print(f"  {len(new_garments)} garments in the current garments.json")

    changed, unchanged, no_longer_live = [], 0, []

    for airtable_id, (old_slug, old_name) in snapshot.items():
        if airtable_id not in new_garments:
            no_longer_live.append({
                "airtable_id": airtable_id,
                "old_slug": old_slug,
                "name": old_name,
            })
            continue

        new_slug, new_name = new_garments[airtable_id]
        if new_slug != old_slug:
            changed.append({
                "airtable_id": airtable_id,
                "old_slug": old_slug,
                "new_slug": new_slug,
                "name": new_name or old_name,
            })
        else:
            unchanged += 1

    print(f"\nUnchanged (no redirect needed): {unchanged}")
    print(f"Slug changed (redirect needed): {len(changed)}")
    print(f"No longer live (can't redirect — no current page to point at): {len(no_longer_live)}")

    written, failed = 0, 0
    for i, item in enumerate(changed, 1):
        ok = kv_write_redirect(item["old_slug"], item["new_slug"])
        if ok:
            written += 1
        else:
            failed += 1
        if i % 100 == 0:
            print(f"  ...{i}/{len(changed)} processed")
        time.sleep(0.1)

    if no_longer_live:
        with open(NO_LONGER_LIVE_REPORT_PATH, "w", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=["airtable_id", "old_slug", "name"])
            writer.writeheader()
            writer.writerows(no_longer_live)
        print(f"\n{len(no_longer_live)} garments no longer have a live page — written to "
              f"{NO_LONGER_LIVE_REPORT_PATH} for manual review. These old URLs will 404 "
              f"until/unless you decide what they should point to instead (e.g. the "
              f"designer's archive page, or nothing).")

    print(f"\nDone — {written} redirects written, {failed} failed.")
    if DRY_RUN:
        print("This was a dry run — nothing was actually written. Set DRY_RUN=false to apply.")


if __name__ == "__main__":
    main()
