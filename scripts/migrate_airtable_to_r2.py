#!/usr/bin/env python3
"""
─────────────────────────────────────────────────────────────────────────────
 AIRTABLE → CLOUDFLARE R2 IMAGE MIGRATION
 Finds Airtable attachment fields, compresses images, uploads them to R2,
 and writes the new public URL back into a matching text field.

 SAFE BY DEFAULT:
   - DRY_RUN = True by default. Nothing is uploaded or changed until you
     flip it off.
   - Original Airtable attachments are NEVER deleted by this script.
     That's a separate, manual step (see bottom of file) you run only
     after confirming the migration worked.
─────────────────────────────────────────────────────────────────────────────
"""

import requests
import boto3
from PIL import Image
import io
import os
import time
import sys

# ─────────────────────────────────────────────────────────────────────────
# CONFIG — reads from environment variables (set locally via export, or as
# GitHub Actions secrets). Falls back to the literal string only if you
# prefer to hardcode for a quick local test — not recommended for git.
# ─────────────────────────────────────────────────────────────────────────

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "YOUR_AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "YOUR_BASE_ID")           # starts with "app..."
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Garments")

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "de893873641e9373ec2b5dd99540f518")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY", "YOUR_R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY", "YOUR_R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET", "filted-images")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "https://pub-9b251a5e9a524f88a7f16e0f7838fd75.r2.dev")

MAX_DIMENSION = 1200     # resize longest side to this many px
JPEG_QUALITY = 80        # 0-100, 80 is a good balance

# Suffix added to attachment field names to find/create the matching
# text field, e.g. "Image 1" -> "Image 1 URL"
URL_FIELD_SUFFIX = " URL"

# DRY_RUN and LIMIT_RECORDS can be overridden via env vars too, so the
# GitHub Actions workflow can control them without editing this file.
DRY_RUN = os.getenv("DRY_RUN", "true").lower() != "false"
_limit = os.getenv("LIMIT_RECORDS", "")
LIMIT_RECORDS = int(_limit) if _limit.isdigit() else None

# ─────────────────────────────────────────────────────────────────────────
# Airtable helpers
# ─────────────────────────────────────────────────────────────────────────

AIRTABLE_API_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json",
}


def fetch_all_records():
    records = []
    params = {"pageSize": 100}
    while True:
        resp = requests.get(AIRTABLE_API_URL, headers=AIRTABLE_HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset or (LIMIT_RECORDS and len(records) >= LIMIT_RECORDS):
            break
        params["offset"] = offset
        time.sleep(0.2)  # stay under Airtable's 5 req/sec limit
    return records[:LIMIT_RECORDS] if LIMIT_RECORDS else records


def update_record(record_id, fields):
    url = f"{AIRTABLE_API_URL}/{record_id}"
    resp = requests.patch(url, headers=AIRTABLE_HEADERS, json={"fields": fields})
    resp.raise_for_status()
    time.sleep(0.2)
    return resp.json()


def find_attachment_fields(records):
    """Scan records to find which fields contain Airtable attachments."""
    attachment_fields = set()
    for r in records:
        for field_name, value in r.get("fields", {}).items():
            if isinstance(value, list) and value and isinstance(value[0], dict) and "url" in value[0]:
                attachment_fields.add(field_name)
    return sorted(attachment_fields)


# ─────────────────────────────────────────────────────────────────────────
# R2 helpers
# ─────────────────────────────────────────────────────────────────────────

r2_client = boto3.client(
    "s3",
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
)


def compress_image(image_bytes):
    img = Image.open(io.BytesIO(image_bytes))
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")
    if max(img.size) > MAX_DIMENSION:
        img.thumbnail((MAX_DIMENSION, MAX_DIMENSION), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True)
    buf.seek(0)
    return buf


def upload_to_r2(image_buf, key):
    r2_client.upload_fileobj(
        image_buf, R2_BUCKET, key, ExtraArgs={"ContentType": "image/jpeg"}
    )
    return f"{R2_PUBLIC_URL}/{key}"


def migrate_attachment(attachment, record_id, field_name, index):
    """Download one Airtable attachment, compress it, upload to R2, return URL."""
    src_url = attachment["url"]
    resp = requests.get(src_url, timeout=20)
    resp.raise_for_status()

    original_size = len(resp.content)
    compressed = compress_image(resp.content)
    compressed_size = compressed.getbuffer().nbytes

    safe_field = field_name.replace(" ", "_").lower()
    key = f"garments/{record_id}/{safe_field}_{index}.jpg"

    if DRY_RUN:
        print(f"      [dry-run] would upload {key} "
              f"({original_size/1024:.0f}KB → {compressed_size/1024:.0f}KB)")
        return f"{R2_PUBLIC_URL}/{key}"

    url = upload_to_r2(compressed, key)
    print(f"      ✅ {key} ({original_size/1024:.0f}KB → {compressed_size/1024:.0f}KB)")
    return url


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────

def main():
    print("═" * 68)
    print("  FILTED · Airtable → R2 Image Migration")
    print(f"  Mode: {'DRY RUN (no changes will be made)' if DRY_RUN else 'LIVE'}")
    print("═" * 68 + "\n")

    print("Fetching records...")
    records = fetch_all_records()
    print(f"  {len(records)} records fetched.\n")

    attachment_fields = find_attachment_fields(records)
    if not attachment_fields:
        print("No attachment fields found. Nothing to migrate.")
        return

    print("Attachment fields found:")
    for f in attachment_fields:
        print(f"  • {f}  →  will write to  '{f}{URL_FIELD_SUFFIX}'")
    print()

    if DRY_RUN:
        print("⚠️  Before running for real, create these TEXT fields in Airtable:")
        for f in attachment_fields:
            print(f"     - {f}{URL_FIELD_SUFFIX}")
        print()

    total_images = 0
    total_records_touched = 0

    for record in records:
        record_id = record["id"]
        fields = record.get("fields", {})
        update_fields = {}

        for field_name in attachment_fields:
            attachments = fields.get(field_name)
            if not attachments:
                continue

            url_field_name = f"{field_name}{URL_FIELD_SUFFIX}"

            # Skip if already migrated (idempotent — safe to re-run)
            if fields.get(url_field_name):
                continue

            urls = []
            for i, att in enumerate(attachments, start=1):
                url = migrate_attachment(att, record_id, field_name, i)
                urls.append(url)
                total_images += 1

            # Single image -> single URL string, multiple -> comma-separated
            update_fields[url_field_name] = urls[0] if len(urls) == 1 else ", ".join(urls)

        if update_fields:
            print(f"   Record {record_id}: {list(update_fields.keys())}")
            if not DRY_RUN:
                update_record(record_id, update_fields)
            total_records_touched += 1

    print("\n" + "═" * 68)
    print(f"  {'Would touch' if DRY_RUN else 'Touched'} {total_records_touched} records, "
          f"{total_images} images.")
    if DRY_RUN:
        print("  This was a DRY RUN — nothing was uploaded or changed.")
        print("  Create the text fields listed above, then set DRY_RUN = False.")
    print("═" * 68)


if __name__ == "__main__":
    if AIRTABLE_API_KEY.startswith("YOUR_") or R2_ACCESS_KEY.startswith("YOUR_"):
        print("⚠️  Fill in your credentials in the CONFIG section before running.")
        sys.exit(1)
    main()
