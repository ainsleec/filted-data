"""
Filted — Retroactive Image Recompression
Downloads each garment image already in Supabase Storage, resizes/compresses it,
and re-uploads to the SAME path (overwrite). URLs do not change, nothing breaks
on the frontend — this purely shrinks file size to bring Storage back under quota.

Run this ONCE to fix existing images. For new garments going forward, the
compression step should also be added to image_upload.py (separate change).
"""

import os
import io
import time
import requests
from PIL import Image
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]  # service role key, not anon — needed to overwrite storage objects
BUCKET = "garment-images"

MAX_DIMENSION = 800
JPEG_QUALITY = 80

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def compress_image(image_bytes, max_dimension=MAX_DIMENSION, quality=JPEG_QUALITY):
    img = Image.open(io.BytesIO(image_bytes))
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")

    if max(img.width, img.height) > max_dimension:
        ratio = max_dimension / max(img.width, img.height)
        new_size = (int(img.width * ratio), int(img.height * ratio))
        img = img.resize(new_size, Image.LANCZOS)

    output = io.BytesIO()
    img.save(output, format="JPEG", quality=quality, optimize=True)
    return output.getvalue()


def get_all_garments_with_images():
    """Fetch all garments that have a Supabase-hosted image_url."""
    all_rows = []
    page_size = 1000
    offset = 0
    while True:
        res = (
            supabase.table("garments")
            .select("id, airtable_id, image_url")
            .like("image_url", f"%{SUPABASE_URL.replace('https://', '')}%")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = res.data or []
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return all_rows


def main():
    garments = get_all_garments_with_images()
    print(f"Found {len(garments)} garments with Supabase-hosted images.\n")

    total_before = 0
    total_after = 0
    success = 0
    skipped = 0
    failed = 0

    for i, g in enumerate(garments):
        airtable_id = g.get("airtable_id")
        image_url = g.get("image_url")
        if not airtable_id or not image_url:
            skipped += 1
            continue

        storage_path = f"{airtable_id}.jpg"

        try:
            # Download current image
            resp = requests.get(image_url, timeout=30)
            if resp.status_code != 200:
                print(f"  [{i+1}/{len(garments)}] SKIP {airtable_id} — fetch failed ({resp.status_code})")
                skipped += 1
                continue

            original_bytes = resp.content
            original_size = len(original_bytes)

            # Skip if already small (already compressed in a previous run)
            if original_size < 100_000:  # under 100KB, likely already compressed
                skipped += 1
                continue

            compressed_bytes = compress_image(original_bytes)
            compressed_size = len(compressed_bytes)

            # Re-upload to the SAME path — overwrite
            supabase.storage.from_(BUCKET).update(
                storage_path,
                compressed_bytes,
                {"content-type": "image/jpeg", "upsert": "true"},
            )

            total_before += original_size
            total_after += compressed_size
            success += 1

            if (i + 1) % 100 == 0:
                print(f"  [{i+1}/{len(garments)}] processed — running total: "
                      f"{total_before/1024/1024:.1f}MB -> {total_after/1024/1024:.1f}MB")

            time.sleep(0.05)  # gentle pacing to avoid rate limits

        except Exception as e:
            print(f"  [{i+1}/{len(garments)}] FAILED {airtable_id} — {e}")
            failed += 1
            continue

    print("\n--- Done ---")
    print(f"Success: {success} | Skipped: {skipped} | Failed: {failed}")
    if total_before > 0:
        print(f"Total before: {total_before/1024/1024:.1f}MB")
        print(f"Total after:  {total_after/1024/1024:.1f}MB")
        print(f"Reduction:    {(1 - total_after/total_before)*100:.1f}%")


if __name__ == "__main__":
    main()
