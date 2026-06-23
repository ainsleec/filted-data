import io
import os
import time
import requests
from PIL import Image
from pyairtable import Api

AIRTABLE_API_KEY = os.environ["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID = "appUk1ThnHvWwFDHG"
GARMENTS_TABLE_ID = "tblmqjU4WqgCzP7cR"
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
BUCKET = "garment-images"

MAX_DIMENSION = 800
JPEG_QUALITY = 80

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

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

# Fetch already-uploaded garments from Supabase
print("Fetching already-uploaded garments from Supabase...")
already_done = set()
offset = 0
while True:
    res = requests.get(
        f"{SUPABASE_URL}/rest/v1/garments?select=airtable_id,image_url&limit=1000&offset={offset}",
        headers={**headers, "Content-Type": "application/json"},
    )
    rows = res.json()
    for r in rows:
        if (r.get("image_url") or "").startswith(f"{SUPABASE_URL}/storage"):
            already_done.add(r["airtable_id"])
    if len(rows) < 1000:
        break
    offset += 1000
print(f"  {len(already_done)} already uploaded — will skip these")

# Fetch all garments from Airtable
print("Fetching all garments from Airtable...")
api = Api(AIRTABLE_API_KEY)
table = api.table(AIRTABLE_BASE_ID, GARMENTS_TABLE_ID)
all_garments = table.all(fields=["Image 1"])
print(f"  {len(all_garments)} garments fetched")

uploaded = skipped = errors = 0

for record in all_garments:
    airtable_id = record["id"]

    if airtable_id in already_done:
        skipped += 1
        continue

    img1 = record["fields"].get("Image 1", [])
    if not img1:
        skipped += 1
        continue

    image_url = img1[0].get("url") if isinstance(img1[0], dict) else img1[0]
    if not image_url:
        skipped += 1
        continue

    path = f"{airtable_id}.jpg"

    # Download from Airtable
    try:
        img_res = requests.get(image_url, timeout=15)
        if img_res.status_code != 200:
            errors += 1
            continue
    except Exception:
        errors += 1
        continue

    # Compress before uploading
    try:
        compressed = compress_image(img_res.content)
    except Exception:
        # If compression fails, fall back to original
        compressed = img_res.content

    # Upload to Supabase Storage (no upsert — bucket is clean)
    upload = requests.post(
        f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{path}",
        headers={**headers, "Content-Type": "image/jpeg"},
        data=compressed,
    )

    if upload.status_code in (200, 201):
        permanent_url = f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{path}"
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/garments?airtable_id=eq.{airtable_id}",
            headers={**headers, "Content-Type": "application/json", "Prefer": "return=minimal"},
            json={"image_url": permanent_url},
        )
        uploaded += 1
        if uploaded % 100 == 0:
            print(f"  {uploaded} uploaded, {skipped} skipped, {errors} errors...", flush=True)
    else:
        print(f"  Upload failed {airtable_id}: {upload.status_code} {upload.text}", flush=True)
        errors += 1

    time.sleep(0.05)

print(f"\nDone — uploaded: {uploaded}, skipped: {skipped}, errors: {errors}")
