import os
import time
import requests
from pyairtable import Api

AIRTABLE_API_KEY = os.environ["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID = "appUk1ThnHvWwFDHG"
GARMENTS_TABLE_ID = "tblmqjU4WqgCzP7cR"
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
BUCKET = "garment-images"

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

print("Fetching all garments from Airtable...")
api = Api(AIRTABLE_API_KEY)
table = api.table(AIRTABLE_BASE_ID, GARMENTS_TABLE_ID)
all_garments = table.all(fields=["Image 1"])
print(f"{len(all_garments)} garments fetched")

uploaded = skipped = errors = 0

for record in all_garments:
    airtable_id = record["id"]
    img1 = record["fields"].get("Image 1", [])
    if not img1:
        skipped += 1
        continue

    image_url = img1[0].get("url") if isinstance(img1[0], dict) else img1[0]
    if not image_url:
        skipped += 1
        continue

    path = f"{airtable_id}.jpg"

    # Skip if already permanent
    check = requests.get(
        f"{SUPABASE_URL}/rest/v1/garments?airtable_id=eq.{airtable_id}&select=image_url",
        headers={**headers, "Content-Type": "application/json"},
    )
    if check.status_code == 200:
        rows = check.json()
        if rows and rows[0].get("image_url", "").startswith(f"{SUPABASE_URL}/storage"):
            skipped += 1
            continue

    # Download from Airtable (fresh during this run)
    try:
        img_res = requests.get(image_url, timeout=15)
        if img_res.status_code != 200:
            errors += 1
            continue
    except Exception as e:
        errors += 1
        continue

    # Upload to Supabase Storage
    upload = requests.post(
        f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{path}",
        headers={**headers, "Content-Type": "image/jpeg", "x-upsert": "true"},
        data=img_res.content,
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
            print(f"  {uploaded} uploaded, {errors} errors so far...")
    else:
        errors += 1

    time.sleep(0.05)

print(f"Done — uploaded: {uploaded}, skipped: {skipped}, errors: {errors}")
