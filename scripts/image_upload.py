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

# Fetch all already-permanent airtable_ids in one batch
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

print("Fetching all garments from Airtable...")
api = Api(AIRTABLE_API_KEY)
table = api.table(AIRTABLE_BASE_ID, GARMENTS_TABLE_ID)
all_garments = table.all(fields=["Image 1"])
print(f"{len(all_garments)} garments fetched")

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

    try:
        img_res = requests.get(image_url, timeout=15)
        if img_res.status_code != 200:
            errors += 1
            continue
    except Exception:
        errors += 1
        continue

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
