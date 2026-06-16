def refresh_supabase_images(all_garments: list):
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not supabase_url or not supabase_key:
        print("  ⚠️  Supabase credentials not found, skipping image refresh")
        return

    print("\n" + "=" * 64)
    print("  Refreshing Supabase image URLs")
    print("=" * 64)

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    updated = skipped = errors = 0
    for record in all_garments:
        airtable_id = record["id"]
        at_fields = record.get("fields", {})

        img1 = at_fields.get("Image 1", [])
        if not img1:
            skipped += 1
            continue

        def extract_url(img):
            return img.get("url") if isinstance(img, dict) else img

        image_url = extract_url(img1[0])
        all_images = [
            extract_url(img)
            for field in ["Image 1", "Image 2", "Image 3", "Image 4", "Image 5", "Image 6", "Image 7"]
            for img in at_fields.get(field, [])
            if img
        ]
        all_images = [url for url in all_images if url]

        res = requests.patch(
            f"{supabase_url}/rest/v1/garments?airtable_id=eq.{airtable_id}",
            headers=headers,
            json={"image_url": image_url, "images": all_images},
        )
        if res.status_code in (200, 204):
            updated += 1
        else:
            errors += 1
        time.sleep(0.02)

    print(f"  Images refreshed: {updated} updated | {skipped} skipped (no image) | {errors} errors")
