#!/usr/bin/env python3
"""
Scrape boutique swimwear listings from eBay and store in Supabase.
Runs daily via GitHub Actions.
Uses existing GitHub secrets: EBAY_APP_ID, SUPABASE_URL, SUPABASE_ANON_KEY
"""

import os
import json
from datetime import datetime, timedelta
import requests
from supabase import create_client, Client

# ── Config ──────────────────────────────────────────────────────────────────
EBAY_APP_ID = os.getenv('EBAY_APP_ID')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_ANON_KEY')  # Using ANON_KEY from secrets

BRANDS = [
    'Ochre Lane',
    'Fella Swim',
    'Zulu & Zephyr',
    'Elce Swim',
    'La Lu Label',
    'TJ Swim',
    'Palm Swimwear',
    'Matteau',
    'Une Piece',
    'Bondi Born',
    'Form and Fold',
    'Cleonie',
    'Faithfull the Brand',
    'Monte & Lou',
    'Peony',
    'Bay of Fire',
    'Zimmermann',
]

EBAY_FINDING_URL = 'https://svcs.ebay.com/services/search/FindingService/v1'
LISTINGS_PER_BRAND = 100
CONDITION_IDS = ['3000', '4000', '5000']  # New, Like New, Excellent
CATEGORY_ID = '16641'  # Women's Clothing

# ── Supabase client ─────────────────────────────────────────────────────────
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── eBay query ──────────────────────────────────────────────────────────────
def query_ebay(brand: str) -> list:
    """Query eBay Finding API for a specific brand."""
    params = {
        'OPERATION-NAME': 'findItemsAdvanced',
        'SERVICE-VERSION': '1.0.0',
        'SECURITY-APPNAME': EBAY_APP_ID,
        'RESPONSE-DATA-FORMAT': 'JSON',
        'REST-PAYLOAD': True,
        'keywords': f'{brand} swimwear -mens -boys -vintage -retro',
        'categoryId': CATEGORY_ID,
        'itemFilter(0).name': 'Condition',
        'itemFilter(0).value(0)': '3000',  # New
        'itemFilter(0).value(1)': '4000',  # Like New
        'itemFilter(0).value(2)': '5000',  # Excellent
        'itemFilter(1).name': 'ListingType',
        'itemFilter(1).value': 'FixedPrice',
        'itemFilter(2).name': 'Currency',
        'itemFilter(2).value': 'AUD',
        'itemFilter(3).name': 'Location',
        'itemFilter(3).value': 'AU',
        'paginationInput.entriesPerPage': str(LISTINGS_PER_BRAND),
        'sortOrder': 'EndTimeSoonest',  # Newest first
        'outputSelector': 'SellerInfo',
        'outputSelector': 'StoreInfo',
    }
    
    try:
        resp = requests.get(EBAY_FINDING_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        if 'findItemsAdvancedResponse' not in data:
            print(f'  ⚠️  No response for {brand}')
            return []
        
        items = data['findItemsAdvancedResponse'][0].get('searchResult', [{}])[0].get('item', [])
        print(f'  ✓ {brand}: {len(items)} listings')
        return items
    
    except Exception as e:
        print(f'  ✗ {brand}: {e}')
        return []

# ── Parse eBay item ────────────────────────────────────────────────────────
def parse_ebay_item(item: dict, brand: str) -> dict:
    """Extract relevant fields from eBay item."""
    try:
        return {
            'brand': brand,
            'ebay_item_id': item['itemId'][0],
            'title': item.get('title', [''])[0],
            'price': float(item.get('sellingStatus', [{}])[0].get('currentPrice', [{'__value__': 0}])[0].get('__value__', 0)),
            'currency': item.get('sellingStatus', [{}])[0].get('currentPrice', [{'currencyId': 'AUD'}])[0].get('currencyId', 'AUD'),
            'condition': item.get('condition', [{}])[0].get('conditionDisplayName', ['Unknown'])[0] if item.get('condition') else 'Unknown',
            'image_url': item.get('galleryURL', [''])[0],
            'ebay_url': item.get('viewItemURL', [''])[0],
            'seller_name': item.get('sellerInfo', [{}])[0].get('sellerUserName', ['Unknown'])[0] if item.get('sellerInfo') else 'Unknown',
            'seller_rating_percent': float(item.get('sellerInfo', [{}])[0].get('positiveFeedbackPercent', ['0'])[0]) if item.get('sellerInfo') else 0,
            'listed_date': datetime.fromisoformat(item.get('listingInfo', [{}])[0].get('startTime', [''])[0].replace('Z', '+00:00')) if item.get('listingInfo', [{}])[0].get('startTime') else datetime.utcnow(),
        }
    except Exception as e:
        print(f'    Error parsing item: {e}')
        return None

# ── Upsert to Supabase ──────────────────────────────────────────────────────
def upsert_listings(listings: list) -> tuple:
    """Upsert listings to Supabase."""
    if not listings:
        return 0, 0
    
    try:
        result = sb.table('ebay_swimwear_listings').upsert(
            listings,
            on_conflict='ebay_item_id'
        ).execute()
        return len(listings), 0
    except Exception as e:
        print(f'  ✗ Upsert error: {e}')
        return 0, len(listings)

# ── Clean old listings ──────────────────────────────────────────────────────
def clean_old_listings():
    """Delete listings older than 6 months."""
    try:
        cutoff = (datetime.utcnow() - timedelta(days=180)).isoformat()
        sb.table('ebay_swimwear_listings').delete().lt('listed_date', cutoff).execute()
        print(f'✓ Cleaned listings before {cutoff}')
    except Exception as e:
        print(f'✗ Clean error: {e}')

# ── Main ────────────────────────────────────────────────────────────────────
def main():
    print(f'\n🏖️  eBay Swimwear Scraper — {datetime.utcnow().isoformat()}\n')
    
    total_scraped = 0
    total_upserted = 0
    total_failed = 0
    
    for brand in BRANDS:
        items = query_ebay(brand)
        listings = [parse_ebay_item(item, brand) for item in items]
        listings = [l for l in listings if l]  # Filter out Nones
        
        if listings:
            upserted, failed = upsert_listings(listings)
            total_upserted += upserted
            total_failed += failed
            total_scraped += len(items)
    
    print(f'\n📊 Summary')
    print(f'  Scraped: {total_scraped} listings')
    print(f'  Upserted: {total_upserted}')
    print(f'  Failed: {total_failed}')
    
    clean_old_listings()
    print(f'\n✓ Done\n')

if __name__ == '__main__':
    main()
