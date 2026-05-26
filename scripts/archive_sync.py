"""
Filted — Archive Sync
Syncs Collections + Garments from Airtable to Webflow CMS Campaign pages.
Generates SEO-ready HTML garment list in the Editorial Content rich text field.
Saves Webflow Item ID back to Airtable for update tracking.

Required GitHub Secrets:
  AIRTABLE_API_KEY   — Airtable personal access token
  AIRTABLE_BASE_ID   — Airtable base ID (appUk1ThnHvWwFDHG)
  WEBFLOW_API_TOKEN  — Webflow API v2 token
"""

import os
import re
import time
import requests
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN          = os.environ['AIRTABLE_API_KEY']
AIRTABLE_BASE_ID        = os.environ['AIRTABLE_BASE_ID']
WEBFLOW_TOKEN           = os.environ['WEBFLOW_API_TOKEN']

AIRTABLE_BASE           = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}'
WEBFLOW_BASE            = 'https://api.webflow.com/v2'

WEBFLOW_CAMPAIGNS_ID    = '68944133543f0cbb26b4aeb9'
WEBFLOW_DESIGNERS_ID    = '687a1d0eeb0f06f63aef724f'

HEADERS_AT  = {'Authorization': f'Bearer {AIRTABLE_TOKEN}'}
HEADERS_WF  = {
    'Authorization': f'Bearer {WEBFLOW_TOKEN}',
    'Content-Type':  'application/json',
    'accept':        'application/json',
}

# Add designers here to include in sync
TARGET_DESIGNERS = ['Aje', 'Alemais']

WF_DELAY = 0.3  # seconds between Webflow API calls to avoid rate limits
AT_DELAY = 0.1  # seconds between Airtable API calls

# ── Airtable helpers ──────────────────────────────────────────────────────────
def at_get_all(table, filter_formula=None, fields=None, sort=None):
    records = []
    offset  = None
    url     = f'{AIRTABLE_BASE}/{requests.utils.quote(table)}'

    while True:
        params = {}
        if filter_formula:
            params['filterByFormula'] = filter_formula
        if offset:
            params['offset'] = offset
        if fields:
            for i, f in enumerate(fields):
                params[f'fields[{i}]'] = f
        if sort:
            for i, s in enumerate(sort):
                params[f'sort[{i}][field]']     = s['field']
                params[f'sort[{i}][direction]'] = s.get('direction', 'asc')

        res = requests.get(url, headers=HEADERS_AT, params=params)
        if not res.ok:
            raise Exception(f'Airtable error {res.status_code}: {res.text}')

        data = res.json()
        records.extend(data.get('records', []))
        offset = data.get('offset')
        if not offset:
            break
        time.sleep(AT_DELAY)

    return records


def at_update(table, record_id, fields):
    res = requests.patch(
        f'{AIRTABLE_BASE}/{requests.utils.quote(table)}/{record_id}',
        headers={**HEADERS_AT, 'Content-Type': 'application/json'},
        json={'fields': fields},
    )
    if not res.ok:
        raise Exception(f'Airtable update error {res.status_code}: {res.text}')
    return res.json()


# ── Webflow helpers ───────────────────────────────────────────────────────────
def wf_get_all_items(collection_id):
    items  = []
    offset = 0
    while True:
        time.sleep(WF_DELAY)
        res = requests.get(
            f'{WEBFLOW_BASE}/collections/{collection_id}/items',
            headers=HEADERS_WF,
            params={'offset': offset, 'limit': 100},
        )
        if not res.ok:
            raise Exception(f'Webflow fetch error {res.status_code}: {res.text}')
        data  = res.json()
        batch = data.get('items', [])
        items.extend(batch)
        total = data.get('pagination', {}).get('total', 0)
        offset += len(batch)
        if offset >= total or not batch:
            break
    return items


def wf_create_item(collection_id, field_data):
    time.sleep(WF_DELAY)
    res = requests.post(
        f'{WEBFLOW_BASE}/collections/{collection_id}/items/live',
        headers=HEADERS_WF,
        json={'fieldData': field_data},
    )
    if not res.ok:
        raise Exception(f'Webflow create error {res.status_code}: {res.text}')
    return res.json()


def wf_update_item(collection_id, item_id, field_data):
    time.sleep(WF_DELAY)
    res = requests.patch(
        f'{WEBFLOW_BASE}/collections/{collection_id}/items/{item_id}/live',
        headers=HEADERS_WF,
        json={'fieldData': field_data},
    )
    if not res.ok:
        raise Exception(f'Webflow update error {res.status_code}: {res.text}')
    return res.json()


# ── Content generation ────────────────────────────────────────────────────────
def slugify(text):
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_]+', '-', text)
    text = re.sub(r'-+', '-', text)
    return text.strip('-')


def generate_garment_html(garments, designer_name, collection_name):
    """Generate SEO-readable HTML table of garments for Webflow rich text field."""
    if not garments:
        return ''

    rows = '\n'.join(
        f'<tr><td>{g["fields"].get("Product Code","")}</td>'
        f'<td>{g["fields"].get("Garment Name","")}</td>'
        f'<td>{g["fields"].get("Product Colour","")}</td>'
        f'<td>{g["fields"].get("Category","")}</td></tr>'
        for g in garments
    )

    count = len(garments)

    return (
        f'<h2>{designer_name} {collection_name} — Full Garment List</h2>'
        f'<p>Complete garment reference for the {designer_name} {collection_name} collection. '
        f'{count} pieces with product codes, colourways and categories.</p>'
        f'<table>'
        f'<thead><tr><th>Code</th><th>Garment</th><th>Colourway</th><th>Category</th></tr></thead>'
        f'<tbody>{rows}</tbody>'
        f'</table>'
    )


def generate_meta_description(garments, designer_name, collection_name):
    """Generate SEO meta description."""
    count  = len(garments)
    sample = [g['fields'].get('Garment Name', '') for g in garments[:4] if g['fields'].get('Garment Name')]
    names  = ', '.join(sample)
    if count > 4:
        names += ' and more'
    return (
        f'Complete garment reference for {designer_name} {collection_name}. '
        f'{count} pieces with product codes and colourways — {names}.'
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print('=' * 60)
    print(f'Filted Archive Sync — {datetime.now().strftime("%d %b %Y, %I:%M %p")}')
    print('=' * 60)

    # ── 1. Load Webflow designer IDs ──────────────────────────────────────
    print('\nLoading Webflow designers...')
    wf_designers  = wf_get_all_items(WEBFLOW_DESIGNERS_ID)
    designer_wf_map = {}
    for d in wf_designers:
        name = d.get('fieldData', {}).get('name', '')
        if name:
            designer_wf_map[name.lower()] = d['id']
    print(f'  {len(designer_wf_map)} designers found')

    # ── 2. Load Airtable collections ──────────────────────────────────────
    print('\nLoading Airtable collections...')
    designer_filter = ', '.join(f'{{Designer Name}}="{d}"' for d in TARGET_DESIGNERS)
    collections = at_get_all(
        'Collections',
        filter_formula=f'AND({{Published}}=1, OR({designer_filter}))',
        fields=[
            'Collection Name', 'Designer Name', 'Season Code',
            'Release Date', 'Hero Image', 'Slug', 'Webflow Item ID',
        ],
        sort=[{'field': 'Designer Name'}, {'field': 'Collection Name'}],
    )
    print(f'  {len(collections)} published collections found')

    # ── 3. Sync each collection ───────────────────────────────────────────
    created = updated = skipped = errors = 0
    publish_queue = []

    for col in collections:
        f          = col['fields']
        col_id     = col['id']
        col_name   = f.get('Collection Name', '').strip()
        designer   = f.get('Designer Name', '').strip()
        wf_item_id = f.get('Webflow Item ID', '').strip()

        if not col_name or not designer:
            skipped += 1
            continue

        print(f'\n  {designer} — {col_name}')

        # Fetch garments with images for this collection
        garments = at_get_all(
            'All Garments',
            filter_formula=f'AND({{Collection}}="{col_name}", {{Image 1}}!="")',
            fields=['Garment Name', 'Product Code', 'Product Colour', 'Category'],
            sort=[{'field': 'Category'}, {'field': 'Garment Name'}],
        )

        if not garments:
            print(f'    Skipped — no garments with images')
            skipped += 1
            continue

        print(f'    {len(garments)} garments with images')

        # Build Webflow field data
        slug       = slugify(f.get('Slug') or col_name)
        hero_imgs  = f.get('Hero Image', [])
        hero_url   = hero_imgs[0].get('url') if hero_imgs else None
        wf_des_id  = designer_wf_map.get(designer.lower())

        field_data = {
            'name':                   col_name,
            'slug':                   slug,
            'season-code':            f.get('Season Code', ''),
            'airtable-collection-id': col_id,
            'editorial-content':      generate_garment_html(garments, designer, col_name),
            'meta-description':       generate_meta_description(garments, designer, col_name),
        }

        if wf_des_id:
            field_data['designer'] = wf_des_id
        if hero_url:
            field_data['hero-image'] = {'url': hero_url, 'alt': f'{designer} {col_name}'}
        if f.get('Release Date'):
            field_data['released'] = f['Release Date']

        try:
            if wf_item_id:
                wf_update_item(WEBFLOW_CAMPAIGNS_ID, wf_item_id, field_data)
                print(f'    ✓ Updated')
                updated += 1

            else:
                result    = wf_create_item(WEBFLOW_CAMPAIGNS_ID, field_data)
                new_id    = result['id']
                at_update('Collections', col_id, {'Webflow Item ID': new_id})
                print(f'    ✓ Created (ID: {new_id})')
                created += 1

        except Exception as e:
            print(f'    ✗ Error: {e}')
            errors += 1

    # ── 4. Summary ────────────────────────────────────────────────────────
    print('\n' + '=' * 60)
    print(f'Collections: {created} created | {updated} updated | {skipped} skipped | {errors} errors')
    print('=' * 60)


if __name__ == '__main__':
    main()
