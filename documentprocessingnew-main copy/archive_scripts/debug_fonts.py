import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from apps.processing.services.ocr import OCRService
from apps.processing.tasks import extract_page_blocks_task
from apps.documents.models import Page, Block
import json

PAGE_ID = 2405

try:
    print(f"Step 1: Re-running OCR recognition for Page ID {PAGE_ID}...")
    page = Page.objects.get(id=PAGE_ID)
    
    # Re-run OCR to regenerate layout_data with font metadata
    OCRService.process_page(page)
    
    # Re-fetch page to get updated layout_data
    page.refresh_from_db()
    ld = page.layout_data
    print(f"  -> Layout data keys: {list(ld.keys()) if ld else 'None'}")
    if ld and ld.get('blocks'):
        b0 = ld['blocks'][0]
        print(f"  -> First block keys: {list(b0.keys())}")
        print(f"  -> First block font data: font_family={b0.get('font_family')}, font_size={b0.get('font_size')}")
    
    print(f"\nStep 2: Re-running block extraction for Page ID {PAGE_ID}...")
    extract_page_blocks_task(PAGE_ID)
    
    print(f"\nStep 3: Checking stored block data...")
    blocks = page.blocks.order_by('y')[:5]
    for b in blocks:
        print(json.dumps({'text': b.current_text[:30], 'font': b.font_name, 'size': b.font_size, 'weight': b.font_weight}))
    
    print("SUCCESS")
except Exception as e:
    import traceback
    traceback.print_exc()
