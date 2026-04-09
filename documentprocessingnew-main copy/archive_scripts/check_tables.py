
import os
import django
import sys
import json

# Set up Django environment
sys.path.append('s:/dp004/docpro copy 2/docpro copy 2/backend')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from apps.documents.models import Document, Page, PageTable

target_ref = "11blue-star-investment-sg-pte-ltd_202429-51e4f544"
doc = Document.objects.get(doc_ref=target_ref)
page = doc.pages.get(page_number=2) # The page with the TOC

print(f"Checking Page {page.page_number} (ID: {page.id})")
tables = page.tables.all()
print(f"Found {tables.count()} tables.")

for t in tables:
    print(f"Table {t.id} (Ref: {t.table_ref})")
    print(f"  table_json: {json.dumps(t.table_json)[:100]}...")
    # Check for null cells
    has_null = False
    for row in (t.table_json or []):
        for cell in (row or []):
            if cell is None:
                has_null = True
                break
    print(f"  Has NULL cells: {has_null}")
