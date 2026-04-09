
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
page = doc.pages.get(page_number=6)

print(f"Checking Page {page.page_number} (ID: {page.id})")
tables = page.tables.all()
print(f"Found {tables.count()} tables.")

for t in tables:
    print(f"Table {t.id} (Ref: {t.table_ref})")
    print(f"  table_json Type: {type(t.table_json)}")
    print(f"  table_json: {json.dumps(t.table_json)[:150]}...")
    
    # Verify structure
    if isinstance(t.table_json, list):
        for i, row in enumerate(t.table_json):
            if not isinstance(row, list):
                print(f"  ERROR: Row {i} is not a list! Type: {type(row)} Value: {row}")
            else:
                pass # print(f"  Row {i} is OK")
    else:
        print(f"  ERROR: table_json is not a list!")
