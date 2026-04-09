import os
import django
import sys

# Setup django
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from apps.documents.models import Document, Page, PageTable, Block
import json

docs = Document.objects.filter(name__icontains='haystack')
for doc in docs:
    print(f"Doc: {doc.name}")
    pages = doc.pages.order_by('page_number')
    if len(pages) > 2:
        p3 = pages[2]
        print(f"  Page 3 scanned: {p3.is_scanned}, provider: {p3.ocr_provider}")
        tables = PageTable.objects.filter(page=p3)
        print(f"  PageTables in DB: {tables.count()}")
        for t in tables:
            print(f"    Table ID: {t.id}, Rows: {t.row_count}, Cols: {t.col_count}")
        blocks = Block.objects.filter(page=p3)
        print(f"  Blocks in DB: {blocks.count()}")
    print("---")
