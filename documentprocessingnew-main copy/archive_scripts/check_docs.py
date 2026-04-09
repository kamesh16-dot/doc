
import os
import django
import sys

# Set up Django environment
sys.path.append('s:/dp004/docpro copy 2/docpro copy 2/backend')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from apps.documents.models import Document, Page

# Get last 2 documents
docs = Document.objects.all().order_by('-created_at')[:2]

print("-" * 50)
for doc in docs:
    print(f"Document: {doc.name} (Ref: {doc.doc_ref}) (ID: {doc.id})")
    pages = doc.pages.all().order_by('page_number')
    print(f"  Total Pages in DB: {pages.count()}")
    for page in pages:
        print(f"    Page {page.page_number} (ID: {page.id}):")
        print(f"      Content File: {page.content_file.name if page.content_file else 'NONE'}")
print("-" * 50)
