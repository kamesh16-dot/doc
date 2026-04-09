
import os
import django
import sys
from django.core.files.base import ContentFile

# Set up Django environment
sys.path.append('s:/dp004/docpro copy 2/docpro copy 2/backend')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from apps.documents.models import Document, Page

# Get a test page
page = Page.objects.first()
if page:
    doc_id_short = str(page.document.id)[:8]
    test_filename = f"{doc_id_short}/test_collision_fix.pdf"
    content = ContentFile(b"%PDF-1.4 test", name="test.pdf")
    
    print(f"Original content_file name: {page.content_file.name}")
    print(f"Attempting to save with name: {test_filename}")
    
    # We won't actually save to DB to avoid mess, just check what path it WOULD generate
    path = page.content_file.storage.get_available_name(os.path.join(page.content_file.field.upload_to, test_filename))
    print(f"Resulting Storage Path: {path}")
else:
    print("No pages found for test")
