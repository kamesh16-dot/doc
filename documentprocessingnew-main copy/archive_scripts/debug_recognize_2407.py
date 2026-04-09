import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from apps.processing.tasks import process_page_ocr_task
from apps.documents.models import Page

PAGE_ID = 2407

try:
    print(f"Running process_page_ocr_task for Page ID {PAGE_ID}...")
    page = Page.objects.get(id=PAGE_ID)
    print(f"Page Number: {page.page_number}, Document: {page.document_id}")
    
    # Run the task directly (synchronously)
    process_page_ocr_task(PAGE_ID)
    print("SUCCESS")
except Exception as e:
    import traceback
    traceback.print_exc()
