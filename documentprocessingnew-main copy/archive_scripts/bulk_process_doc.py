
import os
import django
import sys
import time

# Set up Django environment
sys.path.append('s:/dp004/docpro copy 2/docpro copy 2/backend')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from apps.documents.models import Document, Page
from apps.processing.services.ocr import OCRService
from apps.processing.tasks import extract_page_blocks_task
from apps.documents.tasks import split_document_task

def process_document_pages(doc_id, resplit=False):
    if resplit:
        print(f"Re-splitting document {doc_id}...")
        split_document_task(doc_id)
        
    doc = Document.objects.get(id=doc_id)
    pages = doc.pages.order_by('page_number')
    total = pages.count()
    
    print(f"Starting process for Document: {doc.name} (Ref: {doc.doc_ref})")
    print(f"Total pages: {total}")
    print("-" * 50)
    
    for i, page in enumerate(pages):
        start_time = time.time()
        print(f"[{i+1}/{total}] Processing Page {page.page_number} (ID: {page.id})...")
        
        try:
            # 1. Re-analyze layout and extract text (now saves to page internally)
            OCRService.process_page(page)
            
            # 2. Save blocks to database (use .apply to run synchronously in this script)
            extract_page_blocks_task.apply(args=(page.id,))
            
            elapsed = time.time() - start_time
            print(f"      Success! ({elapsed:.2f}s)")
        except Exception as e:
            print(f"      FAILED: {str(e)}")

if __name__ == "__main__":
    # Blue Star ID from doc_ref: 11blue-star-investment-sg-pte-ltd_202429-51e4f544
    # Finding by doc_ref
    target_ref = "11blue-star-investment-sg-pte-ltd_202429-51e4f544"
    try:
        doc = Document.objects.get(doc_ref=target_ref)
        print(f"Found document: {doc.id}")
        process_document_pages(doc.id, resplit=True)
    except Document.DoesNotExist:
        print(f"Document with ref {target_ref} not found.")
