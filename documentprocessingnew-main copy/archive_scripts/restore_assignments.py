
import os
import django
import sys

# Set up Django environment
sys.path.append('s:/dp004/docpro copy 2/docpro copy 2/backend')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from apps.documents.models import Document, Page
from apps.processing.models import PageAssignment
from common.enums import PageAssignmentStatus

target_ref = "11blue-star-investment-sg-pte-ltd_202429-51e4f544"
doc = Document.objects.get(doc_ref=target_ref)
pages = doc.pages.all()

print(f"Restoring assignments for Document: {doc.name}")

for page in pages:
    latest_a = PageAssignment.objects.filter(page=page).order_by('-assigned_at').first()
    if latest_a and latest_a.status == PageAssignmentStatus.REASSIGNED:
        print(f"  Restoring Page {page.page_number} Assignment {latest_a.id} (Resource: {latest_a.resource.user.username if latest_a.resource else 'None'})")
        latest_a.status = PageAssignmentStatus.IN_PROGRESS
        latest_a.save(update_fields=['status'])
    elif latest_a:
        print(f"  Page {page.page_number} already has status: {latest_a.status}")

print("Done.")
