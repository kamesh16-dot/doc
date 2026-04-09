
import os
import django
import sys

# Set up Django environment
sys.path.append('s:/dp004/docpro copy 2/docpro copy 2/backend')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from apps.documents.models import Document, Page
from apps.processing.models import PageAssignment

target_ref = "11blue-star-investment-sg-pte-ltd_202429-51e4f544"
doc = Document.objects.get(doc_ref=target_ref)
page = doc.pages.get(page_number=6)

print(f"Checking Page {page.page_number} (ID: {page.id})")
assignments = PageAssignment.objects.filter(page=page)
print(f"Found {assignments.count()} assignments.")

for a in assignments:
    print(f"Assignment {a.id}:")
    print(f"  Resource: {a.resource.user.username if a.resource else 'None'}")
    print(f"  Status: {a.status}")
