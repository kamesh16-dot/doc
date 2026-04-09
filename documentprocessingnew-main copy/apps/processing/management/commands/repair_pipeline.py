from django.core.management.base import BaseCommand
from apps.documents.models import Document
from apps.processing.utils.page_integrity import PageIntegrityChecker
import json

class Command(BaseCommand):
    help = 'Checks and repairs pipeline integrity for documents.'

    def add_arguments(self, parser):
        parser.add_argument('--doc_id', type=str, help='Specific document ID to repair')
        parser.add_argument('--all', action='store_true', help='Repair all documents with issues')
        parser.add_argument('--check-only', action='store_true', help='Only check, don\'t repair')

    def handle(self, *args, **options):
        doc_id = options.get('doc_id')
        repair_all = options.get('all')
        check_only = options.get('check_only')

        if doc_id:
            documents = Document.objects.filter(id=doc_id)
        elif repair_all:
            documents = Document.objects.all()
        else:
            self.stdout.write(self.style.ERROR('Must specify --doc_id or --all'))
            return

        for doc in documents:
            self.stdout.write(f"Checking Document {doc.id} ({doc.name})...")
            
            if check_only:
                report = PageIntegrityChecker.run_full_check(doc.id)
                self.stdout.write(json.dumps(report, indent=2))
            else:
                report = PageIntegrityChecker.auto_repair(doc.id)
                if report['is_healthy']:
                    self.stdout.write(self.style.SUCCESS(f"Document {doc.id} is healthy!"))
                else:
                    self.stdout.write(self.style.WARNING(f"Repaired Document {doc.id}, but some issues remain: {report}"))
