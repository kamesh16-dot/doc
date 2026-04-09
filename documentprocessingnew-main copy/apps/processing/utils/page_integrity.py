import logging
import os
from pathlib import Path
from django.db import transaction
from django.utils import timezone
from datetime import timedelta

from apps.documents.models import Document, Page
from apps.processing.models import DocumentQueue
from common.enums import PageStatus, ValidationStatus, QueueStatus

logger = logging.getLogger(__name__)

class PageIntegrityChecker:
    """
    Utility to verify document page integrity and 
    automatically repair inconsistencies.
    """

    @staticmethod
    def run_full_check(document_id):
        """
        Runs a suite of integrity checks for a document.
        """
        try:
            doc = Document.objects.get(id=document_id)
            total = doc.total_pages or 0
            
            # Check Queue
            in_queue = DocumentQueue.objects.filter(document=doc).exists()
            pending_pages_count = Page.objects.filter(document=doc, status=PageStatus.PENDING).count()
            queue_missing = pending_pages_count > 0 and not in_queue
            
            # 1. Check for missing pages in DB
            db_pages = Page.objects.filter(document=doc).values_list('page_number', flat=True)
            missing_in_db = [i for i in range(1, total + 1) if i not in db_pages]
            
            # 2. Check for missing files on disk
            missing_files = []
            for page in Page.objects.filter(document=doc):
                if not page.content_file or not os.path.exists(page.content_file.path):
                    missing_files.append(page.page_number)
            
            # 3. Check for NULL weights
            null_weights = Page.objects.filter(
                document=doc, 
                complexity_weight__isnull=True
            ).values_list('page_number', flat=True)
            
            # 4. Check for stuck validations
            # (In progress for more than 10 minutes)
            threshold = timezone.now() - timedelta(minutes=10)
            stuck_validations = Page.objects.filter(
                document=doc,
                status=PageStatus.PENDING,
                validation_status=ValidationStatus.PENDING_VALIDATION,
                updated_at__lt=threshold
            ).values_list('page_number', flat=True)

            return {
                'document_id': document_id,
                'total_expected': total,
                'missing_in_db': list(missing_in_db),
                'missing_files': list(missing_files),
                'null_weights': list(null_weights),
                'stuck_validations': list(stuck_validations),
                'queue_missing': queue_missing,
                'is_healthy': not (missing_in_db or missing_files or null_weights or stuck_validations or queue_missing)
            }
        except Document.DoesNotExist:
            return None

    @staticmethod
    @transaction.atomic
    def auto_repair(document_id):
        """
        Attempts to fix inconsistencies found by checks.
        """
        doc = Document.objects.select_for_update().get(id=document_id)
        report = PageIntegrityChecker.run_full_check(document_id)
        
        if not report or report['is_healthy']:
            return report

        # Fix 1: Repair NULL weights
        if report['null_weights']:
            Page.objects.filter(
                document=doc, 
                page_number__in=report['null_weights']
            ).update(complexity_weight=1)
            logger.info(f"Repaired weights for {len(report['null_weights'])} pages in Document {document_id}")

        # Fix 2: Reset stuck validations
        if report['stuck_validations']:
            from apps.processing.tasks import validate_single_page
            for page_num in report['stuck_validations']:
                page = Page.objects.get(document=doc, page_number=page_num)
                validate_single_page.delay(page.id)
            logger.info(f"Restarted validation for {len(report['stuck_validations'])} pages in Document {document_id}")

        # Fix 3: Handle missing pages (Requires re-splitting)
        if report['missing_in_db'] or report['missing_files']:
            # We trigger a partial resplit task
            from apps.processing.tasks import resplit_missing_pages
            resplit_missing_pages.delay(document_id)
            logger.warning(f"Triggered re-split for missing pages in Document {document_id}")

        # Fix 4: Re-queue if missing
        if report['queue_missing']:
            DocumentQueue.objects.get_or_create(
                document=doc,
                defaults={'status': QueueStatus.WAITING}
            )
            logger.info(f"Re-queued Document {document_id}")

        return PageIntegrityChecker.run_full_check(document_id)
