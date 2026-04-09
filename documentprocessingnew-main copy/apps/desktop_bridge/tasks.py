from __future__ import annotations

import logging
from celery import shared_task
from django.utils import timezone
from django.db import transaction
from .models import AssignmentBundle, MergeManifest
from .services import ZeroLossMergeEngine

logger = logging.getLogger(__name__)


@shared_task(name="apps.desktop_bridge.tasks.expire_stale_leases")
def expire_stale_leases():
    """
    Periodic task to revoke abandoned leases.
    Makes bundles READY for other agents to pick up.
    """
    now = timezone.now()
    # Leases that timed out OR devices that missed heartbeats for > 5 mins
    stale_leases = AssignmentBundle.objects.filter(
        status=AssignmentBundle.Status.LEASED,
        lease_expires_at__lt=now
    )

    count = stale_leases.count()
    if count > 0:
        stale_leases.update(
            status=AssignmentBundle.Status.READY,
            leased_to=None,
            lease_expires_at=None,
        )
        logger.warning(f"Revoked {count} expired leases. Bundles returned to READY.")

    return f"Revoked {count} leases"


@shared_task(name="apps.desktop_bridge.tasks.task_deterministic_merge")
def task_deterministic_merge(document_id: str):
    """
    Async background task for lossless reconstruction.
    """
    from apps.documents.models import Document
    from common.enums import PipelineStatus, DocumentStatus

    try:
        document = Document.objects.get(id=document_id)
        
        # Atomically check if someone else is already merging
        with transaction.atomic():
            document = Document.objects.select_for_update().get(id=document_id)
            if document.merge_in_progress:
                logger.info(f"Merge already in progress for {document_id}. Skipping.")
                return
            document.merge_in_progress = True
            document.save(update_fields=["merge_in_progress"])

        try:
            logger.info(f"Starting deterministic merge for document {document_id}...")
            
            # ── Legacy vs Strict Flow Branch ──────────────────
            if document.job_id:
                logger.info(f"Using StrictBundleMergeEngine for Job-based doc {document_id}")
                from .services import StrictBundleMergeEngine
                engine = StrictBundleMergeEngine(str(document_id))
                success, message = engine.execute()
            else:
                logger.info(f"Using Legacy BundleMergeEngine for doc {document_id}")
                from .services import BundleMergeEngine
                engine = BundleMergeEngine(document)
                success, message = engine.execute()

            if success:
                logger.info(f"Document {document_id} successfully merged: {message}")
            else:
                logger.warning(f"Merge outcome for {document_id}: {message}")

        finally:
            document.merge_in_progress = False
            # Refresh from DB before final save to avoid overwriting engine's status updates
            document.save(update_fields=["merge_in_progress"])

    except Document.DoesNotExist:
        logger.error(f"Merge task: Document {document_id} not found.")
    except Exception as e:
        logger.error(f"Async merge error for {document_id}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
