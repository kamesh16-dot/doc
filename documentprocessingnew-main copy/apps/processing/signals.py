from django.db.models.signals import pre_save, post_save
from django.dispatch import receiver
from django.utils import timezone
from django.db.models import F

from apps.processing.models import PageAssignment, SubmittedPage, MergedDocument
from apps.documents.models import Document
from common.enums import PageAssignmentStatus, ReviewStatus, PipelineStatus, PageStatus

from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

import logging
logger = logging.getLogger(__name__)

def broadcast_notification(group_name, payload):
    """Utility to send WebSocket notifications via NotificationConsumer"""
    try:
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            group_name,
            {
                'type': 'system_notification',
                'payload': payload
            }
        )
    except Exception as e:
        logger.error(f"WebSocket Broadcast Error: {e}")

# ── Section 7: System Timestamp Tracking ──────────────────────
@receiver(pre_save, sender=PageAssignment)
def capture_assignment_timestamps(sender, instance, **kwargs):
    """
    Automates start/end timestamps based on state transitions.
    """
    if not instance.pk:
        return
        
    try:
        old_instance = PageAssignment.objects.get(pk=instance.pk)
    except PageAssignment.DoesNotExist:
        return

    # Track processing start (when user clicks "Start" or opens workspace for first time)
    if not old_instance.processing_start_at and instance.status == PageAssignmentStatus.IN_PROGRESS:
        instance.processing_start_at = timezone.now()
        
    # Track completion/submission
    if old_instance.status != PageAssignmentStatus.SUBMITTED and instance.status == PageAssignmentStatus.SUBMITTED:
        instance.submitted_at = timezone.now()
        instance.processing_end_at = timezone.now()
        
        if instance.processing_start_at:
            instance.processing_duration = instance.processing_end_at - instance.processing_start_at


# ── Section 1: Active Resource Real-Time Status Tracking ─────
@receiver(post_save, sender=PageAssignment)
def update_resource_status_on_change(sender, instance, created, **kwargs):
    """
    Ensures the `status` (ACTIVE/BUSY) on ResourceProfile is updated 
    when assignments change state.
    """
    prof = instance.resource
    prof.refresh_status()


# ── Section 4: All Pages Submitted Pipeline Trigger ─────────
@receiver(post_save, sender=SubmittedPage)
def check_all_pages_submitted(sender, instance, created, **kwargs):
    """
    When a page is submitted, check if the entire document is now entirely submitted.
    If so, update pipeline status.
    """
    if created:
        doc = instance.document
        total_pages = doc.total_pages
        # Count UNIQUE pages that have at least one PENDING_REVIEW or APPROVED submission
        from common.enums import ReviewStatus
        active_submitted_pages = doc.submitted_pages.filter(
            review_status__in=[ReviewStatus.PENDING_REVIEW, ReviewStatus.APPROVED]
        ).values('page_number').distinct().count()
        
        if active_submitted_pages == total_pages and total_pages > 0:
            if doc.pipeline_status in [PipelineStatus.IN_PROGRESS, PipelineStatus.READY_TO_ASSIGN]:
                doc.pipeline_status = PipelineStatus.ALL_SUBMITTED
                doc.save(update_fields=['pipeline_status'])
                
                # Automatically trigger admin notification via WS
                broadcast_notification('admin_broadcast', {
                    'type': 'document_ready_for_review',
                    'doc_ref': doc.doc_ref,
                    'title': doc.title,
                    'message': f"Document {doc.title} is ready for review."
                })


# ── Section 10: All Pages Approved Pipeline Trigger ─────────
@receiver(post_save, sender=SubmittedPage)
def check_all_pages_approved(sender, instance, **kwargs):
    """
    When admin approves a submission, check if all submissions for the doc are approved.
    If so, trigger the Merge task.
    """
    if instance.review_status == ReviewStatus.APPROVED:
        doc = instance.document
        total_pages = doc.total_pages
        # Count unique pages that reached APPROVED status
        approved_pages_count = doc.submitted_pages.filter(
            review_status=ReviewStatus.APPROVED
        ).values('page_number').distinct().count()
        
        if approved_pages_count == total_pages and total_pages > 0:
             # Legacy automatic merge trigger disabled in favor of Bundle-Based Deterministic Merge
             # if doc.pipeline_status not in [PipelineStatus.MERGING, PipelineStatus.MERGED]:
             #    doc.pipeline_status = PipelineStatus.MERGING
             #    doc.save(update_fields=['pipeline_status'])
             #    from apps.processing.tasks import merge_document_pages
             #    MergedDocument.objects.get_or_create(document=doc)
             #    merge_document_pages.delay(doc.id, instance.reviewed_by_id if getattr(instance, 'reviewed_by_id', None) else None)
             pass

        # Trigger assignment engine to pick up any pending pages if resources are now free
        from apps.processing.tasks import assign_pages_task
        assign_pages_task.delay()

@receiver(post_save, sender=SubmittedPage)
def handle_submission_rejection(sender, instance, **kwargs):
    """
    When a submission is rejected by admin:
    1. Create/Update RejectedPage
    2. Create ReassignmentLog
    3. Reset Page status to PENDING
    4. Log the rejection on ResourceProfile
    """
    if instance.review_status == ReviewStatus.REJECTED:
        from apps.processing.models import RejectedPage, ReassignmentLog
        from apps.processing.tasks import assign_pages_task
        
        doc = instance.document
        page = instance.page
        assignment = instance.assignment
        
        # 1. Create RejectedPage entry
        rejected, created = RejectedPage.objects.get_or_create(
            document=doc,
            page=page,
            page_number=page.page_number,
            submission=instance,
            defaults={
                'original_resource': assignment.resource,
                'rejection_reason': getattr(instance, 'rejection_reason', 'OTHER') or 'OTHER'
            }
        )
        if not created:
            rejected.rejection_count = F('rejection_count') + 1
            rejected.save()
            
        # 2. Create ReassignmentLog
        ReassignmentLog.objects.create(
            original_assignment=assignment,
            reassigned_by_id=instance.reviewed_by_id if getattr(instance, 'reviewed_by_id', None) else None,
            reason=getattr(instance, 'rejection_reason', 'OTHER') or 'OTHER',
            admin_notes=instance.review_notes,
            previous_resource=assignment.resource
        )
        
        # 3. Reset Page
        page.status = PageStatus.PENDING
        page.save()
        
        # 4. Notify Resource & Update metrics
        prof = assignment.resource
        prof.rejection_count = F('rejection_count') + 1
        prof.save()
        
        broadcast_notification(f'user_{prof.user.id}', {
            'type': 'assignment_rejected',
            'doc_ref': doc.doc_ref,
            'page_number': page.page_number,
            'message': f"Your submission for Page {page.page_number} was rejected. Reason: {getattr(instance, 'rejection_reason', 'OTHER')}"
        })
        
        # 5. Trigger auto-reassignment
        if not getattr(instance, '_skip_auto_reassign', False):
            assign_pages_task.delay()

@receiver(post_save, sender=PageAssignment)
def notify_resource_on_assignment(sender, instance, created, **kwargs):
    """
    Notify a resource when they receive a new assignment.
    """
    if created and instance.status == PageAssignmentStatus.ASSIGNED:
        broadcast_notification(f'user_{instance.resource.user.id}', {
            'type': 'new_assignment',
            'doc_ref': instance.document.doc_ref,
            'page_number': instance.page.page_number,
            'message': f"You have been assigned page {instance.page.page_number} of {instance.document.title}"
        })
