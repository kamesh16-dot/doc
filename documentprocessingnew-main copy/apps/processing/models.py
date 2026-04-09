import uuid
from django.db import models
from django.conf import settings
from django.utils.translation import gettext_lazy as _
from common.enums import (
    AssignmentStatus, QueueStatus,
    PageAssignmentStatus, ReviewStatus, ReassignStatus,
    RejectionReason, MergeStatus, ApprovalStatus
)

# ─────────────────────────────────────────────────────────────
# NEW: Hierarchical Job Model (Strict Bundle Workflow)
# ─────────────────────────────────────────────────────────────
class Job(models.Model):
    """
    Top-level business unit for 1 or more documents.
    Used for strict, deterministic bundle-based processing.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255, help_text=_("Business name of this job"))
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'docpro_job'
        verbose_name = "Job"
        verbose_name_plural = "Jobs"

    def __str__(self):
        return f"Job: {self.name} ({self.id.hex[:8]})"


# ─────────────────────────────────────────────────────────────
# Legacy Block Assignment (kept for backwards compatibility)
# ─────────────────────────────────────────────────────────────
class Assignment(models.Model):
    page = models.ForeignKey(
        'documents.Page',
        on_delete=models.CASCADE,
        related_name='assignments',
        null=True, blank=True
    )
    document = models.ForeignKey(
        'documents.Document',
        on_delete=models.CASCADE,
        related_name='assignments',
        null=True
    )
    start_page = models.PositiveIntegerField(null=True)
    end_page = models.PositiveIntegerField(null=True)
    page_count = models.PositiveIntegerField(default=0)

    combined_file = models.FileField(
        upload_to='assignments/combined/%Y/%m/%d/',
        null=True, blank=True
    )
    processed_file = models.FileField(
        upload_to='assignments/processed/%Y/%m/%d/',
        null=True, blank=True
    )
    resource = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='assignments',
        limit_choices_to={'role': 'RESOURCE'}
    )

    assigned_at   = models.DateTimeField(auto_now_add=True)
    expires_at    = models.DateTimeField()
    completed_at  = models.DateTimeField(null=True, blank=True)
    viewed_at     = models.DateTimeField(null=True, blank=True)
    started_at    = models.DateTimeField(null=True, blank=True)
    submitted_at  = models.DateTimeField(null=True, blank=True)
    created_at    = models.DateTimeField(auto_now_add=True, null=True)

    status = models.CharField(
        max_length=20,
        choices=AssignmentStatus.choices,
        default=AssignmentStatus.WAITING
    )
    sla_breached = models.BooleanField(default=False)

    # Time tracking (seconds)
    max_processing_time = models.IntegerField(default=600)
    time_warning_sent   = models.BooleanField(default=False)
    timed_out           = models.BooleanField(default=False)

    class Meta:
        db_table = 'docpro_assignment'
        ordering = ['-assigned_at']
        indexes = [
            models.Index(fields=['document', 'status']),
            models.Index(fields=['status', 'resource']),
            models.Index(fields=['status', 'expires_at']),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['document', 'start_page', 'end_page'],
                name='unique_assignment_range',
                condition=models.Q(status__in=['WAITING', 'PROCESSING'])
            ),
        ]

    def __str__(self):
        return f"{self.resource} -> pages {self.start_page}-{self.end_page} ({self.status})"


class DocumentQueue(models.Model):
    document = models.OneToOneField(
        'documents.Document',
        on_delete=models.CASCADE,
        related_name='queue_entry'
    )
    position   = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    status     = models.CharField(
        max_length=20,
        choices=QueueStatus.choices,
        default=QueueStatus.WAITING
    )

    class Meta:
        db_table = 'docpro_document_queue'
        ordering = ['position', 'created_at']

    def __str__(self):
        return f"Queue[{self.position}] {self.document.name}"


# ─────────────────────────────────────────────────────────────
# NEW: Per-Page Assignment (Section 3 & 10 of spec)
# ─────────────────────────────────────────────────────────────
class PageAssignment(models.Model):
    """
    Per-page work unit assigned to a single resource person.
    Full state machine as per spec Section 10.
    """
    page = models.ForeignKey(
        'documents.Page',
        on_delete=models.CASCADE,
        related_name='page_assignments'
    )
    resource = models.ForeignKey(
        'accounts.ResourceProfile',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='page_assignments'
    )
    document = models.ForeignKey(
        'documents.Document',
        on_delete=models.CASCADE,
        related_name='page_assignments',
        null=True
    )

    # ── Status State Machine (Section 10) ──────────────────
    status = models.CharField(
        max_length=20,
        choices=PageAssignmentStatus.choices,
        default=PageAssignmentStatus.ASSIGNED,
        db_index=True
    )

    # ── Edited Content ─────────────────────────────────────
    edited_blocks_json = models.JSONField(default=list)
    resource_notes     = models.TextField(blank=True)

    # ── Processing Timestamps (Section 7) ──────────────────
    assigned_at         = models.DateTimeField(auto_now_add=True)
    processing_start_at = models.DateTimeField(null=True, blank=True)
    processing_end_at   = models.DateTimeField(null=True, blank=True)
    processing_duration = models.DurationField(null=True, blank=True)
    submitted_at        = models.DateTimeField(null=True, blank=True)
    reviewed_at         = models.DateTimeField(null=True, blank=True)
    approved_at         = models.DateTimeField(null=True, blank=True)

    # ── Timeout Tracking (Section 2) ───────────────────────
    max_processing_time = models.IntegerField(default=600)  # seconds
    time_warning_sent   = models.BooleanField(default=False)
    timed_out           = models.BooleanField(default=False)
    timed_out_at        = models.DateTimeField(null=True, blank=True)

    # ── Reassignment Tracking (Section 9) ──────────────────
    reassignment_count = models.IntegerField(default=0)
    is_reassigned      = models.BooleanField(default=False)
    reassigned_from    = models.ForeignKey(
        'self',
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='reassigned_to'
    )

    class Meta:
        db_table = 'page_assignments'
        ordering = ['-assigned_at']
        indexes = [
            models.Index(fields=['page', 'status']),
            models.Index(fields=['resource', 'status']),
            models.Index(fields=['status', 'processing_start_at']),
            models.Index(fields=['timed_out', 'status']),
            models.Index(fields=['document', 'status']),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['page'],
                name='unique_active_assignment',
                condition=models.Q(status__in=['PENDING', 'ASSIGNED', 'IN_PROGRESS'])
            )
        ]
        verbose_name = 'Page Assignment'

    def save(self, *args, **kwargs):
        # Auto-calculate processing duration
        if self.processing_start_at and self.processing_end_at:
            self.processing_duration = self.processing_end_at - self.processing_start_at
        super().save(*args, **kwargs)

    def __str__(self):
        res_name = self.resource.user.username if self.resource and hasattr(self.resource, 'user') and self.resource.user else "DELETED"
        return f"PageAssignment[{res_name}] page {self.page.page_number} ({self.status})"


# ─────────────────────────────────────────────────────────────
# NEW: Submitted Page (Section 4 of spec)
# ─────────────────────────────────────────────────────────────
class SubmittedPage(models.Model):
    """
    Record created when a resource submits their edited page.
    Pending admin review.
    """
    assignment = models.OneToOneField(
        PageAssignment,
        on_delete=models.CASCADE,
        related_name='submission'
    )
    page = models.ForeignKey(
        'documents.Page',
        on_delete=models.CASCADE,
        related_name='submissions'
    )
    submitted_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='submitted_pages'
    )
    document = models.ForeignKey(
        'documents.Document',
        on_delete=models.CASCADE,
        related_name='submitted_pages'
    )
    bundle = models.ForeignKey(
        'desktop_bridge.AssignmentBundle',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='submitted_pages'
    )

    # ── Submission Content ─────────────────────────────────
    final_text         = models.TextField(blank=True)
    edited_blocks_json = models.JSONField(default=list)
    output_page_file   = models.FileField(
        upload_to='submitted_pages/%Y/%m/%d/',
        null=True, blank=True
    )
    resource_notes = models.TextField(blank=True)
    page_number    = models.IntegerField()

    # ── Processing Metrics ─────────────────────────────────
    bundle_version_id = models.UUIDField(null=True, blank=True, help_text="Atomic ID pairing all pages in a single bundle submission version")
    processing_duration = models.DurationField(null=True, blank=True)
    processing_start_at = models.DateTimeField(null=True, blank=True)
    processing_end_at   = models.DateTimeField(null=True, blank=True)
    words_processed     = models.IntegerField(default=0)
    blocks_edited       = models.IntegerField(default=0)
    blocks_total        = models.IntegerField(default=0)

    # ── Review Status ──────────────────────────────────────
    review_status = models.CharField(
        max_length=20,
        choices=ReviewStatus.choices,
        default=ReviewStatus.PENDING_REVIEW,
        db_index=True
    )

    # ── Timestamps ─────────────────────────────────────────
    submitted_at = models.DateTimeField(auto_now_add=True, db_index=True)
    reviewed_at  = models.DateTimeField(null=True, blank=True)
    reviewed_by  = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='reviewed_submissions'
    )
    review_notes     = models.TextField(blank=True)
    rejection_reason = models.CharField(
        max_length=50, 
        choices=RejectionReason.choices, 
        null=True, blank=True
    )

    class Meta:
        db_table = 'submitted_pages'
        ordering = ['-submitted_at']
        indexes = [
            models.Index(fields=['document', 'page_number']),
            models.Index(fields=['review_status']),
            models.Index(fields=['submitted_by', 'submitted_at']),
            models.Index(fields=['document', 'review_status']),
        ]
        verbose_name = 'Submitted Page'

    def __str__(self):
        user_name = self.submitted_by.username if self.submitted_by else "DELETED"
        return f"Submission[{user_name}] page {self.page_number} ({self.review_status})"


# ─────────────────────────────────────────────────────────────
# NEW: Rejected Page (Section 9 of spec)
# ─────────────────────────────────────────────────────────────
class RejectedPage(models.Model):
    """
    Pages rejected by admin — enters reassignment queue.
    Tracks rejection count and excluded resources.
    """
    submission = models.ForeignKey(
        SubmittedPage,
        on_delete=models.CASCADE,
        related_name='rejections'
    )
    page = models.ForeignKey(
        'documents.Page',
        on_delete=models.CASCADE,
        related_name='rejections'
    )
    document = models.ForeignKey(
        'documents.Document',
        on_delete=models.CASCADE,
        related_name='rejected_pages'
    )
    rejected_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='rejected_pages'
    )
    original_resource = models.ForeignKey(
        'accounts.ResourceProfile',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='rejections_received'
    )
    page_number = models.IntegerField()

    # ── Rejection Details ──────────────────────────────────
    rejection_reason = models.CharField(
        max_length=20,
        choices=RejectionReason.choices
    )
    rejection_notes = models.TextField(blank=True)
    rejected_at     = models.DateTimeField(auto_now_add=True, db_index=True)

    # ── Reassignment Queue ─────────────────────────────────
    reassign_status = models.CharField(
        max_length=15,
        choices=ReassignStatus.choices,
        default=ReassignStatus.IN_QUEUE,
        db_index=True
    )
    rejection_count = models.IntegerField(default=1)
    max_rejections  = models.IntegerField(default=3)
    queue_position  = models.IntegerField(default=0)
    reassigned_at   = models.DateTimeField(null=True, blank=True)
    reassigned_to   = models.ForeignKey(
        'accounts.ResourceProfile',
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='reassigned_rejections'
    )
    new_assignment = models.ForeignKey(
        PageAssignment,
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='created_from_rejection'
    )

    # ── Excluded Resources ─────────────────────────────────
    excluded_resources = models.ManyToManyField(
        'accounts.ResourceProfile',
        blank=True,
        related_name='excluded_from_rejections'
    )

    class Meta:
        db_table = 'rejected_pages'
        ordering = ['queue_position', '-rejected_at']
        indexes = [
            models.Index(fields=['reassign_status', 'queue_position']),
            models.Index(fields=['document', 'page_number']),
            models.Index(fields=['rejected_by', 'rejected_at']),
        ]
        verbose_name = 'Rejected Page'

    def save(self, *args, **kwargs):
        if self.rejection_count >= self.max_rejections:
            self.reassign_status = ReassignStatus.ESCALATED
        super().save(*args, **kwargs)

    @classmethod
    def get_reassignment_queue(cls):
        return cls.objects.filter(
            reassign_status=ReassignStatus.IN_QUEUE
        ).select_related(
            'page', 'document', 'original_resource'
        ).order_by('queue_position', 'rejected_at')

    def __str__(self):
        return f"RejectedPage page {self.page_number} ({self.reassign_status})"


# ─────────────────────────────────────────────────────────────
# NEW: ReassignmentLog (Section 9 audit trail)
# ─────────────────────────────────────────────────────────────
class ReassignmentLog(models.Model):
    original_assignment = models.ForeignKey(
        PageAssignment,
        on_delete=models.CASCADE,
        related_name='reassignment_logs'
    )
    new_assignment = models.ForeignKey(
        PageAssignment,
        null=True,
        on_delete=models.SET_NULL,
        related_name='created_by_reassignment'
    )
    reassigned_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='reassignment_actions'
    )
    reassigned_at     = models.DateTimeField(auto_now_add=True)
    reason            = models.CharField(max_length=20, choices=RejectionReason.choices)
    admin_notes       = models.TextField(blank=True)
    previous_resource = models.ForeignKey(
        'accounts.ResourceProfile',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='lost_assignments'
    )
    new_resource = models.ForeignKey(
        'accounts.ResourceProfile',
        null=True,
        on_delete=models.SET_NULL,
        related_name='gained_assignments'
    )

    class Meta:
        db_table = 'reassignment_logs'
        ordering = ['-reassigned_at']
        verbose_name = 'Reassignment Log'

    def __str__(self):
        user_name = self.reassigned_by.username if self.reassigned_by else "SYSTEM/DELETED"
        return f"Reassignment by {user_name} @ {self.reassigned_at:%Y-%m-%d %H:%M}"


# ─────────────────────────────────────────────────────────────
# NEW: MergedDocument (Section — Celery Merge Task)
# ─────────────────────────────────────────────────────────────
class MergedDocument(models.Model):
    document = models.OneToOneField(
        'documents.Document',
        on_delete=models.CASCADE,
        related_name='merged_document'
    )
    merged_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='merged_documents'
    )

    merged_file            = models.FileField(upload_to='merged/%Y/%m/%d/', null=True, blank=True)
    merged_file_size_bytes = models.BigIntegerField(default=0)
    merged_file_hash_md5   = models.CharField(max_length=32, blank=True)
    page_order_manifest    = models.JSONField(default=list)

    total_pages_merged    = models.IntegerField(default=0)
    total_words           = models.IntegerField(default=0)
    total_processing_time = models.DurationField(null=True, blank=True)
    pages_with_rejections = models.IntegerField(default=0)
    unique_resources_used = models.IntegerField(default=0)

    merge_attempt_count = models.IntegerField(default=0)
    manifest_json       = models.JSONField(default=dict, blank=True)

    merge_status    = models.CharField(
        max_length=15,
        choices=MergeStatus.choices,
        default=MergeStatus.PENDING,
        db_index=True
    )
    merge_error     = models.TextField(blank=True)
    page_gap_errors = models.JSONField(default=list)

    merge_triggered_at  = models.DateTimeField(null=True, blank=True)
    merge_started_at    = models.DateTimeField(null=True, blank=True)
    merge_completed_at  = models.DateTimeField(null=True, blank=True)
    approved_at         = models.DateTimeField(null=True, blank=True)
    delivered_at        = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'merged_documents'
        verbose_name = 'Merged Document'

    @property
    def is_complete(self):
        return self.merge_status == MergeStatus.COMPLETED

    @property
    def merge_duration(self):
        if self.merge_started_at and self.merge_completed_at:
            return self.merge_completed_at - self.merge_started_at
        return None

    def __str__(self):
        return f"MergedDoc for {self.document.name} ({self.merge_status})"


# ─────────────────────────────────────────────────────────────
# NEW: ApprovedDocument (Section — Admin Final Approval)
# ─────────────────────────────────────────────────────────────
class ApprovedDocument(models.Model):
    document = models.OneToOneField(
        'documents.Document',
        on_delete=models.CASCADE,
        related_name='approval'
    )
    merged_document = models.OneToOneField(
        MergedDocument,
        on_delete=models.CASCADE,
        related_name='approval'
    )
    approved_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='approved_documents'
    )

    final_file             = models.FileField(upload_to='approved/%Y/%m/%d/', null=True, blank=True)
    final_file_size_bytes  = models.BigIntegerField(default=0)
    final_file_hash_sha256 = models.CharField(max_length=64, blank=True)
    approval_notes         = models.TextField(blank=True)
    quality_score          = models.FloatField(null=True, blank=True)

    approval_status = models.CharField(
        max_length=15,
        choices=ApprovalStatus.choices,
        default=ApprovalStatus.APPROVED,
        db_index=True
    )

    download_url         = models.URLField(blank=True)
    download_url_expiry  = models.DateTimeField(null=True, blank=True)
    delivered_to_client  = models.BooleanField(default=False)
    client_notified_at   = models.DateTimeField(null=True, blank=True)
    client_downloaded_at = models.DateTimeField(null=True, blank=True)
    summary_report       = models.JSONField(default=dict)

    approved_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at  = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'approved_documents'
        ordering = ['-approved_at']
        indexes = [
            models.Index(fields=['approval_status']),
            models.Index(fields=['approved_by', 'approved_at']),
            models.Index(fields=['delivered_to_client']),
        ]
        verbose_name = 'Approved Document'

    def generate_summary_report(self):
        from django.db.models import Avg, Sum, Count, F
        doc = self.document
        assignments = PageAssignment.objects.filter(
            document=doc,
            status=PageAssignmentStatus.APPROVED
        ).select_related('resource__user')

        total_time_agg = assignments.aggregate(t=Sum('processing_duration'))['t']
        avg_time_agg   = assignments.aggregate(a=Avg('processing_duration'))['a']

        self.summary_report = {
            'total_pages': doc.total_pages,
            'total_resources': assignments.values('resource').distinct().count(),
            'total_processing_time_seconds': int(
                total_time_agg.total_seconds() if total_time_agg else 0
            ),
            'pages_rejected': RejectedPage.objects.filter(document=doc).count(),
            'avg_page_processing_time': int(
                avg_time_agg.total_seconds() if avg_time_agg else 0
            ),
            'resource_breakdown': list(
                assignments.values(
                    username=F('resource__user__username')
                ).annotate(pages=Count('id')).order_by('-pages')
            ),
        }
        self.save()

    def __str__(self):
        user_name = self.approved_by.username if self.approved_by else "SYSTEM/DELETED"
        return f"Approved {self.document.name} by {user_name}"
