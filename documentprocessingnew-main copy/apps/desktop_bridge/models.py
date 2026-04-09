from __future__ import annotations

import uuid
from django.conf import settings
from django.db import models


class DesktopDevice(models.Model):
    """
    Represents a verified Windows workstation running the DocPro Agent.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL, 
        on_delete=models.CASCADE, 
        related_name="desktop_devices"
    )
    device_name = models.CharField(max_length=255, help_text="Human-readable name of the machine")
    machine_id = models.CharField(max_length=255, blank=True, default="", help_text="Hardware-based UUID")
    agent_version = models.CharField(max_length=50, blank=True, default="")
    last_heartbeat_at = models.DateTimeField(null=True, blank=True)
    last_seen_ip = models.GenericIPAddressField(null=True, blank=True)
    is_active = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'docpro_desktop_device'
        verbose_name = "Desktop Device"
        verbose_name_plural = "Desktop Devices"

    def __str__(self) -> str:
        return f"{self.device_name} ({self.user})"


class AssignmentBundle(models.Model):
    """
    A contiguous range of pages from a Document leased to a Desktop Device for ABBYY editing.
    """
    class Status(models.TextChoices):
        READY      = "READY",      "Ready for Download"
        LEASED     = "LEASED",     "Leased to Device"
        DOWNLOADED = "DOWNLOADED", "Downloaded by Agent"
        EDITING    = "EDITING",    "Being Edited in ABBYY"
        UPLOADED   = "UPLOADED",   "Result Uploaded" 
        SUBMITTED  = "SUBMITTED",  "Submitted for Review"
        APPROVED   = "APPROVED",   "Admin Approved"
        REJECTED   = "REJECTED",   "Admin Rejected"
        COMPLETED  = "COMPLETED",  "Legacy: Bundle Completed & Approved" # Transitioned to APPROVED
        MERGED     = "MERGED",     "Merged into Final PDF"
        EXPIRED    = "EXPIRED",    "Lease Expired"
        FAILED     = "FAILED",     "Processing Failed"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    document = models.ForeignKey(
        "documents.Document", 
        on_delete=models.CASCADE, 
        related_name="bundles"
    )
    bundle_index = models.PositiveIntegerField(help_text="Strict sequence index starting from 0")
    page_start = models.PositiveIntegerField(help_text="Start page number (1-indexed)")
    page_end = models.PositiveIntegerField(help_text="End page number (inclusive)")
    page_numbers = models.JSONField(default=list, help_text="List of exact page numbers in this bundle")

    source_pdf = models.FileField(upload_to="storage/internal_bundles/source/%Y/%m/%d/", help_text="Sealed PDF for this range")
    result_pdf = models.FileField(upload_to="storage/2_edited_bundles/%Y/%m/%d/", null=True, blank=True, help_text="The edited PDF returned by the agent")

    # ── New Strict Bundle Flow (Section 2 & 10) ──────────────────
    job = models.ForeignKey(
        "processing.Job",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="bundles",
        help_text="The Job this bundle belongs to (Legacy if null)"
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="assigned_bundles",
        help_text="The specific user assigned to this atomic bundle"
    )
    input_file = models.FileField(
        upload_to="storage/internal_bundles/input/%Y/%m/%d/",
        null=True,
        blank=True,
        help_text="Atomic input file (PDF)"
    )
    output_file = models.FileField(
        upload_to="storage/2_edited_bundles/%Y/%m/%d/",
        null=True,
        blank=True,
        help_text="Atomic output file (DOCX)"
    )

    # ── Admin Review (Section 2 & 5) ──────────────────────────
    reviewed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="reviewed_bundles"
    )
    reviewed_at = models.DateTimeField(null=True, blank=True)
    rejection_reason = models.TextField(null=True, blank=True)
    attempt_count = models.PositiveIntegerField(default=1)
    completed_at = models.DateTimeField(null=True, blank=True)

    status = models.CharField(max_length=20, choices=Status.choices, default=Status.READY)
    version = models.PositiveIntegerField(default=1, help_text="Submission version counter")
    lease_token = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    leased_to = models.ForeignKey(
        DesktopDevice, 
        null=True, 
        blank=True, 
        on_delete=models.SET_NULL, 
        related_name="leases"
    )
    lease_expires_at = models.DateTimeField(null=True, blank=True)

    manifest = models.JSONField(default=dict, help_text="JSON manifest shipped in the ZIP")
    source_sha256 = models.CharField(max_length=64, blank=True, default="", help_text="Checksum of the source PDF")
    result_sha256 = models.CharField(max_length=64, blank=True, default="", help_text="Checksum of the uploaded result")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    downloaded_at = models.DateTimeField(null=True, blank=True)
    uploaded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'docpro_assignment_bundle'
        unique_together = [("document", "bundle_index")]
        indexes = [
            models.Index(fields=["status", "lease_expires_at"]),
            models.Index(fields=["document", "bundle_index"]),
        ]

    def __str__(self) -> str:
        return f"Doc {self.document_id} Bundle {self.bundle_index} ({self.page_start}-{self.page_end})"


class UploadedPDF(models.Model):
    """
    Represents a physical PDF file uploaded by an agent.
    Acts as the source for PageVersions.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    document = models.ForeignKey("documents.Document", on_delete=models.CASCADE, related_name="uploads")
    bundle = models.ForeignKey(AssignmentBundle, on_delete=models.SET_NULL, null=True, related_name="uploads")
    device = models.ForeignKey(DesktopDevice, on_delete=models.SET_NULL, null=True, related_name="uploads")
    
    file = models.FileField(upload_to="desktop_bridge/uploads/%Y/%m/%d/", help_text="The full uploaded PDF")
    checksum = models.CharField(max_length=64, help_text="SHA256 checksum of the uploaded file")
    
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'docpro_uploaded_pdf'
        ordering = ['-created_at']

    def __str__(self) -> str:
        return f"Upload {self.id} (Doc {self.document_id})"


class PageVersion(models.Model):
    """
    A single page extracted/indexed from an UploadedPDF.
    The 'Page Ledger' that allows deterministic resolution.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    document = models.ForeignKey("documents.Document", on_delete=models.CASCADE, related_name="page_versions")
    page = models.ForeignKey("documents.Page", on_delete=models.CASCADE, related_name="versions")
    bundle = models.ForeignKey(AssignmentBundle, on_delete=models.CASCADE, related_name="page_versions")
    uploaded_pdf = models.ForeignKey(UploadedPDF, on_delete=models.CASCADE, related_name="page_versions")
    
    page_number = models.PositiveIntegerField(help_text="1-indexed page number in the original document")
    page_index_in_pdf = models.PositiveIntegerField(help_text="0-indexed position within the uploaded result_pdf")
    
    slice_size = models.PositiveIntegerField(help_text="Number of pages in the bundle (smaller is better/more specific)")
    source_checksum = models.CharField(max_length=64, blank=True, help_text="Checksum of the source page for change detection")
    
    is_valid = models.BooleanField(default=True)
    metadata = models.JSONField(default=dict, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'docpro_page_version'
        unique_together = [("uploaded_pdf", "page_number")]
        indexes = [
            models.Index(fields=["document", "page_number", "slice_size", "updated_at"]),
        ]

    def __str__(self) -> str:
        return f"Doc {self.document_id} P{self.page_number} V{self.id}"


class MergeManifest(models.Model):
    """
    A deterministic record of a successful (or failed) document merge.
    """
    class Status(models.TextChoices):
        PENDING    = "PENDING",    "Merge Pending"
        SUCCESS    = "SUCCESS",    "Merge Successful"
        FAILED     = "FAILED",     "Merge Failed"
        PARTIAL    = "PARTIAL",    "Partial Coverage"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    document = models.ForeignKey("documents.Document", on_delete=models.CASCADE, related_name="merge_manifests")
    
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    final_pdf = models.FileField(upload_to="desktop_bridge/merged/%Y/%m/%d/", null=True, blank=True)
    
    # page_number -> page_version_id
    version_map = models.JSONField(default=dict, help_text="Map of page numbers to the specific PageVersion IDs used")
    missing_pages = models.JSONField(default=list, help_text="List of page numbers missing during this merge attempt")
    
    error_details = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'docpro_merge_manifest'


class MergeAuditLog(models.Model):
    """
    Full history of merge attempts and resolution decisions.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    document = models.ForeignKey("documents.Document", on_delete=models.CASCADE, related_name="merge_logs")
    manifest = models.ForeignKey(MergeManifest, on_delete=models.SET_NULL, null=True, related_name="audit_logs")
    
    event_type = models.CharField(max_length=100)
    details = models.JSONField(default=dict)
    
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'docpro_merge_audit_log'
