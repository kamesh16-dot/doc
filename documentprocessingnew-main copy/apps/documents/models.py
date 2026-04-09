import uuid
from django.db import models
from django.conf import settings
from django.utils.translation import gettext_lazy as _
from common.enums import (
    DocumentStatus, PageStatus, ComplexityType, 
    ConversionStatus, OriginalFormat, ValidationStatus,
    PipelineStatus, DocumentPriority
)

from django.utils.timezone import now

class ActiveDocumentManager(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(is_deleted=False)

class Document(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    client = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='documents',
        limit_choices_to={'role': 'CLIENT'}
    )
    job = models.ForeignKey(
        'processing.Job',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='documents',
        help_text=_("Associated Job for strict bundle workflows (Legacy if null)")
    )
    organization = models.ForeignKey(
        'accounts.Organization',
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='documents'
    )
    name = models.CharField(max_length=255, null=True, blank=True)
    original_file = models.FileField(upload_to='storage/1_original_pdfs/%Y/%m/%d/', null=True, blank=True)
    file = models.FileField(upload_to='storage/internal_processing/%Y/%m/%d/', null=True, blank=True, help_text=_("The PDF file used for OCR/Splitting"))
    converted_pdf = models.FileField(upload_to='storage/converted/%Y/%m/%d/', null=True, blank=True)
    status = models.CharField(
        max_length=20,
        choices=DocumentStatus.choices,
        default=DocumentStatus.UPLOADED
    )
    completion_percentage = models.FloatField(default=0.0)
    merge_in_progress = models.BooleanField(default=False)
    version = models.PositiveIntegerField(default=1)
    total_pages = models.PositiveIntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    conversion_started_at = models.DateTimeField(null=True, blank=True)
    conversion_completed_at = models.DateTimeField(null=True, blank=True)
    conversion_error = models.TextField(null=True, blank=True)
    conversion_status = models.CharField(
        max_length=20,
        choices=ConversionStatus.choices,
        default=ConversionStatus.NOT_REQUIRED
    )
    original_format = models.CharField(
        max_length=20,
        choices=OriginalFormat.choices,
        default=OriginalFormat.READABLE_PDF
    )
    conversion_method = models.CharField(
        max_length=20,
        choices=[
            ('LIBREOFFICE', 'LibreOffice'),
            ('DOCX2PDF', 'docx2pdf'),
        ],
        blank=True,
        null=True
    )
    celery_task_id = models.CharField(max_length=255, blank=True, null=True)
    converted_file_size_bytes = models.BigIntegerField(default=0)
    converted_file_hash_md5 = models.CharField(max_length=32, blank=True, null=True)
    final_file = models.FileField(upload_to='storage/finals/%Y/%m/%d/', null=True, blank=True)
    final_word_file = models.FileField(upload_to='storage/3_final_merged_docx/%Y/%m/%d/', null=True, blank=True)
    final_word_generated_at = models.DateTimeField(null=True, blank=True)
    final_word_manifest = models.JSONField(default=dict, blank=True)
    final_word_error = models.TextField(blank=True, default="")
    is_deleted = models.BooleanField(default=False)
    deleted_at = models.DateTimeField(null=True, blank=True)
    deleted_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='deleted_documents'
    )
    deletion_reason = models.TextField(null=True, blank=True)

    objects = models.Manager() # Default manager
    active = ActiveDocumentManager() # Filtered manager

    # ── Section 6: Slug-based URLs (no job IDs) ──────────────
    doc_ref = models.SlugField(
        unique=True, db_index=True, blank=True, null=True,
        max_length=80,
        help_text=_("Human-readable slug, e.g. invoice-report-2026-03")
    )
    title = models.CharField(max_length=500, blank=True)

    # ── Section 1: Pipeline Status ─────────────────────
    pipeline_status = models.CharField(
        max_length=25,
        choices=PipelineStatus.choices,
        default=PipelineStatus.UPLOADED,
        db_index=True
    )
    pipeline_error = models.TextField(blank=True, default="")

    # ── Priority & SLA ───────────────────────────────
    priority = models.CharField(
        max_length=10,
        choices=DocumentPriority.choices,
        default=DocumentPriority.NORMAL
    )
    deadline = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'docpro_client_document'
        ordering = ['-created_at']

    def save(self, *args, **kwargs):
        # Auto-generate doc_ref slug if not set
        if not self.doc_ref:
            from django.utils.text import slugify
            base = slugify(self.name or self.title or str(self.id))[:40]
            self.doc_ref = f"{base}-{str(self.id)[:8]}"
        # Sync title to name
        if not self.title and self.name:
            self.title = self.name
        elif not self.name and self.file:
            self.name = self.file.name.split('/')[-1]
            self.title = self.name
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.name or self.id} ({self.status})"

class Page(models.Model):
    id = models.BigAutoField(primary_key=True)
    document = models.ForeignKey(
        Document,
        on_delete=models.CASCADE,
        related_name='pages'
    )
    page_number = models.PositiveIntegerField()
    content_file = models.FileField(upload_to='pages/splits/%Y/%m/%d/')
    
    status = models.CharField(
        max_length=20,
        choices=PageStatus.choices,
        default=PageStatus.PENDING
    )
    
    # OCR Tracking
    ocr_provider = models.CharField(
        max_length=50,
        null=True,
        blank=True,
        help_text="Service provider used for OCR (e.g., 'adobe', 'azure', 'tesseract')"
    )
    
    # Locking & Assignment
    current_assignee = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='assigned_pages',
        limit_choices_to={'role': 'RESOURCE'}
    )
    is_locked = models.BooleanField(default=False)
    locked_at = models.DateTimeField(
        null=True, 
        blank=True,
        help_text=_("Timestamp when page was locked to a resource")
    )
    
    # Processing Output
    processed_file = models.FileField(
        upload_to='pages/processed/%Y/%m/%d/',
        null=True, 
        blank=True
    )
    
    # OCR and Editor Data
    version = models.PositiveIntegerField(
        default=0,
        help_text=_("Optimistic locking version")
    )
    layout_data = models.JSONField(
        null=True,
        blank=True,
        help_text=_("Reconstructed layout bounding boxes and structure")
    )
    text_content = models.TextField(
        null=True,
        blank=True,
        help_text=_("Raw extracted or edited text content")
    )
    
    # Policy Compliance & Timing
    processing_started_at = models.DateTimeField(
        null=True, 
        blank=True,
        help_text=_("When the resource first opened this page for editing")
    )
    processing_start_date = models.DateField(
        null=True, 
        blank=True,
        help_text=_("Date when processing started")
    )
    processing_start_time = models.TimeField(
        null=True, 
        blank=True,
        help_text=_("Time when processing started")
    )
    processing_completed_at = models.DateTimeField(
        null=True, 
        blank=True,
        help_text=_("When the resource submitted this page")
    )
    processing_end_date = models.DateField(
        null=True, 
        blank=True,
        help_text=_("Date when processing ended")
    )
    processing_end_time = models.TimeField(
        null=True, 
        blank=True,
        help_text=_("Time when processing ended")
    )
    processing_duration_seconds = models.PositiveIntegerField(
        default=0,
        help_text=_("Total time spent processing this page in seconds")
    )
    total_time_spent = models.PositiveIntegerField(
        default=0,
        help_text=_("Total time spent (alias for duration in seconds)")
    )

    # Processing State
    is_processed = models.BooleanField(
        default=False,
        help_text=_("True if OCR or layout analysis is complete")
    )
    is_scanned = models.BooleanField(
        default=False,
        help_text=_("True if the document was detected as scanned/image-based")
    )

    # Validation Results
    is_validated = models.BooleanField(
        default=False,
        help_text=_("Whether this page has passed the validation requirements")
    )
    validation_errors = models.JSONField(
        null=True,
        blank=True,
        help_text=_("List of validation failures (upload issues, formatting errors, etc.)")
    )

    # Complexity Features
    complexity_type = models.CharField(
        max_length=20,
        choices=ComplexityType.choices,
        default=ComplexityType.SIMPLE
    )
    complexity_score = models.PositiveIntegerField(
        default=1,
        help_text=_("Weighted complexity score (standardized to 1 for all pages)")
    )
    complexity_weight = models.PositiveIntegerField(default=1)
    complexity_scored_at = models.DateTimeField(null=True, blank=True)
    
    # Metadata for filtering/reporting
    word_count = models.IntegerField(default=0)
    table_count = models.IntegerField(default=0)
    image_count = models.IntegerField(default=0)
    block_density = models.FloatField(default=0.0)
    image_density = models.FloatField(default=0.0)
    
    validation_status = models.CharField(
        max_length=30,
        choices=ValidationStatus.choices,
        default=ValidationStatus.PENDING_VALIDATION
    )
    
    # Page Dimensions (in PDF points)
    pdf_page_width = models.FloatField(default=595.0, help_text=_("PDF page width in points (A4 default)"))
    pdf_page_height = models.FloatField(default=842.0, help_text=_("PDF page height in points (A4 default)"))
    
    # Flags & Stats
    blocks_extracted = models.BooleanField(default=False)
    blocks_count = models.IntegerField(default=0)
    has_tables = models.BooleanField(default=False)
    split_error = models.TextField(blank=True, default='', help_text=_("Errors encountered during PDF splitting"))
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'docpro_page'
        ordering = ['document', 'page_number']
        unique_together = ['document', 'page_number']
        indexes = [
            models.Index(fields=['document', 'status', 'page_number']),
            models.Index(fields=['status', 'locked_at']),
            models.Index(fields=['current_assignee', 'status']),
        ]

    def __str__(self):
        return f"Page {self.page_number} of {self.document.id}"

class DocumentVersion(models.Model):
    document = models.ForeignKey(Document, on_delete=models.CASCADE, related_name='versions')
    version_number = models.PositiveIntegerField()
    merged_file = models.FileField(upload_to='documents/versions/%Y/%m/%d/')
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'docpro_document_version'
        unique_together = ['document', 'version_number']

class Block(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    page = models.ForeignKey(Page, on_delete=models.CASCADE, related_name='blocks')
    block_index = models.IntegerField(default=0, help_text=_("Index in extraction order"))
    block_id = models.CharField(max_length=100, help_text=_("Identifier from OCR/Layout analysis"))
    block_type = models.CharField(
        max_length=20,
        choices=[
            ('text', 'Plain Text'),
            ('table_cell', 'Table Cell'),
            ('header', 'Header'),
            ('footer', 'Footer'),
            ('caption', 'Caption'),
            ('table', 'Table Area'),
            ('image', 'Image Area'),
            ('line', 'Single Line'),
            ('underline', 'Underline'),
            ('double_line', 'Double Line'),
        ],
        default='text'
    )
    extracted_text = models.TextField(blank=True, help_text=_("Original text from OCR"))
    original_text = models.TextField(blank=True, help_text=_("OCR/native text — NEVER modified"))
    current_text = models.TextField(blank=True, help_text=_("Latest user edit"))
    last_edited_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='edited_blocks'
    )
    semantic_role = models.CharField(
        max_length=30,
        default='body_text',
        choices=[
            ('heading_1', 'Heading 1'),
            ('heading_2', 'Heading 2'),
            ('heading_3', 'Heading 3'),
            ('body_text', 'Body Text'),
            ('footnote', 'Footnote'),
            ('caption', 'Caption'),
        ],
        help_text=_("Detected semantic role for layout reconstruction")
    )
    last_edited_at = models.DateTimeField(null=True, blank=True)
    
    # Coordinates (in PDF points)
    x = models.FloatField(default=0.0)
    y = models.FloatField(default=0.0)
    width = models.FloatField(default=0.0)
    height = models.FloatField(default=0.0)
    bbox = models.JSONField(help_text=_("Bounding box [x0, y0, x1, y1]"), null=True, blank=True)
    
    # Font Properties
    font_name = models.CharField(max_length=100, default='Helvetica')
    font_size = models.FloatField(default=10.0)
    font_weight = models.CharField(max_length=10, choices=[('normal','Normal'),('bold','Bold')], default='normal')
    font_style = models.CharField(max_length=10, choices=[('normal','Normal'),('italic','Italic')], default='normal')
    font_color = models.CharField(max_length=10, default='#000000')
    text_align = models.CharField(
        max_length=20, 
        choices=[('left','Left'),('center','Center'),('right','Right'),('justify','Justify')], 
        default='left'
    )
    font_variant = models.CharField(
        max_length=20,
        choices=[('normal','Normal'),('superscript','Superscript'),('subscript','Subscript')],
        default='normal'
    )
    z_index = models.IntegerField(default=0, help_text=_("Layering order for overlapping elements"))
    line_height = models.FloatField(default=1.2, help_text=_("Relative line height for paragraph spacing"))
    
    # Table Info (if block_type == 'table_cell')
    table_id = models.CharField(max_length=100, blank=True)
    row_index = models.IntegerField(null=True, blank=True)
    col_index = models.IntegerField(null=True, blank=True)
    rowspan = models.IntegerField(default=1)
    colspan = models.IntegerField(default=1)
    
    is_underlined = models.BooleanField(default=False)
    is_dirty = models.BooleanField(default=False)
    is_manually_edited = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'docpro_block'
        unique_together = [['page', 'block_id']]
        ordering = ['y', 'x']   # reading order: top→bottom, left→right
        indexes = [
            models.Index(fields=['page', 'block_id']),
            models.Index(fields=['page', 'block_type']),
            models.Index(fields=['table_id']),
        ]

    def get_css_coords(self, css_width: float, css_height: float) -> dict:
        """Return CSS pixel coordinates scaled for a given container."""
        sx = css_width  / self.page.pdf_page_width  if self.page.pdf_page_width  else 1
        sy = css_height / self.page.pdf_page_height if self.page.pdf_page_height else 1
        return {
            'left':      round(self.x      * sx, 2),
            'top':       round(self.y      * sy, 2),
            'width':     round(self.width  * sx, 2),
            'height':    round(self.height * sy, 2),
            'font_size': round(self.font_size * sy, 2),
        }

class PageTable(models.Model):
    page = models.ForeignKey(Page, on_delete=models.CASCADE, related_name='tables')
    table_ref = models.CharField(max_length=100)
    x = models.FloatField()
    y = models.FloatField()
    width = models.FloatField()
    height = models.FloatField()
    row_count = models.IntegerField(default=0)
    col_count = models.IntegerField(default=0)
    table_json = models.JSONField(default=list)
    col_widths = models.JSONField(default=list)
    row_heights = models.JSONField(default=list, help_text=_("Array of row heights in percentages"))
    col_aligns = models.JSONField(default=list)
    row_colors = models.JSONField(default=list)
    has_borders = models.BooleanField(default=True)
    has_header = models.BooleanField(default=False)
    is_manually_edited = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'docpro_page_table'
        ordering = ['y', 'x']

class PageImage(models.Model):
    page = models.ForeignKey(Page, on_delete=models.CASCADE, related_name='images')
    image_ref = models.CharField(max_length=100)
    x = models.FloatField()
    y = models.FloatField()
    width = models.FloatField()
    height = models.FloatField()
    image_file = models.FileField(upload_to='pages/images/%Y/%m/%d/', null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'docpro_page_image'
        ordering = ['y', 'x']

class BlockEdit(models.Model):
    id = models.BigAutoField(primary_key=True)
    block = models.ForeignKey(Block, on_delete=models.CASCADE, related_name='edits')
    edited_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    text = models.TextField()
    saved_at = models.DateTimeField(auto_now_add=True)
    page_num = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = 'docpro_block_edit'
        ordering = ['-saved_at']
