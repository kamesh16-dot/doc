from django.db import models
from django.utils.translation import gettext_lazy as _

class UserRole(models.TextChoices):
    ADMIN = 'ADMIN', _('Admin')
    CLIENT = 'CLIENT', _('Client')
    RESOURCE = 'RESOURCE', _('Resource')

# ── Resource Availability ────────────────────────────────────
class ResourceStatus(models.TextChoices):
    ACTIVE   = 'ACTIVE',   _('Active — online and available')
    INACTIVE = 'INACTIVE', _('Inactive — offline or no heartbeat')
    BUSY     = 'BUSY',     _('Busy — at capacity')

# ── Document Pipeline ────────────────────────────────────────
class DocumentStatus(models.TextChoices):
    UPLOADED          = 'UPLOADED',          _('Uploaded')
    SPLITTING         = 'SPLITTING',         _('Splitting')
    ASSIGNED          = 'ASSIGNED',          _('Assigned')
    IN_PROGRESS       = 'IN_PROGRESS',       _('In Progress')
    REVIEWING         = 'REVIEWING',         _('Reviewing')
    COMPLETED         = 'COMPLETED',         _('Completed')
    CONVERTING        = 'CONVERTING',        _('Converting')
    READY             = 'READY',             _('Ready')
    FAILED            = 'FAILED',            _('Failed')
    FAILED_VALIDATION = 'FAILED_VALIDATION', _('Failed Validation')
    VALIDATION_FAILED = 'VALIDATION_FAILED', _('Validation Failed')

class PipelineStatus(models.TextChoices):
    UPLOADED        = 'UPLOADED',        _('Uploaded')
    CONVERTING      = 'CONVERTING',      _('Converting to PDF')
    SPLITTING       = 'SPLITTING',       _('Splitting Pages')
    VALIDATING      = 'VALIDATING',      _('Validating Pages')
    SCORING         = 'SCORING',         _('Scoring Complexity')
    READY_TO_ASSIGN = 'READY_TO_ASSIGN', _('Ready for Assignment')
    ASSIGNING       = 'ASSIGNING',       _('Assigning to Resources')
    IN_PROGRESS     = 'IN_PROGRESS',     _('Processing in Progress')
    ALL_SUBMITTED   = 'ALL_SUBMITTED',   _('All Pages Submitted')
    MERGING         = 'MERGING',         _('Merging Pages')
    MERGED          = 'MERGED',          _('Merge Complete')
    COMPLETED       = 'COMPLETED',       _('Pipeline Completed')
    APPROVED        = 'APPROVED',        _('Approved by Admin')
    REJECTED        = 'REJECTED',        _('Rejected')
    FAILED          = 'FAILED',          _('Pipeline Failed')

class DocumentPriority(models.TextChoices):
    LOW    = 'LOW',    _('Low')
    NORMAL = 'NORMAL', _('Normal')
    HIGH   = 'HIGH',   _('High')
    URGENT = 'URGENT', _('Urgent')

# ── Conversion ───────────────────────────────────────────────
class ConversionStatus(models.TextChoices):
    NOT_REQUIRED      = 'NOT_REQUIRED',      _('Not Required')
    PENDING           = 'PENDING',           _('Pending')
    CONVERTING        = 'CONVERTING',        _('Converting')
    CONVERTED         = 'CONVERTED',         _('Converted')
    CONVERSION_FAILED = 'CONVERSION_FAILED', _('Conversion Failed')

class OriginalFormat(models.TextChoices):
    SCANNED_PDF    = 'SCANNED_PDF',    _('Scanned PDF')
    READABLE_PDF   = 'READABLE_PDF',   _('Readable PDF')
    WORD_DOC       = 'WORD_DOC',       _('Word Document')
    CONVERTED_WORD = 'CONVERTED_WORD', _('Converted Word')

# ── Page Status ──────────────────────────────────────────────
class PageStatus(models.TextChoices):
    PENDING              = 'PENDING',              _('Pending')
    ASSIGNED             = 'ASSIGNED',             _('Assigned')
    IN_PROGRESS          = 'IN_PROGRESS',          _('In Progress')
    COMPLETED            = 'COMPLETED',            _('Completed')
    FAILED               = 'FAILED',               _('Failed')
    IMPROPERLY_PROCESSED = 'IMPROPERLY_PROCESSED', _('Improperly Processed')
    UNASSIGNED           = 'UNASSIGNED',           _('Unassigned')
    SUBMITTED            = 'SUBMITTED',            _('Submitted')
    APPROVED             = 'APPROVED',             _('Approved')
    REJECTED             = 'REJECTED',             _('Rejected')
    TIMED_OUT            = 'TIMED_OUT',            _('Timed Out')
    ESCALATED            = 'ESCALATED',            _('Escalated')
    REASSIGNED           = 'REASSIGNED',           _('Reassigned')

# ── Validation ───────────────────────────────────────────────
class ValidationStatus(models.TextChoices):
    PENDING_VALIDATION     = 'PENDING_VALIDATION',     _('Pending Validation')
    VALIDATION_IN_PROGRESS = 'VALIDATION_IN_PROGRESS', _('Validation In Progress')
    VALIDATED              = 'VALIDATED',              _('Validated')
    VALIDATION_FAILED      = 'VALIDATION_FAILED',      _('Validation Failed')
    NEEDS_REUPLOAD         = 'NEEDS_REUPLOAD',         _('Needs Re-upload')

# ── Page Assignment (Block-based — legacy compatible) ────────
class AssignmentStatus(models.TextChoices):
    WAITING    = 'WAITING',    _('Waiting')
    PROCESSING = 'PROCESSING', _('Processing')
    COMPLETED  = 'COMPLETED',  _('Completed')
    EXPIRED    = 'EXPIRED',    _('Expired')
    REVOKED    = 'REVOKED',    _('Revoked')
    REJECTED   = 'REJECTED',   _('Rejected')

# ── PageAssignment (per-page — new spec) ─────────────────────
class PageAssignmentStatus(models.TextChoices):
    PENDING        = 'PENDING',        _('Pending')
    ASSIGNED       = 'ASSIGNED',       _('Assigned')
    IN_PROGRESS    = 'IN_PROGRESS',    _('In Progress')
    SUBMITTED      = 'SUBMITTED',      _('Submitted')
    APPROVED       = 'APPROVED',       _('Approved')
    QUALITY_FAILED = 'QUALITY_FAILED', _('Quality Failed')
    REASSIGNED     = 'REASSIGNED',     _('Reassigned')
    TIMED_OUT      = 'TIMED_OUT',      _('Timed Out')
    UNASSIGNED     = 'UNASSIGNED',     _('Unassigned')
    ESCALATED      = 'ESCALATED',      _('Escalated')

# ── Review ───────────────────────────────────────────────────
class ReviewStatus(models.TextChoices):
    PENDING_REVIEW = 'PENDING_REVIEW', _('Pending Admin Review')
    UNDER_REVIEW   = 'UNDER_REVIEW',   _('Under Review')
    APPROVED       = 'APPROVED',       _('Approved')
    REJECTED       = 'REJECTED',       _('Rejected')

# ── Reassignment ─────────────────────────────────────────────
class ReassignStatus(models.TextChoices):
    IN_QUEUE    = 'IN_QUEUE',    _('In Reassignment Queue')
    REASSIGNING = 'REASSIGNING', _('Being Reassigned')
    REASSIGNED  = 'REASSIGNED',  _('Reassigned Successfully')
    ESCALATED   = 'ESCALATED',   _('Escalated — Max Attempts')
    CANCELLED   = 'CANCELLED',   _('Cancelled')

class RejectionReason(models.TextChoices):
    QUALITY_FAIL     = 'QUALITY_FAIL',     _('Quality Below Standard')
    INCOMPLETE       = 'INCOMPLETE',       _('Incomplete Processing')
    INCORRECT_DATA   = 'INCORRECT_DATA',   _('Incorrect Data Entry')
    FORMATTING_ERROR = 'FORMATTING_ERROR', _('Formatting Errors')
    MISSING_CONTENT  = 'MISSING_CONTENT',  _('Missing Content')
    OCR_ERROR        = 'OCR_ERROR',        _('OCR Error Unresolved')
    MANUAL_OVERRIDE  = 'MANUAL_OVERRIDE',  _('Admin Manual Rejection')
    TIMEOUT          = 'TIMEOUT',          _('Processing Timeout')

# ── Merge / Approval ─────────────────────────────────────────
class MergeStatus(models.TextChoices):
    PENDING     = 'PENDING',     _('Merge Pending')
    IN_PROGRESS = 'IN_PROGRESS', _('Merge In Progress')
    COMPLETED   = 'COMPLETED',   _('Merge Completed')
    FAILED      = 'FAILED',      _('Merge Failed')
    APPROVED    = 'APPROVED',    _('Approved by Admin')
    DELIVERED   = 'DELIVERED',   _('Delivered to Client')

class ApprovalStatus(models.TextChoices):
    APPROVED  = 'APPROVED',  _('Approved')
    DELIVERED = 'DELIVERED', _('Delivered to Client')
    ARCHIVED  = 'ARCHIVED',  _('Archived')
    REVOKED   = 'REVOKED',   _('Approval Revoked')

# ── Audit ────────────────────────────────────────────────────
class AuditEventType(models.TextChoices):
    ASSIGNED          = 'ASSIGNED',          _('Page Assigned')
    COMPLETED         = 'COMPLETED',         _('Page Completed')
    EXPIRED           = 'EXPIRED',           _('Assignment Expired')
    REVOKED           = 'REVOKED',           _('Assignment Revoked')
    REASSIGNED        = 'REASSIGNED',        _('Page Reassigned')
    RESOURCE_CHANGE   = 'RESOURCE_CHANGE',   _('Resource Status Change')
    DOC_COMPLETED     = 'DOC_COMPLETED',     _('Document Completed')
    DOC_REJECTED      = 'DOC_REJECTED',      _('Document Rejected')
    DOC_DOWNLOADED    = 'DOC_DOWNLOADED',    _('Document Downloaded')
    DOC_UPLOADED      = 'DOC_UPLOADED',      _('Document Uploaded')
    DOC_SPLIT         = 'DOC_SPLIT',         _('Document Split')
    ASSIGNMENT_REVOKED = 'ASSIGNMENT_REVOKED', _('Assignment Revoked')
    TIMEOUT           = 'TIMEOUT',           _('Assignment Timed Out')
    APPROVED          = 'APPROVED',          _('Assignment Approved')

class QueueStatus(models.TextChoices):
    WAITING   = 'WAITING',   _('Waiting')
    ASSIGNED  = 'ASSIGNED',  _('Assigned')
    COMPLETED = 'COMPLETED', _('Completed')

class ComplexityType(models.TextChoices):
    SIMPLE      = 'SIMPLE',      _('Simple (Paragraphs)')
    TABLE_HEAVY = 'TABLE_HEAVY', _('Table-Heavy')
    COMPLEX     = 'COMPLEX',     _('Complex (Mixed/Dense)')
