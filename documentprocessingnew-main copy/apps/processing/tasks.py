import logging
from celery import shared_task
from django.utils import timezone
from datetime import timedelta
from django.db import transaction

from apps.processing.services.core import AssignmentService
from apps.processing.services.validator import PageValidator
from apps.processing.services.complexity import ComplexityScorer
from apps.documents.models import Document, Page
from apps.processing.models import PageAssignment
from apps.accounts.models import ResourceProfile
from common.enums import (
    PageStatus, PipelineStatus, PageAssignmentStatus,
    ResourceStatus, ValidationStatus, ComplexityType
)

logger = logging.getLogger(__name__)

# ── Section 1: Resource Heartbeat Expiry ───────────────────
@shared_task(name='tasks.mark_inactive_resources')
def mark_inactive_resources():
    """
    Runs every 60 seconds via Celery Beat.
    Marks resources as INACTIVE if no heartbeat for > 2 minutes (120s).
    """
    from django.utils import timezone
    from datetime import timedelta
    from apps.accounts.models import ResourceProfile
    from common.enums import ResourceStatus

    cutoff = timezone.now() - timedelta(seconds=120)

    gone_offline = ResourceProfile.objects.filter(
        status__in=[ResourceStatus.ACTIVE, ResourceStatus.BUSY],
        last_seen__lt=cutoff
    )

    count = gone_offline.count()
    if count > 0:
        # Before marking inactive, find their active assignments to revoke them
        from apps.processing.models import PageAssignment
        from common.enums import PageAssignmentStatus, PageStatus
        
        active_assignments = PageAssignment.objects.filter(
            resource__in=gone_offline,
            status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
        )
        
        revoked_count = 0
        for assignment in active_assignments:
            with transaction.atomic():
                # 1. Update assignment
                assignment.status = PageAssignmentStatus.REASSIGNED
                assignment.save(update_fields=['status'])
                
                # 2. Reset page for next person or Escalate
                page = assignment.page
                
                # MAX_REASSIGNMENTS = 3
                if (assignment.reassignment_count or 0) >= 3:
                    page.status = PageStatus.ESCALATED
                    page.validation_errors = (page.validation_errors or []) + ["Max reassignment attempts reached (3). Resource went offline during last attempt."]
                    logger.warning(f"Page {page.id} ESCALATED - resource went offline and max attempts reached.")
                else:
                    page.status = PageStatus.PENDING
                
                page.current_assignee = None
                page.is_locked = False
                page.save(update_fields=['status', 'current_assignee', 'is_locked', 'validation_errors'])
                
                # 3. Notify original resource (if they reappear briefly)
                try:
                    from apps.processing.consumers import send_timeout_notification
                    send_timeout_notification(assignment.resource.user.id, page.page_number)
                except: pass
                
                revoked_count += 1
        
        gone_offline.update(status=ResourceStatus.INACTIVE, is_available=False)
        logger.info(f"Marked {count} resources INACTIVE. Revoked {revoked_count} active assignments.")
        
        if revoked_count > 0:
            assign_pages_task.delay()

    return {'marked_inactive': count, 'revoked_assignments': revoked_count}


# ── Section 2: Processing Time Limits ──────────────────────
@shared_task
def check_processing_timeouts():
    """
    Runs every 60 seconds.
    Checks for IN_PROGRESS assignments approaching or exceeding SLA.
    8 mins = send warning via WebSocket
    10 mins = revoke, reassign, mark TIMED_OUT
    """
    active_assignments = PageAssignment.objects.filter(
        status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
    ).select_related('resource__user', 'page')

    now = timezone.now()
    warnings_sent = 0
    timeouts_triggered = 0

    for assignment in active_assignments:
        # Calculate elapsed from start if available, otherwise from assignment time
        start_time = assignment.processing_start_at or assignment.assigned_at
        elapsed = (now - start_time).total_seconds()
        
        # 10 minute timeout (600s)
        if elapsed >= assignment.max_processing_time:
            with transaction.atomic():
                # 1. Update assignment
                assignment.status = PageAssignmentStatus.TIMED_OUT
                assignment.timed_out = True
                assignment.timed_out_at = now
                assignment.processing_end_at = now
                assignment.save()
                
                # 2. Update page status and CLEAR locks
                # Anti-Loop: Check reassignment count
                page = assignment.page
                
                # MAX_REASSIGNMENTS = 3
                if (assignment.reassignment_count or 0) >= 3:
                    page.status = PageStatus.ESCALATED
                    page.validation_errors = (page.validation_errors or []) + ["Max reassignment attempts reached (3). Likely problematic content."]
                    logger.warning(f"Page {page.id} ESCALATED after 3 failed assignments.")
                else:
                    page.status = PageStatus.PENDING # Re-queue
                
                page.current_assignee = None
                page.is_locked = False
                page.save(update_fields=['status', 'current_assignee', 'is_locked', 'validation_errors'])
                
                # 3. Update Resource stats
                prof = assignment.resource
                from django.db.models import F
                prof.rejection_count = F('rejection_count') + 1
                prof.save(update_fields=['rejection_count'])
                prof.refresh_status()
                
                # 4. Notify via WS
                from apps.processing.consumers import send_timeout_notification
                send_timeout_notification(assignment.resource.user.id, assignment.page.page_number)
                
                # 5. Audit Log for Timeout
                from apps.audit.models import AuditLog
                from common.enums import AuditEventType
                AuditLog.objects.create(
                    action=AuditEventType.TIMEOUT,
                    assignment_id=assignment.id,
                    document_id=assignment.document.id,
                    actor=None,
                    old_status=PageAssignmentStatus.IN_PROGRESS,
                    new_status=PageAssignmentStatus.TIMED_OUT,
                    metadata={
                        'reason': 'SLA Expired', 
                        'page_number': page.page_number,
                        'reassignment_count': assignment.reassignment_count
                    }
                )
                
            timeouts_triggered += 1

        # 8 minute warning (480s)
        elif elapsed >= (assignment.max_processing_time * 0.8) and not assignment.time_warning_sent:
            assignment.time_warning_sent = True
            assignment.save(update_fields=['time_warning_sent'])
            
            from apps.processing.consumers import send_time_warning
            send_time_warning(
                assignment.resource.user.id, 
                assignment.page.page_number, 
                assignment.max_processing_time - elapsed
            )
            warnings_sent += 1

    if timeouts_triggered > 0 or warnings_sent > 0:
        logger.info(f"Timeouts: {timeouts_triggered} revoked, {warnings_sent} warnings sent.")
    
    # If any timed out, trigger the assignment engine to pick up the dropped pages
    if timeouts_triggered > 0:
        assign_pages_task.delay()


@shared_task(bind=True, max_retries=3)
def process_page_ocr_task(self, page_id):
    """
    Runs OCR/Extraction via OCRService and then triggers validation.
    """
    from apps.processing.services.ocr import OCRService
    try:
        page = Page.objects.get(id=page_id)
        OCRService.process_page(page)
        # After OCR, trigger positional extraction
        extract_page_blocks_task.delay(page_id)
    except Page.DoesNotExist:
        logger.warning(f"Page {page_id} not found for OCR.")
    except Exception as exc:
        logger.error(f"OCR failed for page {page_id}: {exc}")
        raise self.retry(exc=exc, countdown=60)

@shared_task(bind=True, max_retries=3)
def extract_page_blocks_task(self, page_id):
    """
    Extracts positional blocks and tables using PDFLayoutEngine.
    """
    from apps.documents.models import Page, Block, PageTable, PageImage
    from apps.processing.services.layout_engine import PDFLayoutEngine
    import fitz
    import os
    from django.core.files import File
    from pathlib import Path
    from django.conf import settings
    from django.utils.timezone import now
    
    try:
        page = Page.objects.get(id=page_id)
        if not page.content_file:
            return
                
        # DocPro Enhancement: Use the 'Dynamic' layout data from OCRService if it existence (covers both Adobe and Native High-Fidelity)
        # Instead of only scanned docs, we now trust the OCR layout data globally to ensure consistency.
        if page.layout_data:
             logger.info(f"Using rich layout data for page {page.id} (Provider: {page.ocr_provider})")
             layout = page.layout_data
        else:
             # Final fallback for pages that somehow skipped the OCR task
             logger.info(f"Falling back to local layout engine for page {page.id}")
             engine = PDFLayoutEngine()
             layout = engine.extract_page_layout(page.content_file.path, page_index=0)

        with transaction.atomic():
            # Clear existing (except manually edited tables)
            Block.objects.filter(page=page).delete()
            PageTable.objects.filter(page=page, is_manually_edited=False).delete()
            PageImage.objects.filter(page=page).delete()
                
            # Update page dimensions
            page.pdf_page_width = layout.get('page_width', 0)
            page.pdf_page_height = layout.get('page_height', 0)
            page.blocks_extracted = True
            # Save blocks
            blocks_to_create = []
            for idx, blk in enumerate(layout['blocks']):
                # NORMALIZE SCHEMA: Handle both pdfplumber and OCRService (Adobe/Native) formats
                # OCRService usually returns 'bbox': [x0, y0, x1, y1]
                # pdfplumber returns 'x', 'y', 'width', 'height'
                
                if 'bbox' in blk and 'x' not in blk:
                    b = blk['bbox']
                    x, y = float(b[0]), float(b[1])
                    w, h = float(b[2]) - x, float(b[3]) - y
                else:
                    x, y = float(blk.get('x', 0)), float(blk.get('y', 0))
                    w, h = float(blk.get('width', 0)), float(blk.get('height', 0))

                # Re-calculate standard PDF bbox [x0, y0, x1, y1] for storage
                bbox = [x, y, x + w, y + h]
                
                b_type = blk.get('block_type') or blk.get('type') or 'text'
                b_id = blk.get('block_id') or f"b_{idx}"
                text = blk.get('text', '').strip()
                
                blocks_to_create.append(Block(
                    page=page,
                    block_index=idx,
                    block_id=b_id,
                    block_type=b_type,
                    semantic_role=blk.get('semantic_role', 'body_text'),
                    extracted_text=text,
                    original_text=text,
                    current_text=text,
                    x=x,
                    y=y,
                    width=w,
                    height=h,
                    bbox=bbox, 
                    font_name=blk.get('font_name') or blk.get('font_family', 'Helvetica'),
                    font_size=blk.get('font_size', 10.0),
                    font_weight=blk.get('font_weight', 'normal'),
                    font_style=blk.get('font_style', 'normal'),
                    font_color=blk.get('color', '#000000'),
                    z_index=blk.get('z_index', 0),
                    line_height=blk.get('line_height', 1.2),
                    table_id=blk.get('table_id') or '',
                    row_index=blk.get('row_index'),
                    col_index=blk.get('col_index'),
                    rowspan=blk.get('rowspan', 1),
                    colspan=blk.get('colspan', 1)
                ))
            
            if blocks_to_create:
                Block.objects.bulk_create(blocks_to_create)
            
            # Save tables (Skip if a manually edited table already covers this area)
            manually_edited_tables = list(PageTable.objects.filter(page=page, is_manually_edited=True))
            
            for t_idx, tbl in enumerate(layout['tables']):
                # NORMALIZE SCHEMA: Handle both pdfplumber and OCRService formats
                if 'bbox' in tbl and 'x' not in tbl:
                    b = tbl['bbox']
                    tx, ty = float(b[0]), float(b[1])
                    tw, th = float(b[2]) - tx, float(b[3]) - ty
                else:
                    tx, ty = float(tbl.get('x', 0)), float(tbl.get('y', 0))
                    tw, th = float(tbl.get('width', 0)), float(tbl.get('height', 0))

                t_bbox = [tx, ty, tx + tw, ty + th]
                t_id = tbl.get('table_id') or f"table_{page.page_number}_{t_idx}"
                t_rows = tbl.get('rows') or tbl.get('cells') or []
                
                # Spatial Matching Logic
                skip_this = False
                for m_tbl in manually_edited_tables:
                    m_bbox = [m_tbl.x, m_tbl.y, m_tbl.x + m_tbl.width, m_tbl.y + m_tbl.height]
                    
                    inter_x0 = max(t_bbox[0], m_bbox[0])
                    inter_y0 = max(t_bbox[1], m_bbox[1])
                    inter_x1 = min(t_bbox[2], m_bbox[2])
                    inter_y1 = min(t_bbox[3], m_bbox[3])
                    
                    if inter_x1 > inter_x0 and inter_y1 > inter_y0:
                        inter_area = (inter_x1 - inter_x0) * (inter_y1 - inter_y0)
                        t_area = tw * th
                        if t_area > 0 and (inter_area / t_area) > 0.8:
                            logger.info(f"Skipping auto-detected table {t_id} due to 80%+ overlap with manual table {m_tbl.table_ref}")
                            skip_this = True
                            break
                
                if skip_this:
                    continue

                # Group 1D cell list into 2D row grid if necessary 
                # (OCRService providers like Azure/Adobe return flat cell lists)
                if isinstance(t_rows, list) and len(t_rows) > 0 and isinstance(t_rows[0], dict):
                    # It's a flat list of cell objects with row_index/col_index
                    max_row = max((c.get('row_index', 0) for c in t_rows), default=0)
                    max_col = max((c.get('col_index', 0) for c in t_rows), default=0)
                    
                    # Create 2D grid [row][col]
                    grid = [[None for _ in range(max_col + 1)] for _ in range(max_row + 1)]
                    for cell in t_rows:
                        r, c = cell.get('row_index', 0), cell.get('col_index', 0)
                        if r <= max_row and c <= max_col:
                            grid[r][c] = cell
                    t_rows = grid

                PageTable.objects.create(
                    page=page,
                    table_ref=t_id,
                    x=tx,
                    y=ty,
                    width=tw,
                    height=th,
                    row_count=tbl.get('row_count', len(t_rows)),
                    col_count=tbl.get('col_count', len(t_rows[0]) if t_rows else 0),
                    table_json=t_rows,
                    col_widths=tbl.get('col_widths', []),
                    row_heights=tbl.get('row_heights', []),
                    col_aligns=tbl.get('col_aligns', []),
                    row_colors=tbl.get('row_colors', [])
                )
                
            # Save images
            if layout.get('images'):
                pdf_doc = fitz.open(page.content_file.path)
                try:
                    pdf_page = pdf_doc[0]
                    # Robust coordinate mapping: Handling CropBox and rotation
                    media_box = pdf_page.rect
                    
                    for img in layout['images']:
                        # Create the image record
                        instance = PageImage(
                            page=page,
                            image_ref=img['image_id'],
                            x=img['x'],
                            y=img['y'],
                            width=img['width'],
                            height=img['height']
                        )
                        
                        # Crop and save 
                        try:
                            # Use absolute coords based on media_box to avoid cropbox offsets
                            ix, iy = img['x'], img['y']
                            iw, ih = img['width'], img['height']
                            
                            # Standard Top-Left Rect
                            crop_rect = fitz.Rect(ix, iy, ix + iw, iy + ih)
                            
                            # Ensure rect is within bounds
                            if crop_rect.width > 0 and crop_rect.height > 0:
                                pix = pdf_page.get_pixmap(clip=crop_rect, matrix=fitz.Matrix(3, 3)) # 3x for higher quality
                                
                                out_dir = Path(settings.MEDIA_ROOT) / 'pages' / 'images' / now().strftime('%Y/%m/%d') / str(page.id)
                                out_dir.mkdir(parents=True, exist_ok=True)
                                img_filename = f"{img['image_id']}.png"
                                img_path = out_dir / img_filename
                                
                                pix.save(str(img_path))
                                
                                with open(img_path, 'rb') as f:
                                    instance.image_file.save(img_filename, File(f), save=False)
                        except Exception as e:
                            logger.error(f"Failed to crop image {img['image_id']}: {e}")
                        
                        instance.save()
                finally:
                    pdf_doc.close()
            
            # Update page
            p_w = layout.get('page_width') or layout.get('page_dims', {}).get('width', 0)
            p_h = layout.get('page_height') or layout.get('page_dims', {}).get('height', 0)
            
            page.pdf_page_width = p_w
            page.pdf_page_height = p_h
            page.blocks_extracted = True
            page.blocks_count = len(blocks_to_create)
            page.has_tables = layout.get('has_tables', len(layout.get('tables', [])) > 0)
            page.save()
            
        # Continue to validation
        validate_single_page.delay(page_id)
        
    except Page.DoesNotExist:
        logger.warning(f"Page {page_id} not found for extraction.")
    except Exception as exc:
        logger.error(f"Layout extraction failed for page {page_id}: {exc}")
        raise self.retry(exc=exc, countdown=60)

# ── Section 4 & 5: Validation and Complexity ───────────────
@shared_task(bind=True, max_retries=3)
def validate_single_page(self, page_id):
    """
    Validates a single page.
    """
    try:
        page = Page.objects.get(id=page_id)
    except Page.DoesNotExist:
        return

    # 1. Validate
    validator = PageValidator()
    result = validator.validate_page(page)
    
    if result.passed:
        page.validation_status = ValidationStatus.VALIDATED
        page.status = PageStatus.PENDING # Ready for assignment
    else:
        page.validation_status = ValidationStatus.VALIDATION_FAILED
        page.status = PageStatus.FAILED
        page.validation_errors = [c['message'] for c in result.checks if not c['passed']]
        
    page.save(update_fields=['validation_status', 'status', 'validation_errors'])
    
    # Trigger scoring independently (could be chained, but we want it done regardless)
    score_page_complexity.delay(page_id)

@shared_task(bind=True, max_retries=5)
def score_page_complexity(self, page_id):
    """
    Scores the complexity of a page.
    """
    try:
        with transaction.atomic():
            page = Page.objects.select_for_update().get(id=page_id)
            ComplexityScorer.apply_score(page)
    except Page.DoesNotExist:
        return
    except Exception as exc:
        logger.error(f"Scoring failed for page {page_id}: {exc}")
        # Apply safe default on ultimate failure
        if self.request.retries >= self.max_retries:
            Page.objects.filter(id=page_id).update(
                complexity_type=ComplexityType.SIMPLE,
                complexity_weight=1,
                complexity_scored_at=timezone.now()
            )
        raise self.retry(exc=exc, countdown=30)

@shared_task
def mark_document_ready_to_assign(document_id):
    """
    Final check before document hits the queue.
    Ensures ALL pages have weights.
    """
    from common.enums import PipelineStatus, DocumentStatus
    
    with transaction.atomic():
        doc = Document.objects.select_for_update().get(id=document_id)
        pages = doc.pages.all()
        
        # Guard: Ensure all pages have a weight
        missing_weights = pages.filter(complexity_weight__isnull=True)
        if missing_weights.exists():
            missing_weights.update(complexity_weight=1)
            
        doc.pipeline_status = PipelineStatus.READY_TO_ASSIGN
        doc.status = DocumentStatus.READY
        doc.save(update_fields=['pipeline_status', 'status'])
        
        # ✅ Add/Update Queue (Crucial for assignment engine)
        from apps.processing.models import DocumentQueue
        from common.enums import QueueStatus
        DocumentQueue.objects.update_or_create(
            document=doc,
            defaults={'status': QueueStatus.WAITING}
        )
        
        # ✅ DocPro Bundle Engine (Desktop Bridge Integration)
        from apps.desktop_bridge.services import create_bundles_for_document
        create_bundles_for_document(doc)

        # Trigger assignment
        from apps.processing.tasks import assign_pages_task
        assign_pages_task.delay()


@shared_task
def validate_all_pages(document_id):
    try:
        doc = Document.objects.get(id=document_id)
        doc.pipeline_status = PipelineStatus.VALIDATING
        doc.save(update_fields=['pipeline_status'])
        
        pages = doc.pages.all()
        for page in pages:
            validate_single_page.delay(page.id)
            
    except Document.DoesNotExist:
        pass


# Redundant convert_word_to_pdf removed. Use apps.documents.tasks.convert_word_to_pdf instead.


# ── Section 10: Merge Document ─────────────────────────────
@shared_task(bind=True, max_retries=5)
def merge_document_pages(self, document_id, admin_user_id=None):
    from apps.processing.services.merge import MergeService
    try:
        doc = Document.objects.get(id=document_id)
        doc.pipeline_status = PipelineStatus.MERGING
        doc.save(update_fields=['pipeline_status'])
        
        MergeService.merge_approved_pages(doc, admin_user_id)
        
        doc.pipeline_status = PipelineStatus.MERGED
        doc.save(update_fields=['pipeline_status'])
        
        # Notify Admin WS
        from apps.processing.consumers import broadcast_admin_update
        broadcast_admin_update({
            'type': 'document_merged',
            'doc_ref': doc.doc_ref,
            'title': doc.title
        })
        
    except Exception as exc:
        countdown = (2 ** self.request.retries) * 60
        raise self.retry(exc=exc, countdown=countdown)


# ── Existing Task Wrapped for New Spec ──────────────────────
@shared_task
def assign_pages_task():
    """
    Distributed assignment trigger.
    Processes the queue until empty or resources saturated.
    """
    total = 0
    try:
        # Safety break to prevent infinite loops in a single task
        for _ in range(100):
            assigned = AssignmentService.assign_pages()
            if assigned == 0:
                break
            total += assigned
        
        if total > 0:
            logger.info(f"Assignment task completed. {total} pages assigned.")
        return f"Assigned {total} pages total"
    except Exception as e:
        logger.error(f"Error in assign_pages_task: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

@shared_task
def resplit_missing_pages(document_id):
    """
    Identifies and re-splits only missing pages from the source PDF.
    """
    import fitz
    import os
    from apps.documents.models import Document, Page
    from common.enums import PageStatus
    from django.core.files import File
    from pathlib import Path
    from django.conf import settings
    from django.utils.timezone import now
    
    with transaction.atomic():
        doc = Document.objects.select_for_update().get(id=document_id)
        
        # Identify missing or broken pages
        db_pages = Page.objects.filter(document=doc).values_list('page_number', flat=True)
        missing_nums = [i for i in range(1, (doc.total_pages or 0) + 1) if i not in db_pages]
        
        for p in Page.objects.filter(document=doc):
             if not p.content_file or not os.path.exists(p.content_file.path):
                 missing_nums.append(p.page_number)
        
        if not missing_nums:
            return "No missing pages found."
            
        missing_nums = sorted(list(set(missing_nums)))
        
        pdf_file = doc.file if doc.file else doc.original_file
        pdf = fitz.open(pdf_file.path)
        
        new_page_ids = []
        for num in missing_nums:
            idx = num - 1
            if idx < 0 or idx >= pdf.page_count: continue
            
            single_pdf = fitz.open()
            single_pdf.insert_pdf(pdf, from_page=idx, to_page=idx)
            
            out_dir = Path(settings.MEDIA_ROOT) / 'pages' / 'splits' / now().strftime('%Y/%m/%d') / str(doc.id)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f'page_{num:04d}_repair.pdf'
            
            single_pdf.save(str(out_path))
            single_pdf.close()
            
            with open(out_path, 'rb') as f:
                django_file = File(f, name=out_path.name)
                page, _ = Page.objects.update_or_create(
                    document=doc,
                    page_number=num,
                    defaults={'content_file': django_file, 'status': PageStatus.PENDING}
                )
                new_page_ids.append(page.id)
        
        pdf.close()
        
        # Restart pipeline for fixed pages (OCR -> Extract -> Validate)
        for pid in new_page_ids:
            process_page_ocr_task.delay(pid)
            
        return f"Repaired {len(new_page_ids)} pages for document {document_id}"
