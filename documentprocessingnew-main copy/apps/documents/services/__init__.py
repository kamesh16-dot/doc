import os
import pypdf
import logging
import io
from pathlib import Path
from django.core.files.base import ContentFile
from django.db import transaction
from django.conf import settings
from apps.documents.models import Document, Page
from common.enums import DocumentStatus, PageStatus
from common.utils import get_upload_path
from django.utils.timezone import now

logger = logging.getLogger(__name__)

class DocumentService:
    @staticmethod
    def create_document(client, file_obj):
        """
        Creates a new Document entry.
        """
        import os
        filename = file_obj.name
        ext = os.path.splitext(filename)[1].lower()
        from common.enums import OriginalFormat, ConversionStatus, DocumentStatus
        
        if ext == '.pdf':
            original_format = OriginalFormat.READABLE_PDF
            conversion_status = ConversionStatus.NOT_REQUIRED
        else:
            original_format = OriginalFormat.WORD_DOC
            conversion_status = ConversionStatus.PENDING

        doc = Document.objects.create(
            client=client,
            name=filename,
            original_file=file_obj,
            status=DocumentStatus.UPLOADED,
            original_format=original_format,
            conversion_status=conversion_status
        )
        logger.debug(f"Document record created: {doc.id}")
        
        # If PDF, we can use it directly for processing
        if ext == '.pdf':
            logger.debug("PDF detected. Setting file and READY status.")
            # Reuse the already saved stream to avoid redundant disk write
            doc.file = doc.original_file
            doc.status = DocumentStatus.READY
            doc.save()

        from apps.audit.models import AuditLog
        from common.enums import AuditEventType
        
        logger.debug("Creating AuditLog entry")
        AuditLog.objects.create(
            action=AuditEventType.DOC_UPLOADED,
            document_id=doc.id,
            actor=client,
            metadata={'filename': filename, 'extension': ext}
        )
        logger.debug("AuditLog created")
        return doc

    @staticmethod
    def split_document(document_id):
        """
        Splits a PDF document into individual pages.
        IDEMPOTENT: If pages already exist, skips creation.
        Uses fitz (PyMuPDF) for more reliable PDF handling.
        """
        import fitz
        from pathlib import Path
        from apps.processing.models import DocumentQueue
        from common.enums import QueueStatus, DocumentStatus, PipelineStatus
        from django.utils.timezone import now
        from django.core.files import File

        try:
            document = Document.objects.get(id=document_id)
            
            # Use converted_pdf if available, otherwise original_file
            pdf_file_field = document.file if document.file else document.original_file
            if not pdf_file_field:
                raise FileNotFoundError(f"Processing file not found for document {document_id}")

            # Open PDF using context manager on the stream for better portability
            pdf_data = pdf_file_field.read()
            pdf = fitz.open(stream=pdf_data, filetype="pdf")
            if pdf.is_encrypted:
                pdf.close()
                raise ValueError(f"Document {document_id} is encrypted/password-protected and cannot be split.")
                
            total = pdf.page_count
            document.total_pages = total
            document.pipeline_status = PipelineStatus.SPLITTING
            document.status = DocumentStatus.SPLITTING
            document.save(update_fields=['total_pages', 'pipeline_status', 'status'])

            errors = []
            pages_to_dispatch = []
            
            # Shared directory and timestamp for this batch
            current_date_path = now().strftime('%Y/%m/%d')
            base_out_dir = Path(settings.MEDIA_ROOT) / 'pages' / 'splits' / current_date_path / str(document_id)
            base_out_dir.mkdir(parents=True, exist_ok=True)

            from concurrent.futures import ThreadPoolExecutor
            
            # Pre-fetch existing pages
            existing_pages = {p.page_number: p for p in Page.objects.filter(document=document)}
            
            pages_to_create = []
            pages_to_update = []
            save_jobs = []


            # 1. Extract pages in parallel (CPU/Memory bound)
            # pdf_data is already read at line 88
            with ThreadPoolExecutor(max_workers=min(4, total)) as executor:
                # Update prepare_page to take data instead of Global path
                # For brevity in this refactor, I'll define a local helper
                def prepare_page_stream(i, data):
                    page_num = i + 1
                    try:
                        local_pdf = fitz.open(stream=data, filetype="pdf")
                        single_pdf = fitz.open()
                        single_pdf.insert_pdf(local_pdf, from_page=i, to_page=i)
                        
                        buffer = io.BytesIO()
                        single_pdf.save(buffer)
                        single_pdf.close()
                        local_pdf.close()
                    
                        filename = f'page_{page_num:04d}.pdf'
                        content = ContentFile(buffer.getvalue(), name=filename)

                        if page_num in existing_pages:
                            page = existing_pages[page_num]
                            page.status = PageStatus.PENDING
                            page.is_locked = False
                            page.text_content = ""
                            # Cleanup old dependencies
                            from apps.documents.models import Block, PageTable
                            Block.objects.filter(page=page).delete()
                            PageTable.objects.filter(page=page).delete()
                        else:
                            page = Page(
                                document=document, page_number=page_num,
                                status=PageStatus.PENDING, is_locked=False
                            )
                        return (page, filename, content)
                    except Exception as e:
                        logger.error(f"Error extracting page {page_num}: {e}")
                        return None

                results = list(executor.map(prepare_page_stream, range(total), [pdf_data]*total))

            # 2. Save file content to storage in parallel (I/O bound)
            def save_to_storage(result):
                if not result: return None
                page, filename, content = result
                # Save the page with a document-specific name to prevent collisions 
                # (e.g. pages/splits/2026/03/21/DOC_ID/page_0001.pdf)
                # Django's FileField.save handles prepending the upload_to path.
                unique_filename = f"{document_id}/{filename}" # filename already contains page_num
                page.content_file.save(unique_filename, content, save=False)
                page.status = PageStatus.PENDING
                return page

            with ThreadPoolExecutor(max_workers=8) as executor:
                final_pages = list(executor.map(save_to_storage, results))

            # 3. Batch DB operations (Atomic)
            with transaction.atomic():
                for page in final_pages:
                    if not page: continue
                    if page.id: # Existing
                        pages_to_update.append(page)
                    else:
                        # SQLite bulk_create doesn't return IDs, so save individually for new pages
                        page.save()
                        pages_to_dispatch.append(page.id)

                if pages_to_update:
                    # Clean up old assignments
                    from apps.processing.models import PageAssignment
                    from common.enums import PageAssignmentStatus
                    PageAssignment.objects.filter(
                        page__in=pages_to_update,
                        status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
                    ).update(status=PageAssignmentStatus.REASSIGNED)

                    Page.objects.bulk_update(pages_to_update, [
                        'content_file', 'status', 'is_locked', 
                        'current_assignee', 'split_error', 
                        'layout_data', 'text_content'
                    ])
                    pages_to_dispatch.extend([p.id for p in pages_to_update])

            pdf.close()

            # ✅ Final verification using the global Integrity Service
            from apps.processing.services.core import AssignmentService
            is_valid, integrity_msg = AssignmentService.validate_document_integrity(document)
            
            if not is_valid:
                document.pipeline_error = integrity_msg
                document.status = DocumentStatus.FAILED
                document.pipeline_status = PipelineStatus.FAILED
                document.save(update_fields=['pipeline_error', 'pipeline_status', 'status'])
                logger.error(f"Post-Split Integrity Verification Failed for {document_id}: {integrity_msg}")
                return

            # ✅ Section 3: Robust Pipeline Dispatch
            from apps.processing.tasks import (
                process_page_ocr_task, 
                mark_document_ready_to_assign
            )
            
            if getattr(settings, 'CELERY_TASK_ALWAYS_EAGER', False):
                logger.info(f"Running pipeline synchronously (parallel) for document {document_id}")
                from concurrent.futures import ThreadPoolExecutor
                from django import db
                with ThreadPoolExecutor(max_workers=min(8, len(pages_to_dispatch) or 1)) as executor:
                    def run_ocr(pid):
                        try:
                            process_page_ocr_task.delay(pid)
                        except Exception as e:
                            logger.error(f"OCR failed for page {pid}: {e}")
                        finally:
                            # CRITICAL: Close DB connections in temporary threads to avoid leaks/locks
                            db.connections.close_all()
                    
                    list(executor.map(run_ocr, pages_to_dispatch))
                
                mark_document_ready_to_assign.delay(document.id)
            else:
                from celery import chain, group
                logger.info(f"Dispatching async pipeline for document {document_id}")
                pipeline = chain(
                    group(process_page_ocr_task.s(pid) for pid in pages_to_dispatch),
                    mark_document_ready_to_assign.si(document.id)
                )
                pipeline.apply_async()
            
            logger.info(f"Pipeline processing initiated for document {document_id} with {total} pages.")

        except Exception as e:
            logger.exception(f"Critical failure in split_document for {document_id}: {e}")
            Document.objects.filter(id=document_id).update(
                status=DocumentStatus.FAILED, 
                pipeline_status=PipelineStatus.FAILED,
                pipeline_error=f"Split Error: {str(e)}"
            )
            raise e


from .final_docx import FinalDocxMergeError, FinalDocxService
