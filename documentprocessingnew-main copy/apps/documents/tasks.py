from celery import shared_task
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from apps.documents.services import DocumentService
import hashlib
import logging
import os

logger = logging.getLogger(__name__)

def broadcast_conversion_status(document_id, event_type, payload):
    """
    Send real-time conversion status to the frontend via WebSocket.
    Channel group: 'conversion_{document_id}'
    """
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        f'conversion_{document_id}',
        {
            'type': event_type,        # maps to consumer method
            'payload': payload,
        }
    )

@shared_task(
    bind=True, 
    autoretry_for=(Exception,), 
    max_retries=3, 
    retry_backoff=True,
    retry_backoff_max=60,
    name='convert_word_to_pdf'
)
def convert_word_to_pdf(self, document_id):
    """
    Async task to convert DOC/DOCX to PDF.
    Uses LibreOffice headless as preferred method, docx2pdf as fallback.
    """
    import subprocess
    from pathlib import Path
    from django.utils.timezone import now
    from django.conf import settings
    from apps.documents.models import Document
    from common.enums import DocumentStatus, ConversionStatus, OriginalFormat
    from django.core.files import File

    try:
        doc = Document.objects.get(pk=document_id)
        doc.conversion_status = ConversionStatus.CONVERTING
        doc.conversion_started_at = now()
        doc.save(update_fields=['conversion_status', 'conversion_started_at'])

        broadcast_conversion_status(document_id, 'conversion.started', {
            'document_id': document_id,
            'doc_ref': doc.doc_ref,
            'filename': doc.name,
            'progress': 0,
            'stage': 'STARTED',
            'message': 'Starting conversion process...',
        })

        input_path = Path(doc.original_file.path)
        output_dir = input_path.parent
        output_name = input_path.stem + '.pdf'
        output_path = output_dir / output_name

        # ── Step 2: Reading file (20%) ───────────────────────
        file_size_mb = input_path.stat().st_size / (1024 * 1024)
        broadcast_conversion_status(document_id, 'conversion.progress', {
            'document_id': document_id,
            'progress': 20,
            'stage': 'READING',
            'message': f'Reading document ({file_size_mb:.1f} MB)...',
        })

        conversion_method = None
        
        # ── Step 3: Converting (40%) ─────────────────────────
        broadcast_conversion_status(document_id, 'conversion.progress', {
            'document_id': document_id,
            'progress': 40,
            'stage': 'CONVERTING',
            'message': 'Converting document to PDF, please wait...',
        })

        # SEARCH FOR LIBREOFFICE SOFFICE.EXE
        soffice_path = 'libreoffice' # Default to PATH
        if os.name == 'nt':
            common_paths = [
                r"C:\Program Files\LibreOffice\program\soffice.exe",
                r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
                os.path.join(os.environ.get('LOCALAPPDATA', ''), r"Programs\LibreOffice\program\soffice.exe"),
                os.path.join(os.environ.get('PROGRAMFILES', 'C:\Program Files'), r"LibreOffice\program\soffice.exe"),
            ]
            for p in common_paths:
                if os.path.exists(p):
                    soffice_path = f'"{p}"'
                    break

        # METHOD 1: LibreOffice headless
        try:
            # Add timeout and better error capture
            result = subprocess.run(
                f'{soffice_path} --headless --convert-to pdf --outdir "{output_dir}" "{input_path}"',
                shell=True,
                capture_output=True,
                timeout=120,
                check=False,
                encoding='utf-8',
                errors='replace'
            )
            
            if result.returncode == 0 and output_path.exists():
                conversion_method = 'LIBREOFFICE'
            else:
                lo_error = result.stderr or result.stdout or "Process exited with non-zero code"
                logger.warning(f"LibreOffice conversion failed. Error: {lo_error}")
                raise Exception(f"LibreOffice failed: {lo_error}")
        except Exception as e1:
            logger.warning(f"LibreOffice failed: {e1}. Trying docx2pdf...")
            # METHOD 2: docx2pdf fallback
            try:
                # Initialize COM for Windows
                import pythoncom
                pythoncom.CoInitialize()
                from docx2pdf import convert
                convert(str(input_path), str(output_path))
                if output_path.exists():
                    conversion_method = 'DOCX2PDF'
                else:
                    raise Exception("docx2pdf produced no output file")
            except Exception as e2:
                logger.warning(f"docx2pdf failed: {e2}. Trying aspose-words (final fallback)...")
                # METHOD 3: aspose-words (Final resort, works without Office/LibreOffice)
                try:
                    import aspose.words as aw
                    # Aspose.Words can direct load and save to PDF
                    doc_aw = aw.Document(str(input_path))
                    doc_aw.save(str(output_path))
                    if output_path.exists():
                        conversion_method = 'ASPOSE'
                    else:
                        raise Exception("aspose-words produced no output file")
                except Exception as e3:
                    logger.error(f"Aspose conversion failed: {e3}")
                    raise Exception(f"All conversion methods failed. LO: {e1}, Word: {e2}, Aspose: {e3}")
            finally:
                try:
                    import pythoncom
                    pythoncom.CoUninitialize()
                except: pass

        # ── Step 4: Validating output (70%) ──────────────────
        broadcast_conversion_status(document_id, 'conversion.progress', {
            'document_id': document_id,
            'progress': 70,
            'stage': 'VALIDATING',
            'message': 'Validating converted PDF...',
        })

        # Validate the output PDF is readable
        import fitz
        page_count = 0
        try:
            pdf_doc = fitz.open(str(output_path))
            page_count = pdf_doc.page_count
            pdf_doc.close()
            if page_count == 0:
                raise Exception("Converted PDF has 0 pages — conversion failed")
        except Exception as e:
            raise Exception(f"PDF validation failed: {str(e)}")

        # ── Step 5: Saving (90%) ─────────────────────────────
        broadcast_conversion_status(document_id, 'conversion.progress', {
            'document_id': document_id,
            'progress': 90,
            'stage': 'SAVING',
            'message': 'Saving converted file...',
        })

        # Generate MD5 hash for integrity
        md5 = hashlib.md5()
        with open(str(output_path), 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                md5.update(chunk)

        # ✅ Save converted file reference
        with open(output_path, 'rb') as f:
            pdf_file = File(f)
            doc.converted_pdf.save(output_name, pdf_file, save=False)
            doc.file.save(output_name, pdf_file, save=False)

        doc.conversion_status = ConversionStatus.CONVERTED
        doc.conversion_completed_at = now()
        doc.original_format = OriginalFormat.CONVERTED_WORD
        doc.status = DocumentStatus.READY
        doc.conversion_method = conversion_method
        doc.converted_file_size_bytes = output_path.stat().st_size
        doc.converted_file_hash_md5 = md5.hexdigest()
        doc.total_pages = page_count
        doc.save()

        # ── Step 6: Completed (100%) ─────────────────────────
        broadcast_conversion_status(document_id, 'conversion.completed', {
            'document_id': document_id,
            'doc_ref': doc.doc_ref,
            'progress': 100,
            'stage': 'COMPLETED',
            'message': 'Conversion completed successfully.',
            'page_count': page_count,
            'conversion_method': conversion_method,
            'file_size_mb': round(output_path.stat().st_size / (1024*1024), 2),
        })

        # Cleanup local PDF if it was a temporary out-of-storage file 
        # (Though here it's in the media dir, we keep it via doc.file.save)

        # ✅ Continue pipeline with splitting
        split_document_task.delay(document_id)

    except Document.DoesNotExist:
        logger.warning(f"Document {document_id} not found.")
    except Exception as e:
        logger.error(f"Conversion failed for document {document_id}: {e}")
        doc = Document.objects.filter(pk=document_id).first()
        stage_if_failed = 'STARTED' # Default failure stage
        if doc:
            doc.conversion_status = ConversionStatus.CONVERSION_FAILED
            doc.conversion_error = str(e)
            doc.status = DocumentStatus.FAILED
            doc.save()

            # Map current pipeline status or logic to a stage for the UI
            # If it failed during conversion, it's likely after 'STARTED'
            stage_if_failed = 'CONVERTING'
            
            # Broadcast failure
            broadcast_conversion_status(document_id, 'conversion.failed', {
                'document_id': document_id,
                'doc_ref': doc.doc_ref,
                'progress': 0,
                'stage': stage_if_failed,
                'message': 'Document conversion failed. Please try again.',
                'error': str(e),
                'retry_count': self.request.retries,
                'max_retries': self.max_retries,
            })
        
        # Avoid bubbling up retry exception if ALWAYS_EAGER is on (prevents 500 in view)
        if getattr(settings, 'CELERY_TASK_ALWAYS_EAGER', False):
            return False
            
        raise self.retry(exc=e)

@shared_task(name='cancel_document_tasks')
def cancel_document_tasks(document_id):
    """
    Cancels or revokes all pending tasks related to this document.
    """
    from django.conf import settings
    # In a real production app, we would use flower/celery inspect to find 
    # and revoke tasks using their IDs if we stored them in a task_id map.
    # For now, we signal to any running tasks via the is_deleted flag.
    logger.info(f"Cleanup: Document {document_id} marked for deletion. Tasks will be ignored.")
    return True

@shared_task(bind=True, max_retries=3)
def split_document_task(self, document_id):
    """
    Async task to split a document.
    """
    from apps.documents.models import Document
    from common.enums import DocumentStatus
    try:
        DocumentService.split_document(document_id)
    except Exception as exc:
        logger.error(f"Splitting failed for document {document_id}: {exc}")
        
        # Ensure FAILED status persists outside of any internal split_document transactions
        doc_qs = Document.objects.filter(id=document_id)
        if doc_qs.exists():
            doc_qs.update(
                status=DocumentStatus.FAILED,
                pipeline_error=str(exc)
            )
        
        raise self.retry(exc=exc, countdown=60)
