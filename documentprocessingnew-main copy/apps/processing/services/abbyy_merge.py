import logging
import os
import time
import tempfile
from django.conf import settings
from django.core.files import File
from apps.processing.services.ocr import AbbyyClient
from apps.processing.services.pdf_baking import PDFBakeService
from common.enums import MergeStatus, DocumentStatus, PipelineStatus

logger = logging.getLogger(__name__)

class AbbyyMergeService:
    @staticmethod
    def merge_and_export(document, export_format="pdfSearchable"):
        """
        Uses ABBYY Cloud OCR SDK to merge approved pages and export in requested format.
        Workflow: submitImage (for each page) -> processDocument -> download result.
        """
        from apps.processing.models import SubmittedPage, MergedDocument
        from common.enums import ReviewStatus
        
        client = AbbyyClient()
        task_id = None
        
        approved_pages = SubmittedPage.objects.filter(
            document=document, 
            review_status=ReviewStatus.APPROVED
        ).order_by('page_number', '-submitted_at', '-id').distinct('page_number')
        
        total_pages = document.total_pages
        if approved_pages.count() != total_pages:
            raise ValueError(f"Incomplete approval: {approved_pages.count()}/{total_pages}")

        try:
            # 1. Submit each baked page
            for sub in approved_pages:
                logger.info(f"Submitting page {sub.page_number} to ABBYY...")
                
                # Bake the latest edits
                baked_content = PDFBakeService.bake_page_edits(sub.page)
                
                with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
                    tmp.write(baked_content)
                    tmp_path = tmp.name
                
                try:
                    # Collect pages into a single ABBYY task
                    task_id = client.submit_image(tmp_path, task_id=task_id)
                finally:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)

            # 2. Start Processing
            logger.info(f"Triggering processDocument for task {task_id} with format {export_format}")
            client.process_document(task_id, export_format=export_format)
            
            # 3. Polling
            result_url = None
            for _ in range(120): # Longer timeout for full docs
                time.sleep(2)
                status_info = client.get_task_status(task_id)
                status = status_info.get('status')
                logger.info(f"ABBYY Task {task_id} Status: {status}")
                
                if status == 'Completed':
                    result_url = status_info.get('resultUrl')
                    break
                elif status == 'Failed':
                    raise Exception(f"ABBYY Task {task_id} failed: {status_info.get('error')}")

            if not result_url:
                raise Exception("ABBYY Document processing timed out")

            # 4. Download and Save
            result_content = client.download_result(result_url)
            # result_content is likely bytes if it's PDF/DOCX
            if isinstance(result_content, str):
                result_content = result_content.encode('utf-8', errors='ignore')

            # Update Document models (logic similar to MergeService)
            from django.core.files.base import ContentFile
            filename = f"abbyy_export_{document.doc_ref}.{export_format.lower().replace('searchable', '')}.pdf"
            if export_format == 'xlsx': filename = filename.replace('.pdf', '.xlsx')
            elif export_format == 'docx': filename = filename.replace('.pdf', '.docx')

            document.final_file.save(filename, ContentFile(result_content), save=False)
            document.pipeline_status = PipelineStatus.MERGED
            document.status = DocumentStatus.COMPLETED
            document.save()
            
            # Clean up ABBYY
            client.delete_task(task_id)
            
            return True

        except Exception as e:
            logger.error(f"ABBYY Merge/Export failed: {e}")
            document.pipeline_status = PipelineStatus.FAILED
            document.pipeline_error = str(e)
            document.save()
            raise e
