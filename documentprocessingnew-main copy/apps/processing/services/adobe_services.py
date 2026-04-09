import os
import logging
import json
from django.conf import settings
from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
from adobe.pdfservices.operation.pdf_services import PDFServices
from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
from adobe.pdfservices.operation.io.cloud_asset import CloudAsset
from adobe.pdfservices.operation.operation_params.document_merge.document_merge_params import DocumentMergeParams
from adobe.pdfservices.operation.operation_params.document_merge.output_format import OutputFormat
from adobe.pdfservices.operation.pdf_jobs.jobs.document_merge_job import DocumentMergeJob
from adobe.pdfservices.operation.pdf_jobs.results.document_merge_pdf_result import DocumentMergePDFResult

logger = logging.getLogger(__name__)

class AdobeMergeService:
    """
    Handles dynamic PDF generation using Adobe Document Merge API.
    Uses Word (DOCX) templates and JSON data.
    """
    
    @staticmethod
    def generate_pdf_from_template(template_path, data_dict, output_path):
        try:
            # 1. Setup credentials
            credentials = ServicePrincipalCredentials(
                client_id=settings.ADOBE_CLIENT_ID,
                client_secret=settings.ADOBE_CLIENT_SECRET,
            )

            # 2. Initialize PDF Services
            pdf_services = PDFServices(credentials=credentials)

            # 3. Upload template
            with open(template_path, "rb") as f:
                input_stream = f.read()
            input_asset = pdf_services.upload(input_stream=input_stream, mime_type=PDFServicesMediaType.DOCX)

            # 4. Create Merge operation
            merge_params = DocumentMergeParams(
                json_data_for_merge=data_dict, 
                output_format=OutputFormat.PDF
            )
            merge_job = DocumentMergeJob(input_asset=input_asset, document_merge_params=merge_params)

            # 5. Submit and get result
            location = pdf_services.submit(merge_job)
            response = pdf_services.get_job_result(location, DocumentMergePDFResult)

            # 6. Save output
            result_asset = response.get_result().get_asset()
            stream_asset = pdf_services.get_content(result_asset)
            
            with open(output_path, "wb") as f:
                f.write(stream_asset.get_input_stream())
            
            return True
        except Exception as e:
            logger.error(f"Adobe Merge Error: {str(e)}")
            return False

class AdobeSealService:
    """
    Handles electronic sealing of PDFs using Adobe PDF Electronic Seal API.
    """
    # Placeholder implementation based on user's research example
    @staticmethod
    def apply_seal(pdf_path, seal_image_path, provider_info, output_path):
        # Implementation would follow the example provided in the user's research (ElectronicSeal class)
        # This requires TSP provider credentials (access_token, credential_id, pin)
        pass
