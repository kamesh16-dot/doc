from django.core.files.storage import default_storage
from common.enums import PageStatus
import logging

logger = logging.getLogger(__name__)

class ValidationService:
    @classmethod
    def validate_page(cls, page):
        """
        Validates a single page's processing results.
        Returns (is_valid, errors_list)
        """
        errors = []
        
        # 1. Upload Success Check
        if not page.content_file:
            errors.append("Split page file is missing.")
        elif not default_storage.exists(page.content_file.name):
            errors.append("Split page file does not exist in storage.")
            
        # 2. Content Quality Check (Cleaning & Formatting)
        text = page.text_content or ""
        text = text.strip()
        
        if not text:
            errors.append("Extracted text is empty or only whitespace.")
        elif len(text) < 10:
            errors.append("Extracted text is suspiciously short (under 10 characters).")
            
        # 3. Structural/Formatting Check (e.g. basic HTML check if applicable)
        # For now, we'll just check for basic text presence.
        # If the text contains tags, we could check for balanced tags here.
        if "<" in text and ">" in text:
            # Simple check for balanced angle brackets as a proxy for HTML formatting
            if text.count("<") != text.count(">"):
                errors.append("Possible malformed formatting: unmatched tags detected.")

        is_valid = len(errors) == 0
        
        # Update page object
        page.is_validated = is_valid
        page.validation_errors = errors
        if not is_valid:
            page.status = PageStatus.IMPROPERLY_PROCESSED
            
        page.save(update_fields=['is_validated', 'validation_errors', 'status'])
        
        return is_valid, errors

    @classmethod
    def validate_document(cls, document):
        """
        Validates all pages in a document.
        Returns (is_valid, summary_of_errors)
        """
        all_valid = True
        doc_errors = {}
        
        for page in document.pages.all():
            is_valid, errors = cls.validate_page(page)
            if not is_valid:
                all_valid = False
                doc_errors[page.page_number] = errors
                
        return all_valid, doc_errors
