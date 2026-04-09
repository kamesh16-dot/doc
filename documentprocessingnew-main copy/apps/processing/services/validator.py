import os
import fitz  # PyMuPDF
from PIL import Image
import pytesseract
import numpy as np
import cv2
from django.conf import settings
from apps.documents.models import Page
from common.enums import ValidationStatus

class ValidationResult:
    def __init__(self, page_id, passed, checks):
        self.page_id = page_id
        self.passed = passed
        self.checks = checks  # List of dicts {check_name: "", passed: bool, message: ""}

    @property
    def checks_dict(self):
        return {c['check_name']: {'passed': c['passed'], 'message': c['message']} for c in self.checks}

class PageValidator:
    """
    Validation pipeline runs after upload, before assignment.
    Ensures pages are not corrupt, blank, or unreadable.
    """
    
    def validate_page(self, page: Page) -> ValidationResult:
        checks = []
        checks.append(self.check_uploaded(page))
        
        if checks[-1]['passed']:
            checks.append(self.check_not_corrupted(page))
            
            if checks[-1]['passed']:
                checks.append(self.check_text_extractable(page))
                checks.append(self.check_not_blank(page))
                
                if page.is_scanned:
                    checks.append(self.check_image_quality(page))
                    
        passed = all(c['passed'] for c in checks)
        return ValidationResult(page.id, passed, checks)

    def check_uploaded(self, page: Page) -> dict:
        try:
            path = page.content_file.path
            if os.path.exists(path) and os.path.getsize(path) > 0:
                return {'check_name': 'check_uploaded', 'passed': True, 'message': 'File exists and size > 0'}
        except ValueError:
            pass
        return {'check_name': 'check_uploaded', 'passed': False, 'message': 'File missing or empty'}

    def check_not_corrupted(self, page: Page) -> dict:
        try:
            doc = fitz.open(page.content_file.path)
            doc.close()
            return {'check_name': 'check_not_corrupted', 'passed': True, 'message': 'PDF opened successfully'}
        except Exception as e:
            return {'check_name': 'check_not_corrupted', 'passed': False, 'message': f'Corruption error: {str(e)}'}

    def check_text_extractable(self, page: Page) -> dict:
        try:
            doc = fitz.open(page.content_file.path)
            fitz_page = doc[0]
            text = fitz_page.get_text()
            doc.close()
            
            if len(text.strip()) > 10:
                page.is_scanned = False
                page.save(update_fields=['is_scanned'])
                return {'check_name': 'check_text_extractable', 'passed': True, 'message': 'Native text extracted'}
                
            # If native text fails, assume scanned and check OCR confidence
            page.is_scanned = True
            page.save(update_fields=['is_scanned'])
            
            doc = fitz.open(page.content_file.path)
            pix = doc[0].get_pixmap()
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            osd = pytesseract.image_to_osd(img) # Simple check if Tesseract can read it
            doc.close()
            return {'check_name': 'check_text_extractable', 'passed': True, 'message': 'Fall back to OCR successful'}
            
        except Exception as e:
            page.is_scanned = True
            page.save(update_fields=['is_scanned'])
            return {'check_name': 'check_text_extractable', 'passed': False, 'message': f'Text extraction failed: {str(e)}'}

    def check_image_quality(self, page: Page) -> dict:
        try:
            doc = fitz.open(page.content_file.path)
            pix = doc[0].get_pixmap()
            doc.close()
            
            # Convert to OpenCV image
            img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
            gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
            
            # Calculate contrast
            std_dev = np.std(gray)
            if std_dev < 15: # Very low contrast (mostly one color)
                 return {'check_name': 'check_image_quality', 'passed': False, 'message': f'Contrast too low: std_dev={std_dev:.2f}'}
                 
            return {'check_name': 'check_image_quality', 'passed': True, 'message': 'Image contrast acceptable'}
        except Exception as e:
            return {'check_name': 'check_image_quality', 'passed': False, 'message': f'Image quality check failed: {str(e)}'}

    def check_not_blank(self, page: Page) -> dict:
        try:
            doc = fitz.open(page.content_file.path)
            pix = doc[0].get_pixmap()
            doc.close()
            
            img = np.frombuffer(pix.samples, dtype=np.uint8)
            # Check if image is almost entirely white (blank page)
            white_pixels = np.sum(img > 245)
            total_pixels = img.size
            white_ratio = white_pixels / total_pixels
            
            if white_ratio > 0.99:
                 return {'check_name': 'check_not_blank', 'passed': False, 'message': 'Page appears to be blank (>99% white)'}
                 
            return {'check_name': 'check_not_blank', 'passed': True, 'message': 'Page is not blank'}
        except Exception as e:
            return {'check_name': 'check_not_blank', 'passed': False, 'message': f'Blank check failed: {str(e)}'}
