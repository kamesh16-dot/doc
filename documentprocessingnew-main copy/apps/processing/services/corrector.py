import re
import logging

logger = logging.getLogger(__name__)

class TextCorrector:
    """
    Utility class for cleaning and normalizing OCR-extracted text.
    Used during DOCX and PDF export to ensure high-fidelity text representation.
    """

    @staticmethod
    def process_block(text: str) -> str:
        """
        Main entry point for block-level text cleaning.
        """
        if not text:
            return ""
        
        # 1. Normalize whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        
        # 2. Fix common OCR artifacts (e.g., '1 l' -> '11' if context suggests number)
        # Note: Keeping this minimal to avoid over-correcting valid text.
        
        # 3. Strip surrounding whitespace
        return text.strip()

    @staticmethod
    def clean_placeholders(text: str) -> str:
        """
        Removes technical placeholders that might have leaked into the text field.
        """
        _PLACEHOLDERS = ("[Line Area]", "[Image Area]", "[Table Cell]", "[New Table]")
        t = text.strip()
        for ph in _PLACEHOLDERS:
            if t == ph:
                return ""
        return t