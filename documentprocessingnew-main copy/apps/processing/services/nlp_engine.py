import logging
import spacy
import re
from django.conf import settings

logger = logging.getLogger(__name__)

class NLPInspector:
    _nlp = None

    @classmethod
    def get_nlp(cls):
        # LIGHTWEIGHT MODE: Default to None if not already loaded to prevent request hangs
        # In a real production setup, we'd pre-load this or use a separate microservice
        if not getattr(settings, 'NLP_ENABLED', True):
            return None
            
        if cls._nlp is None:
            try:
                # Only attempt load if we aren't in a super-constrained environment
                cls._nlp = spacy.load("en_core_web_sm")
            except Exception as e:
                logger.warning(f"spaCy model not available: {e}. Falling back to heuristics.")
                return None
        return cls._nlp

    @classmethod
    def analyze_page_structure(cls, page_obj):
        """
        ENTERPRISE LAYER 2: NLP Structure Detection.
        Identifies Titles, Headings, and Body text.
        """
        nlp = cls.get_nlp()
        suggestions = []
        
        layout = page_obj.layout_data
        if not layout: return []

        for block in layout.get("blocks", []):
            text = block.get("text", "")
            if not text: continue
            
            # Use NLP if available, otherwise just heuristics
            doc = nlp(text[:500]) if nlp else None
            
            # 1. Structure Detection
            if block.get("type") == "paragraph":
                # Heuristic: Short blocks at top are likely titles/headings
                is_heading = False
                if len(text) < 100 and block["bbox"][1] < 200:
                    is_heading = True
                elif doc and any(token.pos_ == "PROPN" for token in doc) and len(text) < 200:
                    # NLP-aided heuristic: Proper nouns in short text blocks
                    is_heading = True

                if is_heading:
                    suggestions.append({
                        "id": f"struct_{block['bbox'][1]}",
                        "type": "structure_hint",
                        "label": "Potential Heading",
                        "bbox": block["bbox"]
                    })

        # 2. Table Semantic Validation
        for table in layout.get("tables", []):
            suggestions.extend(cls._validate_table_semantics(table))

        return suggestions

    @classmethod
    def _validate_table_semantics(cls, table):
        """ Detects numeric inconsistencies and format anomalies """
        suggestions = []
        cells = table.get("cells", [])
        
        # Heuristic: Check for currency symbols and ensure numeric columns
        for cell in cells:
            text = cell.get("text", "")
            if "₹" in text or "$" in text:
                # Suggest currency normalization
                suggestions.append({
                    "id": f"sem_{cell['row_index']}_{cell['col_index']}",
                    "type": "format_warning",
                    "label": "Currency detected - ensure decimals consistent",
                    "row": cell["row_index"],
                    "col": cell["col_index"]
                })
        
        # TODO: Implement cross-row sum validation (Phase 3)
        return suggestions

    @classmethod
    def check_grammar(cls, text):
        """ Basic language enhancement stub """
        # Real-time transformer-based check would happen here
        return []
