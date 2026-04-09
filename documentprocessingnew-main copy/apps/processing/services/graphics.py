import fitz
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class LineDetector:
    """
    Detects and classifies horizontal graphical lines from PDF vector data.
    """
    
    @staticmethod
    def detect_lines(page: fitz.Page) -> List[Dict[str, Any]]:
        """
        Extracts horizontal lines from the page's drawings.
        """
        lines = []
        try:
            drawings = page.get_drawings()
            for i, d in enumerate(drawings):
                rect = d.get('rect')
                if not rect:
                    continue
                
                x0, y0, x1, y1 = rect
                width = x1 - x0
                height = y1 - y0
                
                # Horizontal Line Heuristics
                if height < 3.5 and width > 15:
                    # Check for double lines (often two drawings very close)
                    # For now, classify basic single line
                    line_type = "line"
                    
                    # If very thin, might be an underline
                    if height < 1.0:
                        line_type = "underline"
                        
                    lines.append({
                        "id": f"line_{i}",
                        "type": line_type,
                        "bbox": [x0, y0, x1, y1],
                        "x": x0, "y": y0,
                        "width": width, "height": height,
                        "color": LineDetector._rgb_to_hex(d.get('color', (0,0,0)))
                    })
        except Exception as e:
            logger.error(f"Drawings extraction failed: {e}")
            
        return lines

    @staticmethod
    def _rgb_to_hex(rgb: tuple) -> str:
        if not rgb or len(rgb) < 3:
            return "#000000"
        r, g, b = [int(v * 255) if isinstance(v, float) else v for v in rgb]
        return f"#{r:02x}{g:02x}{b:02x}"
