import logging
from typing import List, Dict, Any, Tuple
import json

logger = logging.getLogger(__name__)

class LayoutReconstructor:
    """
    High-fidelity layout reconstruction engine.
    Transforms raw OCR/Native blocks into logical structures (Paragraphs, Columns).
    """

    # Grouping constants (in PDF points)
    Y_CLUSTER_TOLERANCE = 3.0
    COL_GAP_THRESHOLD = 15.0
    PARA_GAP_THRESHOLD_RATIO = 1.5 # relative to font size

    def __init__(self, page_width: float, page_height: float):
        self.page_width = page_width
        self.page_height = page_height
        # Approximate margins (could be dynamically detected)
        self.margin_left = page_width * 0.05
        self.margin_right = page_width * 0.95

    def reconstruct(self, blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Main entry point for layout reconstruction.
        """
        if not blocks:
            return []

        # 1. Pre-process: Ensure standard fields
        for b in blocks:
            if 'bbox' not in b:
                # Fallback from x,y,w,h
                x, y = b.get('x', 0), b.get('y', 0)
                w, h = b.get('width', 0), b.get('height', 0)
                b['bbox'] = [x, y, x + w, y + h]
            
            # Ensure coordinates are floats
            b['bbox'] = [float(v) for v in b['bbox']]

        # 2. Sort by Y (reading order primary) then X
        # We use a small tolerance for Y to group lines into rows
        blocks.sort(key=lambda b: (round(b['bbox'][1] / self.Y_CLUSTER_TOLERANCE) * self.Y_CLUSTER_TOLERANCE, b['bbox'][0]))

        # 3. Logical Grouping: Merge lines into paragraphs
        reconstructed_blocks = []
        if not blocks:
            return []

        current_para = blocks[0].copy()
        
        for i in range(1, len(blocks)):
            next_block = blocks[i]
            
            if self._should_merge(current_para, next_block):
                current_para = self._merge_blocks(current_para, next_block)
            else:
                reconstructed_blocks.append(self._finalize_block(current_para))
                current_para = next_block.copy()
        
        reconstructed_blocks.append(self._finalize_block(current_para))

        # 4. Global Refinement: Column detection & Z-Indexing
        # (Reserved for complex multi-column logic)
        
        return reconstructed_blocks

    def _should_merge(self, b1: Dict[str, Any], b2: Dict[str, Any]) -> bool:
        """
        Determines if two blocks should be merged into one logical paragraph.
        """
        # Don't merge different types
        if b1.get('type') != b2.get('type'):
            return False
            
        # Don't merge if they are table cells
        if b1.get('type') == 'table_cell' or b1.get('block_type') == 'table_cell':
            return False

        r1 = b1['bbox']
        r2 = b2['bbox']
        
        # Vertical gap
        gap = r2[1] - r1[3]
        font_size = b1.get('font_size', 10.0)
        
        # If they overlap significantly in X and gap is small
        x_overlap = min(r1[2], r2[2]) - max(r1[0], r2[0])
        x_union = max(r1[2], r2[2]) - min(r1[0], r2[0])
        
        if x_overlap > 0 and gap < (font_size * self.PARA_GAP_THRESHOLD_RATIO):
            # Check for column split (if they are very far apart horizontally despite Y being close)
            # but since we sorted by Y then X, next_block is usually the next line or next item on same line.
            return True
            
        return False

    def _merge_blocks(self, b1: Dict[str, Any], b2: Dict[str, Any]) -> Dict[str, Any]:
        """
        Combines metadata and text from two blocks.
        """
        # Update Bbox to encompass both
        r1, r2 = b1['bbox'], b2['bbox']
        b1['bbox'] = [
            min(r1[0], r2[0]),
            min(r1[1], r2[1]),
            max(r1[2], r2[2]),
            max(r1[3], r2[3])
        ]
        
        # Merge text
        t1, t2 = b1.get('text', ''), b2.get('text', '')
        if t1 and t2:
            # Detect if we need a space (simple heuristic)
            b1['text'] = t1 + " " + t2
        elif t2:
            b1['text'] = t2
            
        # Update dimensions from new bbox
        b1['x'], b1['y'] = b1['bbox'][0], b1['bbox'][1]
        b1['width'] = b1['bbox'][2] - b1['bbox'][0]
        b1['height'] = b1['bbox'][3] - b1['bbox'][1]
        
        return b1

    def _finalize_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """
        Assigns semantic roles, alignment, and z-index.
        """
        # Detect Alignment
        block['alignment'] = self._detect_alignment(block)
        
        # Default Z-Index
        block['z_index'] = block.get('z_index', 0)
        
        # Line Height Calculation
        if 'font_size' in block and 'height' in block:
             # Basic estimate: height of block / number of suspected lines
             # For merged paras, height/font_size gives approximate lines
             block['line_height'] = 1.15 # Standard default
             
        return block

    def _detect_alignment(self, block: Dict[str, Any]) -> str:
        """
        Detects text alignment relative to page width.
        """
        if block.get('type') == 'image':
            return 'center'
            
        bbox = block['bbox']
        center_x = (bbox[0] + bbox[2]) / 2
        page_center = self.page_width / 2
        
        # Tolerance for centering
        center_tol = self.page_width * 0.1
        
        if abs(center_x - page_center) < center_tol and block['width'] < (self.page_width * 0.8):
            return 'center'
        
        # Check for right alignment
        if bbox[2] > (self.page_width * 0.85) and bbox[0] > (self.page_width * 0.4):
            return 'right'
            
        return 'left'

class NormalizationService:
    """
    Standardizes OCR results from various providers.
    """
    @staticmethod
    def normalize_layout(layout_data: Dict[str, Any]) -> Dict[str, Any]:
        page_dims = layout_data.get('page_dims', {"width": 595.0, "height": 842.0})
        reconstructor = LayoutReconstructor(page_dims['width'], page_dims['height'])
        
        raw_blocks = layout_data.get('blocks', [])
        normalized_blocks = reconstructor.reconstruct(raw_blocks)
        
        layout_data['blocks'] = normalized_blocks
        return layout_data
