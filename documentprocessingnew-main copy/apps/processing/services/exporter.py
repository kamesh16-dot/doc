import io
import logging
from typing import Any, Dict, List, Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph, Table, TableStyle

from apps.documents.models import Block, Page, PageImage, PageTable
from apps.processing.services.corrector import TextCorrector

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants – must match those used in the editor and DOCX export
# ---------------------------------------------------------------------------
BASE_MARGIN_PT = 36.0          # 0.5 inch
DEFAULT_PAGE_W_PT = 595.0
DEFAULT_PAGE_H_PT = 842.0

HEADING_1_SIZE = 18
HEADING_2_SIZE = 14
HEADING_3_SIZE = 12

FONT_MAP = {
    "arial": "Helvetica",
    "helv": "Helvetica",
    "helvetica": "Helvetica",
    "times": "Times-Roman",
    "serif": "Times-Roman",
    "courier": "Courier",
    "mono": "Courier",
    "consolas": "Courier",
    "georgia": "Times-Roman",      # fallback
    "verdana": "Helvetica",
    "tahoma": "Helvetica",
    "trebuchet": "Helvetica",
    "comic": "Helvetica",
    "impact": "Helvetica-Bold",
    "symbol": "Symbol",
}


class PDFExporter:
    """
    High‑fidelity PDF generation using ReportLab.
    Translates Workspace elements (Blocks, Tables, Images) to PDF with
    precise positioning, fonts, and alignment.
    """

    def __init__(self):
        self._base_styles = getSampleStyleSheet()
        self._page_width = DEFAULT_PAGE_W_PT
        self._page_height = DEFAULT_PAGE_H_PT

    def generate(self, page: Page) -> io.BytesIO:
        """
        Generate a PDF for a single page.
        Returns an in‑memory bytes buffer.
        """
        buffer = io.BytesIO()

        # Use the page's stored dimensions or fallback to A4
        page_width = getattr(page, "pdf_page_width", None) or DEFAULT_PAGE_W_PT
        page_height = getattr(page, "pdf_page_height", None) or DEFAULT_PAGE_H_PT
        self._page_width = page_width
        self._page_height = page_height

        # Create canvas with the correct page size
        c = canvas.Canvas(buffer, pagesize=(page_width, page_height))

        # 1. Background color (if any)
        layout_data = getattr(page, "layout_data", {}) or {}
        page_meta = layout_data.get("page_meta", {})
        bg_color = page_meta.get("bg_color")
        if bg_color:
            c.setFillColor(colors.HexColor(bg_color))
            c.rect(0, 0, page_width, page_height, fill=1, stroke=0)

        # 2. Render all items in proper order (respect z-index)
        items = self._collect_items(page)
        items.sort(key=lambda it: (it["y"], it["z"], it["x"]))

        for item in items:
            if item["type"] == "block":
                self._draw_block(c, item["obj"])
            elif item["type"] == "image":
                self._draw_image(c, item["obj"])
            elif item["type"] == "manual_table":
                self._draw_manual_table(c, item["obj"])

        c.showPage()
        c.save()
        buffer.seek(0)
        return buffer

    # -----------------------------------------------------------------------
    # Item collection (mirrors the DOCX export logic)
    # -----------------------------------------------------------------------
    def _collect_items(self, page: Page) -> List[Dict[str, Any]]:
        """Collect all layout items from the page."""
        items = []

        # Blocks (text, lines)
        blocks = Block.objects.filter(page=page).order_by("y", "x")
        for b in blocks:
            items.append({"type": "block", "obj": b, "x": b.x, "y": b.y, "z": b.z_index or 0})

        # Images
        images = PageImage.objects.filter(page=page).order_by("y", "x")
        for img in images:
            items.append({"type": "image", "obj": img, "x": img.x, "y": img.y, "z": img.z_index or 1})

        # Manual tables (created in editor)
        tables = PageTable.objects.filter(page=page).order_by("y", "x")
        for tbl in tables:
            items.append({"type": "manual_table", "obj": tbl, "x": tbl.x, "y": tbl.y, "z": 0})

        return items

    # -----------------------------------------------------------------------
    # Block rendering (text, lines)
    # -----------------------------------------------------------------------
    def _draw_block(self, canvas_obj: canvas.Canvas, block: Block) -> None:
        """Draw a text block (or graphical line) with proper styling."""
        block_type = getattr(block, "block_type", None)

        # Graphical lines
        if block_type in ("line", "double_line"):
            self._draw_line(canvas_obj, block)
            return

        # Normal text block
        text = block.current_text or block.original_text or ""
        if not text.strip():
            return

        # Clean text (remove OCR placeholders)
        text = self._clean_text(text)

        # Build style from block attributes
        style = self._build_paragraph_style(block)

        # Create Paragraph and wrap it to the block's width and height
        p = Paragraph(text, style)
        w_pts = block.width or 10
        h_pts = block.height or 10

        # Determine where to place the paragraph – its top‑left corner
        # PDF origin is bottom‑left; our editor uses top‑left (y from top)
        x = block.x
        y = self._page_height - block.y  # top of block in PDF coordinates
        # The paragraph's bounding box will be placed with its top at y
        p.wrapOn(canvas_obj, w_pts, h_pts)
        p.drawOn(canvas_obj, x, y - h_pts)  # because wrapOn gives height, we need to shift up

    def _build_paragraph_style(self, block: Block) -> ParagraphStyle:
        """Create a ReportLab ParagraphStyle from block attributes."""
        # Base style (default)
        style_name = f"block_{id(block)}"
        font_name = self._map_font(block.font_name)
        font_size = block.font_size or 10
        font_color = block.font_color or "#000000"
        alignment = self._get_alignment(block)

        # Heading roles adjust size
        role = block.semantic_role or ""
        if role == "heading_1":
            font_size = max(font_size, HEADING_1_SIZE)
        elif role == "heading_2":
            font_size = max(font_size, HEADING_2_SIZE)
        elif role == "heading_3":
            font_size = max(font_size, HEADING_3_SIZE)

        style = ParagraphStyle(
            name=style_name,
            parent=self._base_styles["Normal"],
            fontName=font_name,
            fontSize=font_size,
            textColor=colors.HexColor(font_color),
            alignment=alignment,
            leading=font_size * 1.15,      # line spacing similar to DOCX
            spaceBefore=0,
            spaceAfter=0,
            leftIndent=0,
            rightIndent=0,
        )

        # Bold / Italic
        if block.font_weight == "bold" or role.startswith("heading"):
            style.fontName = font_name + "-Bold"
        if block.font_style == "italic" or role == "footnote":
            if style.fontName.endswith("-Bold"):
                style.fontName = font_name + "-BoldOblique"
            else:
                style.fontName = font_name + "-Oblique"

        # Underline – ReportLab doesn't have a direct property; we could use <u> tags in text
        if getattr(block, "is_underlined", False):
            # Wrap text with <u> tags – handled when creating the Paragraph
            # (we'll modify the text before passing)
            pass

        # Strikethrough – similar, use <strike> tag
        if getattr(block, "is_strikethrough", False):
            pass

        return style

    def _get_alignment(self, block: Block) -> int:
        """Return ReportLab alignment constant based on block x/width and page width."""
        if block.width <= 0:
            return TA_LEFT

        x = block.x
        w = block.width
        text_center = x + w / 2.0
        page_center = self._page_width / 2.0

        # Use same tolerances as DOCX export
        if abs(text_center - page_center) < 15.0:
            return TA_CENTER
        if abs(x + w - (self._page_width - BASE_MARGIN_PT)) < 20.0:
            return TA_RIGHT
        return TA_LEFT

    def _map_font(self, raw_name: str) -> str:
        """Map OCR font name to a ReportLab‑compatible font name."""
        if not raw_name:
            return "Helvetica"
        lower = raw_name.lower()
        for key, mapped in FONT_MAP.items():
            if key in lower:
                return mapped
        return "Helvetica"

    def _clean_text(self, text: str) -> str:
        """Same cleaning as in DOCX export (remove placeholders, extra spaces)."""
        if not text:
            return ""
        text = TextCorrector.process_block(text)
        text = re.sub(r"[ \t]+", " ", text)
        _PLACEHOLDERS = ("[Line Area]", "[Image Area]", "[Table Cell]", "[New Table]")
        for ph in _PLACEHOLDERS:
            if text.strip() == ph:
                return ""
        # Remove leading markers like ">", "•", "-", "*"
        text = re.sub(r"^[>•\-*]\s*", "", text.strip())
        return text

    def _draw_line(self, canvas_obj: canvas.Canvas, block: Block) -> None:
        """Draw a graphical line (single or double) as a rectangle with fill."""
        x = block.x
        y_top = self._page_height - block.y           # top of block in PDF
        y_bottom = y_top - block.height               # bottom
        w = block.width
        h = block.height

        # For a single line, we draw a filled rectangle
        # For a double line, we draw two thin rectangles
        block_type = block.block_type
        line_color = colors.HexColor(block.font_color or "#000000")
        canvas_obj.setFillColor(line_color)

        if block_type == "line":
            # Simple rectangle
            canvas_obj.rect(x, y_bottom, w, h, fill=1, stroke=0)
        elif block_type == "double_line":
            # Two thin rectangles – use half the height each, with a small gap
            half_h = h / 2.0
            gap = 0.5   # points
            canvas_obj.rect(x, y_bottom, w, half_h - gap, fill=1, stroke=0)
            canvas_obj.rect(x, y_bottom + half_h + gap, w, half_h - gap, fill=1, stroke=0)

    # -----------------------------------------------------------------------
    # Image rendering
    # -----------------------------------------------------------------------
    def _draw_image(self, canvas_obj: canvas.Canvas, img: PageImage) -> None:
        """Draw an image at its exact position and size."""
        if not img.image_file:
            return

        x = img.x
        y_top = self._page_height - img.y        # top of image in PDF
        y_bottom = y_top - img.height

        try:
            # ReportLab's drawImage expects the bottom-left corner
            canvas_obj.drawImage(
                img.image_file.path,
                x, y_bottom,
                width=img.width,
                height=img.height,
                preserveAspectRatio=True,
                mask='auto'
            )
        except Exception as e:
            logger.error(f"Failed to draw image {img.id}: {e}")

    # -----------------------------------------------------------------------
    # Manual table rendering (from PageTable)
    # -----------------------------------------------------------------------
    def _draw_manual_table(self, canvas_obj: canvas.Canvas, page_table: PageTable) -> None:
        """Render a table created via the editor."""
        table_json = page_table.table_json or []
        if not table_json:
            return

        # Determine number of rows and columns
        rows = len(table_json)
        cols = max(len(row) for row in table_json) if rows else 0

        if rows == 0 or cols == 0:
            return

        # Build data matrix from table_json (each cell may be dict with 'text' etc.)
        data = []
        for ri, row in enumerate(table_json):
            data_row = []
            for ci in range(cols):
                cell = row[ci] if ci < len(row) else {}
                if isinstance(cell, dict):
                    text = cell.get("text", "")
                else:
                    text = str(cell)
                data_row.append(text)
            data.append(data_row)

        # Column widths: either from stored percentages or equal distribution
        col_widths_pct = getattr(page_table, "col_widths", None) or []
        col_widths = []
        if col_widths_pct and len(col_widths_pct) == cols:
            total_pct = sum(col_widths_pct)
            if total_pct != 100 and total_pct > 0:
                col_widths_pct = [p * 100.0 / total_pct for p in col_widths_pct]
            for pct in col_widths_pct:
                col_widths.append(page_table.width * pct / 100.0)
        else:
            col_widths = [page_table.width / cols] * cols

        # Create ReportLab Table
        table = Table(data, colWidths=col_widths, rowHeights=None)

        # Table style: borders and alignment
        has_borders = getattr(page_table, "has_borders", True)
        style_commands = []
        if has_borders:
            style_commands.append(('GRID', (0,0), (-1,-1), 0.5, colors.grey))
        else:
            style_commands.append(('BOX', (0,0), (-1,-1), 0, colors.white))
            style_commands.append(('INNERGRID', (0,0), (-1,-1), 0, colors.white))

        # Vertical alignment: default to middle
        style_commands.append(('VALIGN', (0,0), (-1,-1), 'MIDDLE'))

        # Apply cell‑specific indentation and alignment from table_json
        for ri, row in enumerate(table_json):
            for ci, cell in enumerate(row):
                if isinstance(cell, dict):
                    indent = cell.get("indent", 0)
                    if indent:
                        # Left indent – ReportLab can only set left padding via cell style
                        # We'll use a custom cell style: left padding = indent
                        style_commands.append(('LEFTPADDING', (ci, ri), (ci, ri), indent))
                    # Text alignment (if stored)
                    align = cell.get("align", "left")
                    if align == "center":
                        style_commands.append(('ALIGN', (ci, ri), (ci, ri), 'CENTER'))
                    elif align == "right":
                        style_commands.append(('ALIGN', (ci, ri), (ci, ri), 'RIGHT'))
                    # Bold? Underline? Could be added but not needed for now

        table.setStyle(TableStyle(style_commands))

        # Position the table on the page
        x = page_table.x
        y_top = self._page_height - page_table.y        # top of table in PDF
        y_bottom = y_top - page_table.height

        # Table's height may be auto‑calculated; we must wrap it to the given height
        table.wrapOn(canvas_obj, page_table.width, page_table.height)
        table.drawOn(canvas_obj, x, y_bottom)