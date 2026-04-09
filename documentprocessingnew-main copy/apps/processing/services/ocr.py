import fitz  # PyMuPDF
import cv2
import requests
import time
import base64
import numpy as np
import os
import tempfile
import json
import html
import copy
import logging
from django.conf import settings
from apps.processing.services.reconstructor import NormalizationService
from apps.processing.services.graphics import LineDetector

logger = logging.getLogger(__name__)


def _value_to_text(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _to_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _rgb_to_hex(color):
    """
    Accepts PyMuPDF color formats:
    - int (0xRRGGBB)
    - tuple/list floats in 0..1
    - tuple/list ints in 0..255
    """
    if color is None:
        return ""

    if isinstance(color, str):
        if color.startswith("#") and len(color) in {4, 7}:
            return color
        return ""

    if isinstance(color, (int, float)):
        raw = int(color)
        r = (raw >> 16) & 0xFF
        g = (raw >> 8) & 0xFF
        b = raw & 0xFF
        return f"#{r:02X}{g:02X}{b:02X}"

    if isinstance(color, (tuple, list)) and len(color) >= 3:
        vals = list(color[:3])
        if all(isinstance(v, float) and 0.0 <= v <= 1.0 for v in vals):
            vals = [int(round(v * 255)) for v in vals]
        vals = [max(0, min(255, int(v))) for v in vals]
        return f"#{vals[0]:02X}{vals[1]:02X}{vals[2]:02X}"

    return ""


def _is_bold_font(font_name="", flags=0):
    font_lower = (font_name or "").lower()
    if any(x in font_lower for x in ["bold", "bd", "black", "heavy", "w700", "w800", "w900"]):
        return True
    try:
        if int(flags) & 16:
            return True
    except Exception:
        pass
    return False


def _is_italic_font(font_name="", flags=0):
    font_lower = (font_name or "").lower()
    if any(x in font_lower for x in ["italic", "it", "oblique", "slant"]):
        return True
    try:
        if int(flags) & 2:
            return True
    except Exception:
        pass
    return False


def _bbox_to_rect(bbox):
    try:
        return fitz.Rect(bbox)
    except Exception:
        return None


def _rect_area(rect):
    if rect is None:
        return 0.0
    try:
        return max(0.0, float(rect.width) * float(rect.height))
    except Exception:
        return 0.0


def _overlap_ratio(a, b):
    ra = _bbox_to_rect(a)
    rb = _bbox_to_rect(b)
    if ra is None or rb is None:
        return 0.0
    inter = ra & rb
    inter_area = _rect_area(inter)
    if inter_area <= 0:
        return 0.0
    denom = min(_rect_area(ra), _rect_area(rb))
    if denom <= 0:
        return 0.0
    return inter_area / denom


def _is_mostly_white(fill_hex):
    if not fill_hex:
        return True
    try:
        fill_hex = fill_hex.lstrip("#")
        r = int(fill_hex[0:2], 16)
        g = int(fill_hex[2:4], 16)
        b = int(fill_hex[4:6], 16)
        return (r + g + b) >= 735
    except Exception:
        return False


def _collect_spans(page_dict):
    spans = []
    for block in page_dict.get("blocks", []):
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                bbox = span.get("bbox") or line.get("bbox") or block.get("bbox") or [0, 0, 0, 0]
                spans.append({
                    "bbox": bbox,
                    "text": span.get("text", ""),
                    "font": span.get("font", "Helvetica"),
                    "size": span.get("size", 10.0),
                    "flags": span.get("flags", 0),
                    "color": _rgb_to_hex(span.get("color", 0)),
                    "bold": _is_bold_font(span.get("font", ""), span.get("flags", 0)),
                    "italic": _is_italic_font(span.get("font", ""), span.get("flags", 0)),
                })
    return spans


def _spans_in_bbox(spans, bbox, threshold=0.15):
    matched = []
    for span in spans or []:
        ratio = _overlap_ratio(span.get("bbox"), bbox)
        if ratio >= threshold:
            matched.append(span)
    return matched


def _best_span_style(spans):
    """
    FIX: Properly pick dominant span style.
    Prioritises largest font-size, then longest text.
    Never returns a zero/None font_size — falls back to 10.0 only
    when no real size is available.
    """
    if not spans:
        return {
            "font_family": "Helvetica",
            "font_size": 10.0,
            "font_weight": "normal",
            "font_style": "normal",
            "color": "#000000",
            "bold": False,
            "italic": False,
        }

    # Filter out spans with no meaningful size
    sized = [s for s in spans if float(s.get("size") or 0) > 0]
    chosen = max(
        sized or spans,
        key=lambda s: (float(s.get("size") or 0), len(_value_to_text(s.get("text", "")).strip()))
    )

    font = chosen.get("font", "Helvetica") or "Helvetica"
    raw_size = chosen.get("size")
    # Guarantee a sensible default — never 0 or None
    size = float(raw_size) if raw_size and float(raw_size) > 0 else 10.0
    color = chosen.get("color") or "#000000"
    bold = any(s.get("bold") for s in spans) or _is_bold_font(font, chosen.get("flags", 0))
    italic = any(s.get("italic") for s in spans) or _is_italic_font(font, chosen.get("flags", 0))

    return {
        "font_family": font,
        "font_size": size,
        "font_weight": "bold" if bold else "normal",
        "font_style": "italic" if italic else "normal",
        "color": color,
        "bold": bold,
        "italic": italic,
    }


def _detect_fill_color_from_drawings(drawings, bbox):
    target = _bbox_to_rect(bbox)
    if target is None:
        return ""

    best = ""
    best_score = 0.0

    for drawing in drawings or []:
        fill = drawing.get("fill")
        if fill is None:
            continue
        fill_hex = _rgb_to_hex(fill)
        if _is_mostly_white(fill_hex):
            continue

        rect = drawing.get("rect")
        if rect is None:
            for item in drawing.get("items", []):
                if not item:
                    continue
                if item[0] == "re":
                    rect = item[1]
                    break

        if rect is None:
            continue

        try:
            rect = fitz.Rect(rect)
        except Exception:
            continue

        inter = rect & target
        if inter is None:
            continue

        inter_area = _rect_area(inter)
        if inter_area <= 0:
            continue

        score = inter_area / max(1.0, _rect_area(target))
        score *= float(drawing.get("fill_opacity", 1.0) or 1.0)

        if score > best_score:
            best_score = score
            best = fill_hex

    return best if best_score >= 0.35 else ""


def _row_bg_from_drawings(drawings, row_bbox):
    return _detect_fill_color_from_drawings(drawings, row_bbox)


def _text_style_for_bbox(spans, bbox):
    matched = _spans_in_bbox(spans, bbox, threshold=0.12)
    if not matched:
        return _best_span_style([])

    centered = []
    try:
        rect = fitz.Rect(bbox)
        for span in matched:
            srect = fitz.Rect(span.get("bbox"))
            if rect.intersects(srect):
                centered.append(span)
    except Exception:
        centered = matched

    return _best_span_style(centered or matched)


def _cell_style_from_context(page, drawings, spans, cell_bbox, row_bbox=None):
    style = _text_style_for_bbox(spans, cell_bbox)
    cell_bg = _detect_fill_color_from_drawings(drawings, cell_bbox)
    row_bg = _row_bg_from_drawings(drawings, row_bbox) if row_bbox else ""
    bg = cell_bg or row_bg

    return {
        "text_color": style.get("color", "#000000"),
        "background_color": bg,
        "font_family": style.get("font_family", "Helvetica"),
        "font_size": style.get("font_size", 10.0),
        "font_weight": style.get("font_weight", "normal"),
        "font_style": style.get("font_style", "normal"),
        "bold": style.get("bold", False),
        "italic": style.get("italic", False),
    }


def _table_rows_from_cells(cells, row_count=None, col_count=None):
    cells = cells or []
    if row_count is None:
        row_count = 0
        for cell in cells:
            row_index = cell.get("row_index")
            if isinstance(row_index, int) and row_index >= row_count:
                row_count = row_index + 1
    if col_count is None:
        col_count = 0
        for cell in cells:
            col_index = cell.get("col_index")
            if isinstance(col_index, int) and col_index >= col_count:
                col_count = col_index + 1

    if row_count <= 0 or col_count <= 0:
        return []

    grid = [[None for _ in range(col_count)] for _ in range(row_count)]
    for cell in cells:
        r = cell.get("row_index")
        c = cell.get("col_index")
        if not isinstance(r, int) or not isinstance(c, int):
            continue
        if r < 0 or c < 0 or r >= row_count or c >= col_count:
            continue
        grid[r][c] = {
            "text": _value_to_text(cell.get("text", "")),
            "row_index": r,
            "col_index": c,
            "bbox": cell.get("bbox", []),
            "rowspan": _to_int(cell.get("rowspan", 1), 1),
            "colspan": _to_int(cell.get("colspan", 1), 1),
            "indent": cell.get("indent", 0),
            "bold": bool(cell.get("bold", False)),
            "italic": bool(cell.get("italic", False)),
            "font_family": cell.get("font_family", ""),
            "font_size": cell.get("font_size", 10.0),
            "font_weight": cell.get("font_weight", "normal"),
            "font_style": cell.get("font_style", "normal"),
            "text_color": cell.get("text_color", "#000000"),
            "background_color": cell.get("background_color", ""),
            "spans": cell.get("spans", []),
        }

    for r in range(row_count):
        for c in range(col_count):
            if grid[r][c] is None:
                grid[r][c] = {
                    "text": "",
                    "row_index": r,
                    "col_index": c,
                    "bbox": [],
                    "rowspan": 1,
                    "colspan": 1,
                    "indent": 0,
                    "bold": False,
                    "italic": False,
                    "font_family": "",
                    "font_size": 10.0,
                    "font_weight": "normal",
                    "font_style": "normal",
                    "text_color": "#000000",
                    "background_color": "",
                    "spans": [],
                }
    return grid


def _normalize_table_rows(table):
    if not table:
        return []

    rows = table.get("rows") or table.get("table_json")
    if rows:
        normalized = []
        for r_idx, row in enumerate(rows):
            if not isinstance(row, list):
                row = [row]
            norm_row = []
            for c_idx, cell in enumerate(row):
                if isinstance(cell, dict):
                    norm_row.append({
                        "text": _value_to_text(cell.get("text", "")),
                        "row_index": cell.get("row_index", r_idx),
                        "col_index": cell.get("col_index", c_idx),
                        "bbox": cell.get("bbox", []),
                        "rowspan": _to_int(cell.get("rowspan", 1), 1),
                        "colspan": _to_int(cell.get("colspan", 1), 1),
                        "indent": cell.get("indent", 0),
                        "bold": bool(cell.get("bold", False)),
                        "italic": bool(cell.get("italic", False)),
                        "font_family": cell.get("font_family", ""),
                        "font_size": cell.get("font_size", 10.0),
                        "font_weight": cell.get("font_weight", "normal"),
                        "font_style": cell.get("font_style", "normal"),
                        "text_color": cell.get("text_color", "#000000"),
                        "background_color": cell.get("background_color", ""),
                        "spans": cell.get("spans", []),
                    })
                else:
                    norm_row.append({
                        "text": _value_to_text(cell),
                        "row_index": r_idx,
                        "col_index": c_idx,
                        "bbox": [],
                        "rowspan": 1,
                        "colspan": 1,
                        "indent": 0,
                        "bold": False,
                        "italic": False,
                        "font_family": "",
                        "font_size": 10.0,
                        "font_weight": "normal",
                        "font_style": "normal",
                        "text_color": "#000000",
                        "background_color": "",
                        "spans": [],
                    })
            normalized.append(norm_row)
        return normalized

    cells = table.get("cells") or []
    row_count = table.get("row_count")
    col_count = table.get("col_count")
    if not isinstance(row_count, int) or row_count <= 0:
        row_count = None
    if not isinstance(col_count, int) or col_count <= 0:
        col_count = None
    return _table_rows_from_cells(cells, row_count=row_count, col_count=col_count)


# ---------------------------------------------------------------------------
# FIX: _resolve_cell_font_size
# ---------------------------------------------------------------------------
def _resolve_cell_font_size(cell):
    """
    Return the best available font-size for a cell.
    Priority: spans list → cell-level font_size → fallback 10.0
    Never returns 0 or None.
    """
    spans = cell.get("spans") or []
    for span in spans:
        sz = span.get("font_size") or span.get("size")
        if sz and float(sz) > 0:
            return float(sz)

    cell_sz = cell.get("font_size")
    if cell_sz and float(cell_sz) > 0:
        return float(cell_sz)

    return 10.0


def _resolve_cell_font_family(cell):
    """Return best available font-family for a cell."""
    spans = cell.get("spans") or []
    for span in spans:
        ff = span.get("font_family") or span.get("font")
        if ff and ff.strip():
            return ff.strip()
    return cell.get("font_family") or "Helvetica"


def _resolve_cell_font_weight(cell):
    """Return best available font-weight for a cell."""
    spans = cell.get("spans") or []
    if any(s.get("bold") or s.get("font_weight") == "bold" for s in spans):
        return "bold"
    if cell.get("bold") or cell.get("font_weight") == "bold":
        return "bold"
    return "normal"


def _resolve_cell_font_style(cell):
    """Return best available font-style for a cell."""
    spans = cell.get("spans") or []
    if any(s.get("italic") or s.get("font_style") == "italic" for s in spans):
        return "italic"
    if cell.get("italic") or cell.get("font_style") == "italic":
        return "italic"
    return "normal"


def _resolve_cell_text_color(cell):
    """Return best available text colour for a cell."""
    spans = cell.get("spans") or []
    for span in spans:
        c = span.get("text_color") or span.get("color")
        if c and c != "#000000":
            return c
    return cell.get("text_color") or "#000000"


# ---------------------------------------------------------------------------
# FIX: _span_to_html — use resolved font properties; add contenteditable
# ---------------------------------------------------------------------------
def _span_to_html(span, fallback_bbox=None):
    span = span or {}
    text = html.escape(_value_to_text(span.get("text", "")))
    bbox = span.get("bbox") or fallback_bbox or []
    bbox_str = json.dumps(bbox)

    font_family = html.escape(_value_to_text(
        span.get("font_family") or span.get("font") or "Helvetica"
    ))
    raw_size = span.get("font_size") or span.get("size")
    font_size = float(raw_size) if raw_size and float(raw_size) > 0 else 10.0

    font_weight = span.get("font_weight", "normal") or "normal"
    font_style = span.get("font_style", "normal") or "normal"
    color = span.get("text_color") or span.get("color") or "#000000"

    return (
        f'<span class="ocr-span" data-bbox="{bbox_str}" '
        f'data-font-family="{font_family}" data-font-size="{font_size}" '
        f'data-font-weight="{font_weight}" data-font-style="{font_style}" '
        f'data-text-color="{color}" '
        f'style="font-family: {font_family}; font-size: {font_size}pt; '
        f'font-weight: {font_weight}; font-style: {font_style}; color: {color};">'
        f'{text}</span>'
    )


# ---------------------------------------------------------------------------
# FIX: _cell_inner_html — use resolved helpers so font size is always real
# ---------------------------------------------------------------------------
def _cell_inner_html(cell):
    spans = cell.get("spans") or []
    if spans:
        return "".join(_span_to_html(span, fallback_bbox=cell.get("bbox")) for span in spans)

    # Fallback: build a single span from cell-level properties
    text = _value_to_text(cell.get("text", ""))
    bbox_str = json.dumps(cell.get("bbox") or [])

    font_family = html.escape(_resolve_cell_font_family(cell))
    font_size   = _resolve_cell_font_size(cell)
    font_weight = _resolve_cell_font_weight(cell)
    font_style  = _resolve_cell_font_style(cell)
    text_color  = _resolve_cell_text_color(cell)

    return (
        f'<span class="ocr-span ocr-span-fallback" data-bbox="{bbox_str}" '
        f'data-font-family="{font_family}" data-font-size="{font_size}" '
        f'data-font-weight="{font_weight}" data-font-style="{font_style}" '
        f'data-text-color="{text_color}" '
        f'style="font-family: {font_family}; font-size: {font_size}pt; '
        f'font-weight: {font_weight}; font-style: {font_style}; color: {text_color};">'
        f'{html.escape(text)}</span>'
    )


# ---------------------------------------------------------------------------
# FIX: _table_to_html
#   • Uses _resolve_* helpers for every style property
#   • Adds contenteditable="true" to table-cell-inner div so the toolbar works
#   • Propagates all data-* attributes to both <td> and inner <div>
# ---------------------------------------------------------------------------
def _table_to_html(table):
    rows = _normalize_table_rows(table)
    if not rows:
        return ""

    table_bbox = table.get("bbox") or [
        table.get("x", 0),
        table.get("y", 0),
        table.get("x", 0) + table.get("width", 0),
        table.get("y", 0) + table.get("height", 0),
    ]
    table_bbox_str = json.dumps(table_bbox)
    col_widths  = table.get("col_widths", [])
    col_aligns  = table.get("col_aligns", [])
    row_colors  = table.get("row_colors", [])

    table_html = (
        f"<table class='pdf-table' data-bbox='{table_bbox_str}' "
        f"style=\"width: 100%; table-layout: fixed; border-collapse: collapse; "
        f"border: 1px solid #4a90e222; background-color: #f8fbff;\">"
    )
    if col_widths:
        table_html += '<colgroup>'
        for w in col_widths:
            table_html += f'<col style="width: {w}%;">'
        table_html += '</colgroup>'

    for r_idx, row in enumerate(rows):
        r_color = row_colors[r_idx] if r_idx < len(row_colors) else None
        r_style = f' style="background-color: {r_color};"' if r_color else ""
        table_html += f'<tr{r_style}>'

        for c_idx, cell in enumerate(row):
            if not cell:
                continue
            if cell.get("colspan") == 0 or cell.get("rowspan") == 0:
                continue

            cspan = _to_int(cell.get("colspan", 1), 1)
            rspan = _to_int(cell.get("rowspan", 1), 1)
            align  = col_aligns[c_idx] if c_idx < len(col_aligns) else "left"
            indent = cell.get("indent", 0)

            # ---- FIX: always resolve real values ----
            font_family = html.escape(_resolve_cell_font_family(cell))
            font_size   = _resolve_cell_font_size(cell)
            font_weight = _resolve_cell_font_weight(cell)
            font_style  = _resolve_cell_font_style(cell)
            text_color  = _resolve_cell_text_color(cell)
            bg          = cell.get("background_color", "")

            inner_html = _cell_inner_html(cell)
            bbox_str   = json.dumps(cell.get("bbox") or [])

            td_style = (
                f'text-align: {align}; padding: 4px 6px; '
                f'padding-left: {indent}pt; font-weight: {font_weight}; '
                f'font-style: {font_style}; color: {text_color}; '
                f'border: 1px solid #4a90e233; vertical-align: top; '
                f'font-family: {font_family}; font-size: {font_size}pt;'
            )
            if bg:
                td_style += f' background-color: {bg};'

            span_attr = ""
            if cspan > 1:
                span_attr += f' colspan="{cspan}"'
            if rspan > 1:
                span_attr += f' rowspan="{rspan}"'

            # ---- FIX: contenteditable on the inner div so toolbar binds ----
            table_html += (
                f'<td{span_attr} data-bbox="{bbox_str}" '
                f'data-font-family="{font_family}" '
                f'data-font-size="{font_size}" '
                f'data-font-weight="{font_weight}" '
                f'data-font-style="{font_style}" '
                f'data-text-color="{text_color}" '
                f'style="{td_style}">'
                f'<div class="table-cell-inner" '
                f'contenteditable="true" '
                f'data-bbox="{bbox_str}" '
                f'data-font-family="{font_family}" '
                f'data-font-size="{font_size}" '
                f'data-font-weight="{font_weight}" '
                f'data-font-style="{font_style}" '
                f'data-text-color="{text_color}" '
                f'style="outline: none; min-height: 1em; '
                f'font-family: {font_family}; font-size: {font_size}pt; '
                f'font-weight: {font_weight}; font-style: {font_style}; '
                f'color: {text_color};">'
                f'{inner_html}</div>'
                f'</td>'
            )

        table_html += '</tr>'
    table_html += '</table>'
    return table_html


def _enrich_layout_with_table_cell_blocks(layout):
    """
    Flatten table cells into regular layout blocks so editor tools can read
    font-size / font-weight / bbox / spans from table text too.
    Keeps original tables intact.
    """
    if not layout:
        return layout

    blocks = list(layout.get("blocks", []) or [])
    tables = list(layout.get("tables", []) or [])

    flat_blocks = []
    for t_idx, table in enumerate(tables):
        rows = table.get("rows") or _normalize_table_rows(table)
        row_colors = table.get("row_colors", []) or []
        table_bbox = table.get("bbox") or []

        for r_idx, row in enumerate(rows):
            for c_idx, cell in enumerate(row):
                if not cell:
                    continue
                text = _value_to_text(cell.get("text", "")).strip()
                bbox = cell.get("bbox") or []

                # FIX: use resolvers so enriched blocks also carry real values
                font_family = _resolve_cell_font_family(cell)
                font_size   = _resolve_cell_font_size(cell)
                font_weight = _resolve_cell_font_weight(cell)
                font_style  = _resolve_cell_font_style(cell)
                text_color  = _resolve_cell_text_color(cell)
                bg = cell.get("background_color") or (
                    row_colors[r_idx] if r_idx < len(row_colors) else ""
                )

                span = {
                    "bbox": bbox,
                    "text": text,
                    "font_family": font_family,
                    "font_size": font_size,
                    "font_weight": font_weight,
                    "font_style": font_style,
                    "text_color": text_color,
                    "bold": font_weight == "bold",
                    "italic": font_style == "italic",
                }

                flat_blocks.append({
                    "id": f"tbl_{t_idx}_r{r_idx}_c{c_idx}",
                    "type": "paragraph",
                    "block_type": "table_cell",
                    "bbox": bbox,
                    "text": text,
                    "font_family": font_family,
                    "font_size": font_size,
                    "font_weight": font_weight,
                    "font_style": font_style,
                    "color": text_color,
                    "background_color": bg,
                    "table_index": t_idx,
                    "row_index": r_idx,
                    "col_index": c_idx,
                    "table_bbox": table_bbox,
                    "spans": [span],
                })

    if flat_blocks:
        blocks.extend(flat_blocks)
    layout["blocks"] = blocks
    return layout


ABBYY_APP_ID  = getattr(settings, 'OCR_APP_ID',   'DocPro_OCR')
ABBYY_PASSWORD = getattr(settings, 'OCR_PASSWORD', 'XXXX-XXXX-XXXX-XXXX-XXXX')
ABBYY_BASE_URL = getattr(settings, 'OCR_BASE_URL', 'https://cloud.ocrsdk.com/v2/')

AZURE_ENDPOINT = getattr(settings, 'AZURE_OCR_ENDPOINT', 'https://{your-endpoint}.cognitiveservices.azure.com/')
AZURE_KEY      = getattr(settings, 'AZURE_OCR_KEY', None)


class AbbyyClient:
    def __init__(self, app_id=ABBYY_APP_ID, password=ABBYY_PASSWORD):
        self.app_id = app_id
        self.password = password
        self.auth_header = f"Basic {base64.b64encode(f'{app_id}:{password}'.encode()).decode()}"

    def process_image(self, file_path, language="English", export_format="xml"):
        base = ABBYY_BASE_URL.rstrip('/')
        url  = f"{base}/processImage?language={language}&exportFormat={export_format}"
        with open(file_path, 'rb') as f:
            resp = requests.post(url, headers={"Authorization": self.auth_header},
                                 files={'file': f}, timeout=60)
        if resp.status_code != 200:
            raise Exception(f"ABBYY Error: {resp.text}")
        data = resp.json()
        return data.get('taskId')

    def get_task_status(self, task_id):
        base = ABBYY_BASE_URL.rstrip('/')
        url  = f"{base}/getTaskStatus?taskId={task_id}"
        resp = requests.get(url, headers={"Authorization": self.auth_header}, timeout=60)
        if resp.status_code != 200:
            raise Exception(f"ABBYY Status Error: {resp.text}")
        return resp.json()

    def download_result(self, result_url):
        resp = requests.get(result_url, timeout=60)
        if resp.status_code != 200:
            raise Exception("Failed to download result")
        return resp.content

    def delete_task(self, task_id):
        base = ABBYY_BASE_URL.rstrip('/')
        url  = f"{base}/deleteTask?taskId={task_id}"
        requests.get(url, headers={"Authorization": self.auth_header}, timeout=60)


class AzureOCRClient:
    def __init__(self, endpoint=AZURE_ENDPOINT, key=AZURE_KEY):
        self.endpoint = endpoint.rstrip('/')
        self.key = key

    def process_document(self, file_path):
        url = (
            f"{self.endpoint}/formrecognizer/documentModels/"
            f"prebuilt-layout:analyze?api-version=2023-07-31"
        )
        headers = {
            "Ocp-Apim-Subscription-Key": self.key,
            "Content-Type": "application/octet-stream",
        }
        with open(file_path, 'rb') as f:
            resp = requests.post(url, headers=headers, data=f, timeout=60)
        if resp.status_code != 202:
            raise Exception(f"Azure Submission Error: {resp.text}")
        return resp.headers.get("Operation-Location")

    def get_result(self, operation_url):
        headers = {"Ocp-Apim-Subscription-Key": self.key}
        for _ in range(60):
            resp = requests.get(operation_url, headers=headers, timeout=60)
            if resp.status_code != 200:
                raise Exception(f"Azure Result Error: {resp.text}")
            data = resp.json()
            if data.get('status') == 'succeeded':
                return data.get('analyzeResult', {})
            elif data.get('status') == 'failed':
                raise Exception("Azure OCR Task Failed")
            time.sleep(1)
        raise Exception("Azure OCR Task Timeout")

    @staticmethod
    def map_to_local_layout(azure_result, page_number=1):
        pages = azure_result.get('pages', [])
        if not pages:
            return None

        az_page = next(
            (p for p in pages if p.get('pageNumber') == page_number), pages[0]
        )

        reconstructed = {
            "page_dims": {
                "width":  az_page.get('width',  0),
                "height": az_page.get('height', 0),
            },
            "blocks": [],
            "tables": [],
        }

        for p in azure_result.get('paragraphs', []):
            p_page = p.get('boundingRegions', [{}])[0].get('pageNumber')
            if p_page != page_number:
                continue

            poly   = p.get('boundingRegions', [{}])[0].get(
                'polygon', [0, 0, 0, 0, 0, 0, 0, 0]
            )
            x_vals = poly[0::2]
            y_vals = poly[1::2]
            bbox   = [min(x_vals), min(y_vals), max(x_vals), max(y_vals)]

            reconstructed["blocks"].append({
                "type": "paragraph",
                "bbox": bbox,
                "text": p.get('content', ''),
            })

        for t in azure_result.get('tables', []):
            t_page = t.get('boundingRegions', [{}])[0].get('pageNumber')
            if t_page != page_number:
                continue

            poly   = t.get('boundingRegions', [{}])[0].get(
                'polygon', [0, 0, 0, 0, 0, 0, 0, 0]
            )
            x_vals = poly[0::2]
            y_vals = poly[1::2]
            t_bbox = [min(x_vals), min(y_vals), max(x_vals), max(y_vals)]

            cells = []
            for c in t.get('cells', []):
                c_poly = c.get('boundingRegions', [{}])[0].get(
                    'polygon', [0, 0, 0, 0, 0, 0, 0, 0]
                )
                cx     = c_poly[0::2]
                cy     = c_poly[1::2]
                c_bbox = [min(cx), min(cy), max(cx), max(cy)]

                cells.append({
                    "text":       c.get('content', ''),
                    "row_index":  c.get('rowIndex'),
                    "col_index":  c.get('columnIndex'),
                    "bbox":       c_bbox,
                    "rowspan":    c.get("rowSpan",    1),
                    "colspan":    c.get("columnSpan", 1),
                    "bold":       False,
                    "italic":     False,
                    "font_family": "",
                    "font_size":  10.0,
                    "font_weight": "normal",
                    "font_style":  "normal",
                    "text_color":  "#000000",
                    "background_color": "",
                    "spans": [{
                        "bbox":        c_bbox,
                        "text":        c.get('content', ''),
                        "font_family": "Helvetica",
                        "font_size":   10.0,
                        "font_weight": "normal",
                        "font_style":  "normal",
                        "text_color":  "#000000",
                    }],
                })

            col_count = t.get('columnCount', 0)
            row_count = t.get('rowCount',    0)
            t_w = t_bbox[2] - t_bbox[0]
            t_h = t_bbox[3] - t_bbox[1]

            col_widths  = []
            row_heights = []

            if t_w > 0 and col_count > 0:
                for c_idx in range(col_count):
                    col_cells = [cc for cc in cells if cc['col_index'] == c_idx]
                    if col_cells:
                        w = (max(cc['bbox'][2] for cc in col_cells)
                             - min(cc['bbox'][0] for cc in col_cells))
                        col_widths.append(round((w / t_w) * 100, 2))
                    else:
                        col_widths.append(round(100 / col_count, 2))

            if t_h > 0 and row_count > 0:
                for r_idx in range(row_count):
                    row_cells = [cc for cc in cells if cc['row_index'] == r_idx]
                    if row_cells:
                        h = (max(cc['bbox'][3] for cc in row_cells)
                             - min(cc['bbox'][1] for cc in row_cells))
                        row_heights.append(round((h / t_h) * 100, 2))
                    else:
                        row_heights.append(round(100 / row_count, 2))

            reconstructed["tables"].append({
                "bbox":        t_bbox,
                "cells":       cells,
                "rows":        _table_rows_from_cells(
                    cells, row_count=row_count, col_count=col_count
                ),
                "col_count":   col_count,
                "row_count":   row_count,
                "col_widths":  col_widths,
                "row_heights": row_heights,
            })

        return reconstructed


class TesseractOCRClient:
    @staticmethod
    def process_image(file_path):
        from PIL import Image
        import pytesseract

        data = pytesseract.image_to_data(
            Image.open(file_path), output_type=pytesseract.Output.DICT
        )

        reconstructed = {
            "page_dims": {"width": 0, "height": 0},
            "blocks":    [],
            "tables":    [],
        }

        current_block = -1
        block_text    = []
        block_bbox    = [0, 0, 0, 0]

        n_boxes = len(data['text'])
        for i in range(n_boxes):
            try:
                conf = float(data['conf'][i])
            except Exception:
                conf = -1
            if conf < 10:
                continue

            b_num = data['block_num'][i]
            if b_num != current_block:
                if current_block != -1 and block_text:
                    reconstructed["blocks"].append({
                        "type": "paragraph",
                        "bbox": block_bbox,
                        "text": " ".join(block_text),
                    })
                current_block = b_num
                block_text    = []
                block_bbox    = [
                    data['left'][i],
                    data['top'][i],
                    data['left'][i]  + data['width'][i],
                    data['top'][i]   + data['height'][i],
                ]

            block_text.append(data['text'][i])
            block_bbox[0] = min(block_bbox[0], data['left'][i])
            block_bbox[1] = min(block_bbox[1], data['top'][i])
            block_bbox[2] = max(block_bbox[2], data['left'][i]  + data['width'][i])
            block_bbox[3] = max(block_bbox[3], data['top'][i]   + data['height'][i])

        if block_text:
            reconstructed["blocks"].append({
                "type": "paragraph",
                "bbox": block_bbox,
                "text": " ".join(block_text),
            })

        return reconstructed


class AdobeOCRClient:
    def __init__(
        self,
        client_id=settings.ADOBE_CLIENT_ID,
        client_secret=settings.ADOBE_CLIENT_SECRET,
    ):
        self.client_id     = client_id
        self.client_secret = client_secret

    def process_document(self, file_path):
        import os
        import zipfile
        import json
        from adobe.pdfservices.operation.auth.service_principal_credentials import (
            ServicePrincipalCredentials,
        )
        from adobe.pdfservices.operation.pdf_services import PDFServices
        from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
        from adobe.pdfservices.operation.operation_params.extract_pdf.extract_pdf_params import (
            ExtractPDFParams,
        )
        from adobe.pdfservices.operation.operation_params.extract_pdf.extract_element_type import (
            ExtractElementType,
        )
        from adobe.pdfservices.operation.pdf_jobs.jobs.extract_pdf_job import ExtractPDFJob
        from adobe.pdfservices.operation.pdf_jobs.results.extract_pdf_result import (
            ExtractPDFResult,
        )

        try:
            credentials = ServicePrincipalCredentials(
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            pdf_services = PDFServices(credentials=credentials)

            with open(file_path, "rb") as f:
                input_stream = f.read()

            mime_type = (
                PDFServicesMediaType.PDF
                if file_path.lower().endswith('.pdf')
                else PDFServicesMediaType.PNG
            )
            input_asset = pdf_services.upload(
                input_stream=input_stream, mime_type=mime_type
            )

            extract_pdf_params = ExtractPDFParams(
                elements_to_extract=[
                    ExtractElementType.TEXT,
                    ExtractElementType.TABLES,
                ],
            )
            extract_pdf_job = ExtractPDFJob(
                input_asset=input_asset,
                extract_pdf_params=extract_pdf_params,
            )

            location = pdf_services.submit(extract_pdf_job)
            pdf_services_response = pdf_services.get_job_result(
                location, ExtractPDFResult
            )

            result_asset = pdf_services_response.get_result().get_resource()
            stream_asset = pdf_services.get_content(result_asset)

            with tempfile.TemporaryDirectory() as tmp_dir:
                zip_path = os.path.join(tmp_dir, "extract.zip")
                with open(zip_path, "wb") as f:
                    f.write(stream_asset.get_input_stream())

                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(tmp_dir)

                json_path = os.path.join(tmp_dir, "structuredData.json")
                if os.path.exists(json_path):
                    with open(json_path, 'r', encoding='utf-8') as f:
                        return json.load(f)

            return None
        except Exception as e:
            raise Exception(f"Adobe Extract API Error: {str(e)}")

    @staticmethod
    def map_to_local_layout(adobe_json, page_number=0):
        elements = adobe_json.get('elements', [])

        reconstructed = {
            "page_dims": {"width": 595.0, "height": 842.0},
            "blocks":    [],
            "tables":    [],
        }

        for el in elements:
            if el.get('Page') != page_number:
                continue

            bounds = el.get('Bounds', [0, 0, 0, 0])

            if 'Text' in el:
                reconstructed["blocks"].append({
                    "type": "paragraph",
                    "bbox": bounds,
                    "text": el.get('Text', ''),
                })

            if el.get('Path', '').endswith('/Table'):
                cells      = []
                table_path = el.get('Path')
                row_index  = 0
                col_index  = 0
                for cell_el in elements:
                    if cell_el.get('Path', '').startswith(table_path + '/TR'):
                        c_bounds = cell_el.get('Bounds', [0, 0, 0, 0])
                        cells.append({
                            "text":       cell_el.get('Text', ''),
                            "row_index":  row_index,
                            "col_index":  col_index,
                            "bbox":       c_bounds,
                            "rowspan":    1,
                            "colspan":    1,
                            "bold":       False,
                            "italic":     False,
                            "font_family": "",
                            "font_size":  10.0,
                            "font_weight": "normal",
                            "font_style":  "normal",
                            "text_color":  "#000000",
                            "background_color": "",
                            "spans": [{
                                "bbox":        c_bounds,
                                "text":        cell_el.get('Text', ''),
                                "font_family": "Helvetica",
                                "font_size":   10.0,
                                "font_weight": "normal",
                                "font_style":  "normal",
                                "text_color":  "#000000",
                            }],
                        })
                        col_index += 1

                reconstructed["tables"].append({
                    "bbox":  bounds,
                    "cells": cells,
                    "rows":  _table_rows_from_cells(cells, row_count=0, col_count=0),
                })
        # End of elements loop
        return reconstructed

# --- OCR SERVICE (RELOAD) ---
class OCRService:

    @classmethod
    def process_page(cls, page_obj):
        logger.info(
            f"--- Starting OCR/Extraction for Page {page_obj.id} "
            f"(No. {page_obj.page_number}) ---"
        )
        file_path = page_obj.content_file.path

        doc = fitz.open(file_path)
        if len(doc) == 0:
            return

        page       = doc[0]
        native_text = page.get_text("text").strip()

        layout   = None
        provider = None

        adobe_id     = getattr(settings, 'ADOBE_CLIENT_ID',     None)
        adobe_secret = getattr(settings, 'ADOBE_CLIENT_SECRET', None)
        if (
            adobe_id and adobe_id not in ['XXXX-XXXX-XXXX-XXXX-XXXX', 'test_app_id']
            and adobe_secret and adobe_secret != 'XXXX-XXXX-XXXX-XXXX-XXXX'
        ):
            try:
                logger.info(f"Attempting Dynamic Adobe Extraction for page {page_obj.id}")
                layout, _ = cls._extract_adobe_layout(page)
                page_obj.is_scanned = (len(native_text) <= 30)
                provider = 'adobe'
            except Exception as e:
                logger.error(f"Adobe Extract failed, falling back: {e}")

        if not layout:
            if len(native_text) > 30:
                logger.info(f"Using Native Extraction for page {page_obj.id}")
                layout          = cls._extract_native_layout(page_obj)
                page_obj.is_scanned = False
                provider        = 'native'
            else:
                azure_key = getattr(settings, 'AZURE_OCR_KEY', None)
                if azure_key and azure_key not in [
                    'XXXX-XXXX-XXXX-XXXX-XXXX', 'XXXX-XXXX-XXXX-XXXX-XXXX'
                ]:
                    try:
                        layout, _ = cls._extract_azure_layout(page)
                        page_obj.is_scanned = True
                        provider = 'azure'
                    except Exception as e:
                        logger.error(f"Azure OCR failed, falling back: {e}")

                if not layout:
                    try:
                        layout, _ = cls._extract_tesseract_layout(page)
                        page_obj.is_scanned = True
                        provider = 'tesseract'
                    except Exception as e:
                        logger.error(f"Tesseract failed, falling back to ABBYY: {e}")

                        abbyy_id   = getattr(settings, 'OCR_APP_ID',   None)
                        abbyy_pass = getattr(settings, 'OCR_PASSWORD', None)
                        if (
                            abbyy_id and abbyy_id not in ['test_app_id', 'DocPro_OCR']
                            and abbyy_pass and abbyy_pass != 'XXXX-XXXX-XXXX-XXXX-XXXX'
                        ):
                            layout, _ = cls._extract_ocr_layout(page)
                            page_obj.is_scanned = True
                            provider = 'abbyy'
                        else:
                            logger.info(
                                "Skipping ABBYY OCR due to missing/placeholder credentials."
                            )

        if layout:
            layout = cls._enrich_layout_with_table_cell_blocks(layout)
            page_obj.layout_data  = layout
            page_obj.ocr_provider = provider

            if 'blocks' in layout:
                layout_for_text = copy.deepcopy(layout)
                layout_for_text["blocks"] = [
                    b for b in layout_for_text.get("blocks", [])
                    if b.get("block_type") != "table_cell"
                ]
                try:
                    layout_for_text = NormalizationService.normalize_layout(layout_for_text)
                except Exception as e:
                    logger.warning(f"Layout normalization skipped due to error: {e}")
                text_content = "\n".join(
                    [b.get('text', '') for b in layout_for_text.get('blocks', [])]
                )
                page_obj.text_content = text_content

            page_obj.save(
                update_fields=['layout_data', 'is_scanned', 'ocr_provider', 'text_content']
            )

        if not layout:
            logger.error(
                f"Failed to extract layout for page {page_obj.id} using any provider "
                f"(Provider: {provider})"
            )
            page_obj.is_processed = True
            page_obj.save(update_fields=['is_processed'])
            return

        html_blocks = []

        for block in layout.get("blocks", []):
            if not isinstance(block, dict):
                continue
            if block.get("block_type") == "table_cell":
                continue

            if block.get("lines"):
                for line in block["lines"]:
                    line_bbox = line.get("bbox") or block.get("bbox") or []
                    span_html = []
                    for span in line.get("spans", []):
                        span_text   = html.escape(span.get("text", ""))
                        span_bbox   = json.dumps(span.get("bbox") or line_bbox or [])
                        span_font   = html.escape(span.get("font", "sans-serif"))
                        raw_sz      = span.get("size", 10)
                        span_size   = float(raw_sz) if raw_sz and float(raw_sz) > 0 else 10.0
                        span_color  = _rgb_to_hex(span.get("color", 0)) or "#000000"
                        span_weight = (
                            "bold" if _is_bold_font(span.get("font", ""), span.get("flags", 0))
                            else "normal"
                        )
                        span_style  = (
                            "italic" if _is_italic_font(span.get("font", ""), span.get("flags", 0))
                            else "normal"
                        )
                        style = (
                            f"font-family: {span_font}; font-size: {span_size}pt; "
                            f"font-weight: {span_weight}; font-style: {span_style}; "
                            f"color: {span_color};"
                        )
                        span_html.append(
                            f'<span data-bbox=\'{span_bbox}\' '
                            f'data-font-family="{span_font}" '
                            f'data-font-size="{span_size}" '
                            f'data-font-weight="{span_weight}" '
                            f'data-font-style="{span_style}" '
                            f'data-text-color="{span_color}" '
                            f'style="{style}">{span_text}</span>'
                        )
                    if span_html:
                        line_bbox_str = json.dumps(line_bbox)
                        html_blocks.append(
                            f'<div class="pdf-line" data-bbox=\'{line_bbox_str}\'>'
                            f"{''.join(span_html)}</div>"
                        )
            else:
                bbox_str    = json.dumps(block.get('bbox', []))
                text        = html.escape(block.get("text", ""))
                font_weight = block.get("font_weight", "normal")
                font_style  = block.get("font_style",  "normal")
                font_family = block.get("font_family", "sans-serif")
                raw_sz      = block.get("font_size", 10.0)
                font_size   = float(raw_sz) if raw_sz and float(raw_sz) > 0 else 10.0
                color       = block.get("color", "#000000")
                align       = block.get("alignment", "left")
                style = (
                    f"font-family: {font_family}; font-size: {font_size}pt; "
                    f"font-weight: {font_weight}; font-style: {font_style}; "
                    f"color: {color}; text-align: {align};"
                )
                html_blocks.append(
                    f'<p data-bbox=\'{bbox_str}\' '
                    f'data-font-family="{font_family}" '
                    f'data-font-size="{font_size}" '
                    f'data-font-weight="{font_weight}" '
                    f'data-font-style="{font_style}" '
                    f'data-text-color="{color}" '
                    f'style="{style}">{text}</p>'
                )

        for table in layout.get("tables", []):
            table_html = _table_to_html(table)
            if table_html:
                html_blocks.append(table_html)

        structured_html = "".join(html_blocks)

        page_obj.layout_data  = layout
        page_obj.text_content = structured_html
        page_obj.is_processed = True
        page_obj.save(update_fields=[
            'layout_data', 'text_content', 'is_processed', 'is_scanned'
        ])
        doc.close()

    @classmethod
    def _enrich_layout_with_table_cell_blocks(cls, layout):
        return _enrich_layout_with_table_cell_blocks(layout)

    @staticmethod
    def _rect_overlap(rect1, rect2):
        """Simple overlap check for bboxes."""
        if not rect1 or not rect2:
            return False
        try:
            return not (
                rect1[2] < rect2[0] or rect1[0] > rect2[2] or
                rect1[3] < rect2[1] or rect1[1] > rect2[3]
            )
        except Exception:
            return False

    @classmethod
    def _extract_native_layout(cls, page_obj):
        """
        ENTERPRISE LAYER 1: Extracts granular word-level coordinates,
        fonts, table cells, borders, and background fills.
        """
        try:
            doc  = fitz.open(page_obj.content_file.path)
            page = doc[0]
            logger.info(f"Native extraction: Opened PDF {page_obj.content_file.name}")
        except Exception as e:
            logger.error(f"Native extraction: Failed to open PDF: {e}")
            raise

        try:
            width  = page.rect.width
            height = page.rect.height

            reconstructed = {
                "page_width":  width,
                "page_height": height,
                "page_dims":   {"width": width, "height": height},
                "blocks":      [],
                "tables":      [],
                "images":      [],
                "lines":       [],
            }

            layout_dict = page.get_text("dict")
            spans       = _collect_spans(layout_dict)

            try:
                drawings = page.get_drawings()
            except Exception:
                drawings = []

            vector_lines = []
            try:
                vector_lines = LineDetector.detect_lines(page) or []
            except Exception as e:
                logger.warning(f"Line detection failed: {e}")
            reconstructed["blocks"].extend(vector_lines)

            try:
                finder = page.find_tables(snap_tolerance=10, join_tolerance=10)
            except Exception:
                finder = page.find_tables()

            candidate_tables = getattr(finder, "tables", None)
            if candidate_tables is None:
                try:
                    candidate_tables = list(finder)
                except Exception:
                    candidate_tables = []

            filtered_tabs = []
            for t in candidate_tables:
                try:
                    bbox       = list(t.bbox)
                    t_area     = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1])
                    p_area     = page.rect.width * page.rect.height
                    area_ratio = t_area / max(1.0, p_area)

                    rows_text  = t.extract() or []
                    row_count  = getattr(t, "row_count", len(rows_text) or 0)
                    col_count  = getattr(t, "col_count",
                                         len(rows_text[0]) if rows_text else 0)

                    if area_ratio > 0.6 and col_count < 2:
                        continue

                    if col_count >= 5 and rows_text:
                        numeric_cells = 0
                        total_cells   = 0
                        for row in rows_text:
                            for cell in row:
                                val = (cell or '').strip()
                                if val:
                                    total_cells += 1
                                    if val.isdigit() or (
                                        val.replace('.', '', 1).replace(',', '').isdigit()
                                    ):
                                        numeric_cells += 1

                        num_ratio = numeric_cells / max(1, total_cells)
                        if col_count >= 6 and num_ratio < 0.09:
                            continue

                    if not cls._table_candidate_ok(page, bbox, row_count, col_count, rows_text):
                        continue

                    filtered_tabs.append(t)
                except Exception as e:
                    logger.warning(f"Table candidate skipped due to error: {e}")

            table_bboxes = [list(t.bbox) for t in filtered_tabs]

            block_idx = 0
            for block in layout_dict.get("blocks", []):
                if block.get("type") != 0:
                    continue

                bbox = block.get("bbox", [0, 0, 0, 0])
                if any(cls._rect_overlap(bbox, t_bbox) for t_bbox in table_bboxes):
                    continue

                for line in block.get("lines", []):
                    line_bbox  = line.get("bbox", bbox)
                    all_spans  = line.get("spans", [])
                    if not all_spans:
                        continue

                    dominant_span = max(all_spans, key=lambda s: s.get("size", 0) or 0)
                    raw_color     = dominant_span.get("color", 0)
                    hex_color     = _rgb_to_hex(raw_color) or "#000000"
                    font_name_raw = dominant_span.get("font", "Helvetica")
                    is_bold       = _is_bold_font(font_name_raw, dominant_span.get("flags", 0))
                    is_italic     = _is_italic_font(font_name_raw, dominant_span.get("flags", 0))
                    line_text     = " ".join(
                        [_value_to_text(s.get("text", "")) for s in all_spans]
                    ).strip()
                    if not line_text:
                        continue

                    raw_sz = dominant_span.get("size", 10.0)
                    fs     = float(raw_sz) if raw_sz and float(raw_sz) > 0 else 10.0

                    reconstructed["blocks"].append({
                        "id":          f"l_{block_idx}",
                        "type":        "paragraph",
                        "bbox":        list(line_bbox),
                        "text":        line_text,
                        "font_family": font_name_raw,
                        "font_size":   fs,
                        "font_weight": "bold"   if is_bold   else "normal",
                        "font_style":  "italic" if is_italic else "normal",
                        "color":       hex_color,
                        "alignment":   "left",
                    })
                    block_idx += 1

            img_idx = 0
            for block in layout_dict.get("blocks", []):
                if block.get("type") != 1:
                    continue

                b = block.get("bbox", [0, 0, 0, 0])
                w, h = b[2] - b[0], b[3] - b[1]
                if w < 10 or h < 10:
                    continue
                if (w * h) > 0.9 * (page.rect.width * page.rect.height):
                    continue

                in_table = any(
                    not (b[2] < tb[0] or b[0] > tb[2] or b[3] < tb[1] or b[1] > tb[3])
                    for tb in table_bboxes
                )
                if in_table:
                    continue

                img_id   = f"img_{block_idx}_{img_idx}"
                img_idx += 1

                reconstructed["images"].append({
                    "image_id": img_id,
                    "x":        float(b[0]),
                    "y":        float(b[1]),
                    "width":    float(w),
                    "height":   float(h),
                })
                reconstructed["blocks"].append({
                    "type": "image",
                    "bbox": list(b),
                    "text": "",
                })

            for t in filtered_tabs:
                table_data = cls._build_table_layout(page, drawings, spans, t)
                if table_data:
                    reconstructed["tables"].append(table_data)

            return reconstructed
        finally:
            try:
                doc.close()
            except Exception:
                pass

    @classmethod
    def _approximate_grid_boxes(cls, table_bbox, row_count, col_count):
        x0, y0, x1, y1 = table_bbox
        w = max(1.0, x1 - x0)
        h = max(1.0, y1 - y0)

        row_h = h / max(1, row_count)
        col_w = w / max(1, col_count)

        row_boxes  = []
        cell_boxes = []
        for r in range(row_count):
            ry0 = y0 + r * row_h
            ry1 = y0 + (r + 1) * row_h
            row_boxes.append([x0, ry0, x1, ry1])

            row_cells = []
            for c in range(col_count):
                cx0 = x0 + c * col_w
                cx1 = x0 + (c + 1) * col_w
                row_cells.append([cx0, ry0, cx1, ry1])
            cell_boxes.append(row_cells)

        return row_boxes, cell_boxes

    @classmethod
    def _table_has_visible_rules(cls, page, bbox):
        try:
            rect = fitz.Rect(bbox)
        except Exception:
            return False

        horiz     = 0
        vert      = 0
        rect_hits = 0

        try:
            drawings = page.get_drawings() or []
        except Exception:
            drawings = []

        for drawing in drawings:
            try:
                drect = drawing.get("rect")
                if drect:
                    drect = fitz.Rect(drect)
                    if drect.intersects(rect):
                        if drawing.get("items"):
                            rect_hits += 1
            except Exception:
                pass

            for item in drawing.get("items", []):
                if not item:
                    continue

                kind = item[0]
                if kind == "l" and len(item) >= 3:
                    p1, p2 = item[1], item[2]
                    try:
                        x0, y0 = float(p1.x), float(p1.y)
                        x1, y1 = float(p2.x), float(p2.y)
                    except Exception:
                        continue

                    seg = fitz.Rect(
                        min(x0, x1), min(y0, y1),
                        max(x0, x1), max(y0, y1),
                    )
                    if not seg.intersects(rect):
                        continue

                    dx = abs(x1 - x0)
                    dy = abs(y1 - y0)

                    if dy <= 2.5 and dx >= max(18.0, rect.width  * 0.18):
                        horiz += 1
                    elif dx <= 2.5 and dy >= max(18.0, rect.height * 0.18):
                        vert  += 1

                elif kind == "re" and len(item) >= 2:
                    try:
                        r = fitz.Rect(item[1])
                    except Exception:
                        continue
                    if r.intersects(rect):
                        rect_hits += 1

        if horiz >= 2 and vert >= 2:
            return True
        if rect_hits >= 1 and (horiz >= 1 or vert >= 1):
            return True

        return False

    @classmethod
    def _table_candidate_ok(cls, page, bbox, row_count, col_count, rows_text=None):
        if row_count is None:
            row_count = 0
        if col_count is None:
            col_count = 0
        if row_count < 2 or col_count < 2:
            return False
        if cls._table_has_visible_rules(page, bbox):
            return True
        if (
            rows_text
            and len(rows_text) >= 2
            and max(
                (len(r) for r in rows_text if isinstance(r, list)), default=0
            ) >= 2
        ):
            return True
        return False

    @classmethod
    def _build_table_layout(cls, page, drawings, spans, table):
        table_bbox = list(table.bbox)
        rows_text  = table.extract() or []

        row_objs  = list(getattr(table, "rows",  []) or [])
        raw_cells = list(getattr(table, "cells", []) or [])

        row_count = getattr(table, "row_count", 0) or len(rows_text) or len(row_objs) or 0
        col_count = getattr(table, "col_count", 0) or max(
            (len(r) for r in rows_text if isinstance(r, list)), default=0
        )

        if row_count <= 0 and rows_text:
            row_count = len(rows_text)
        if col_count <= 0 and rows_text:
            col_count = (
                max(len(r) for r in rows_text if isinstance(r, list))
                if rows_text else 0
            )

        if row_count <= 0 or col_count <= 0:
            return None

        approx_row_boxes, approx_cell_boxes = cls._approximate_grid_boxes(
            table_bbox, row_count, col_count
        )

        rows        = []
        cells       = []
        row_colors  = []

        for r_idx in range(row_count):
            row_bbox = approx_row_boxes[r_idx]
            if r_idx < len(row_objs):
                row_obj = row_objs[r_idx]
                try:
                    row_bbox = list(getattr(row_obj, "bbox", row_bbox) or row_bbox)
                except Exception:
                    pass

            row_color = _detect_fill_color_from_drawings(drawings, row_bbox)
            row_colors.append(row_color)

            row_data  = []
            row_texts = (
                rows_text[r_idx]
                if r_idx < len(rows_text) and isinstance(rows_text[r_idx], list)
                else []
            )

            row_cell_boxes = None
            if r_idx < len(row_objs):
                try:
                    row_cell_boxes = list(getattr(row_objs[r_idx], "cells", []) or [])
                except Exception:
                    row_cell_boxes = None

            if not row_cell_boxes and raw_cells:
                start          = r_idx * col_count
                end            = start + col_count
                row_cell_boxes = raw_cells[start:end] if start < len(raw_cells) else []

            if not row_cell_boxes:
                row_cell_boxes = approx_cell_boxes[r_idx]

            for c_idx in range(col_count):
                cell_bbox = None
                if c_idx < len(row_cell_boxes):
                    cell_bbox = row_cell_boxes[c_idx]
                if not cell_bbox:
                    cell_bbox = approx_cell_boxes[r_idx][c_idx]

                if isinstance(cell_bbox, (tuple, list)) and len(cell_bbox) == 4:
                    cell_bbox = list(cell_bbox)
                else:
                    cell_bbox = list(approx_cell_boxes[r_idx][c_idx])

                text = ""
                if c_idx < len(row_texts):
                    text = _value_to_text(row_texts[c_idx]).strip()
                if not text:
                    try:
                        text = page.get_text("text", clip=fitz.Rect(cell_bbox)).strip()
                    except Exception:
                        text = ""

                style = _cell_style_from_context(
                    page, drawings, spans, cell_bbox, row_bbox=row_bbox
                )

                # FIX: guarantee non-zero font size from context spans
                resolved_size = style["font_size"] if style["font_size"] > 0 else 10.0

                # Calculate Indentation from PDF Coordinates
                # Many financial tables use left-padding (indent) for hierarchy.
                # We derive this by looking at how far the text spans are from the cell's left edge.
                actual_indent = 0
                try:
                    # Get all text spans in this cell to find the leftmost character
                    cell_rect = fitz.Rect(cell_bbox)
                    cell_dict = page.get_text("dict", clip=cell_rect)
                    leftmost_x = 999999
                    has_text = False
                    for block in cell_dict.get("blocks", []):
                        for line in block.get("lines", []):
                            for span in line.get("spans", []):
                                if span.get("text", "").strip():
                                    leftmost_x = min(leftmost_x, span["bbox"][0])
                                    has_text = True
                    if has_text:
                        # Point-based horizontal offset from cell boundary
                        actual_indent = max(0, round(leftmost_x - cell_bbox[0], 1))
                except Exception:
                    actual_indent = 0

                cell = {
                    "text":       text,
                    "row_index":  r_idx,
                    "col_index":  c_idx,
                    "bbox":       cell_bbox,
                    "rowspan":    1,
                    "colspan":    1,
                    "indent":     actual_indent,
                    "bold":       style["bold"],
                    "italic":     style["italic"],
                    "font_family": style["font_family"],
                    "font_size":  resolved_size,
                    "font_weight": style["font_weight"],
                    "font_style":  style["font_style"],
                    "text_color":  style["text_color"],
                    "background_color": style["background_color"] or row_color,
                    "spans": [{
                        "bbox":        cell_bbox,
                        "text":        text,
                        "font_family": style["font_family"],
                        "font_size":   resolved_size,
                        "font_weight": style["font_weight"],
                        "font_style":  style["font_style"],
                        "text_color":  style["text_color"],
                        "bold":        style["bold"],
                        "italic":      style["italic"],
                    }],
                }
                cells.append(cell)
                row_data.append(cell)

            rows.append(row_data)

        col_widths = []
        x0, y0, x1, y1 = table_bbox
        table_w = max(1.0, x1 - x0)
        for c_idx in range(col_count):
            if row_objs and len(row_objs) > 0:
                first_row_cells = list(getattr(row_objs[0], "cells", []) or [])
                if c_idx < len(first_row_cells):
                    bbox = first_row_cells[c_idx]
                    if bbox:
                        try:
                            col_widths.append(
                                round(((float(bbox[2]) - float(bbox[0])) / table_w) * 100, 2)
                            )
                            continue
                        except Exception:
                            pass
            col_widths.append(round(100 / col_count, 2))

        row_heights = []
        table_h     = max(1.0, y1 - y0)
        if row_objs:
            for r_idx in range(min(row_count, len(row_objs))):
                try:
                    rb = list(
                        getattr(row_objs[r_idx], "bbox", approx_row_boxes[r_idx])
                        or approx_row_boxes[r_idx]
                    )
                    row_heights.append(
                        round(((float(rb[3]) - float(rb[1])) / table_h) * 100, 2)
                    )
                except Exception:
                    row_heights.append(round(100 / row_count, 2))
        else:
            row_heights = [round(100 / row_count, 2) for _ in range(row_count)]

        return {
            "bbox":        table_bbox,
            "cells":       cells,
            "rows":        rows,
            "row_colors":  row_colors,
            "col_count":   col_count,
            "row_count":   row_count,
            "col_widths":  col_widths,
            "row_heights": row_heights,
        }

    @classmethod
    def _extract_ocr_layout(cls, page):
        pix = cls._generate_low_dpi_pixmap(page)

        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
            tmp_path = tmp.name
            pix.save(tmp_path)

        try:
            client  = AbbyyClient()
            task_id = client.process_image(tmp_path, language="English", export_format="xml")

            status     = 'InProgress'
            result_url = None
            for _ in range(60):
                time.sleep(1)
                task_info = client.get_task_status(task_id)
                status    = task_info.get('status')
                if status == 'Completed':
                    result_url = task_info.get('resultUrl')
                    break
                elif status == 'Failed':
                    raise Exception("ABBYY Task Failed")

            if not result_url:
                raise Exception("ABBYY Polling Timeout")

            xml_data = client.download_result(result_url)
            client.delete_task(task_id)

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(xml_data, 'xml')

            page_node = soup.find('page')
            pdf_w = float(page_node['width'])  if page_node else page.rect.width
            pdf_h = float(page_node['height']) if page_node else page.rect.height

            reconstructed = {
                "page_dims": {"width": pdf_w, "height": pdf_h},
                "blocks":    [],
                "tables":    [],
            }

            for block_node in soup.find_all('block'):
                b_bbox = [
                    float(block_node['l']), float(block_node['t']),
                    float(block_node['r']), float(block_node['b']),
                ]

                if block_node.get('type') == 'Table':
                    cells   = []
                    max_row = 0
                    max_col = 0

                    for r_idx, row_node in enumerate(block_node.find_all('row')):
                        max_row = max(max_row, r_idx + 1)
                        for c_idx, cell_node in enumerate(row_node.find_all('cell')):
                            max_col = max(max_col, c_idx + 1)
                            c_bbox  = [
                                float(cell_node['l']), float(cell_node['t']),
                                float(cell_node['r']), float(cell_node['b']),
                            ]
                            c_text = "".join(
                                [c.text for c in cell_node.find_all('char')]
                            )

                            cells.append({
                                "text":       c_text,
                                "row_index":  r_idx,
                                "col_index":  c_idx,
                                "bbox":       c_bbox,
                                "rowspan":    1,
                                "colspan":    1,
                                "bold":       False,
                                "italic":     False,
                                "font_family": "",
                                "font_size":  10.0,
                                "font_weight": "normal",
                                "font_style":  "normal",
                                "text_color":  "#000000",
                                "background_color": "",
                                "spans": [{
                                    "bbox":        c_bbox,
                                    "text":        c_text,
                                    "font_family": "Helvetica",
                                    "font_size":   10.0,
                                    "font_weight": "normal",
                                    "font_style":  "normal",
                                    "text_color":  "#000000",
                                }],
                            })

                    t_w = b_bbox[2] - b_bbox[0]
                    t_h = b_bbox[3] - b_bbox[1]
                    col_widths  = []
                    row_heights = []

                    if t_w > 0 and max_col > 0:
                        for ci in range(max_col):
                            c_cells = [cc for cc in cells if cc['col_index'] == ci]
                            if c_cells:
                                w = (max(cc['bbox'][2] for cc in c_cells)
                                     - min(cc['bbox'][0] for cc in c_cells))
                                col_widths.append(round((w / t_w) * 100, 2))
                            else:
                                col_widths.append(round(100 / max_col, 2))

                    if t_h > 0 and max_row > 0:
                        for ri in range(max_row):
                            r_cells = [cc for cc in cells if cc['row_index'] == ri]
                            if r_cells:
                                h = (max(cc['bbox'][3] for cc in r_cells)
                                     - min(cc['bbox'][1] for cc in r_cells))
                                row_heights.append(round((h / t_h) * 100, 2))
                            else:
                                row_heights.append(round(100 / max_row, 2))

                    reconstructed["tables"].append({
                        "bbox":        b_bbox,
                        "cells":       cells,
                        "rows":        _table_rows_from_cells(
                            cells, row_count=max_row, col_count=max_col
                        ),
                        "col_count":   max_col,
                        "row_count":   max_row,
                        "col_widths":  col_widths,
                        "row_heights": row_heights,
                    })
                else:
                    words = []
                    for line_node in block_node.find_all('line'):
                        line_text = "".join([c.text for c in line_node.find_all('char')])
                        words.append(line_text)

                    if words:
                        reconstructed["blocks"].append({
                            "text": " ".join(words),
                            "bbox": b_bbox,
                            "type": "paragraph",
                        })

            return reconstructed, ""
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    @classmethod
    def _generate_low_dpi_pixmap(cls, page):
        try:
            return page.get_pixmap(dpi=300)
        except Exception:
            return page.get_pixmap(dpi=150)

    @classmethod
    def _extract_azure_layout(cls, page):
        pix = cls._generate_low_dpi_pixmap(page)

        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
            tmp_path = tmp.name
            pix.save(tmp_path)

        try:
            client        = AzureOCRClient()
            operation_url = client.process_document(tmp_path)
            result_json   = client.get_result(operation_url)
            layout        = client.map_to_local_layout(result_json, page_number=1)
            return layout, ""
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    @classmethod
    def _extract_adobe_layout(cls, page):
        pix = cls._generate_low_dpi_pixmap(page)

        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
            tmp_path = tmp.name
            pix.save(tmp_path)

        try:
            client     = AdobeOCRClient()
            adobe_json = client.process_document(tmp_path)
            layout     = client.map_to_local_layout(adobe_json, page_number=0)
            return layout, ""
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    @classmethod
    def _extract_tesseract_layout(cls, page):
        pix = cls._generate_low_dpi_pixmap(page)

        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
            tmp_path = tmp.name
            pix.save(tmp_path)

        try:
            client = TesseractOCRClient()
            layout = client.process_image(tmp_path)
            layout["page_dims"] = {
                "width":  page.rect.width,
                "height": page.rect.height,
            }
            return layout, ""
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)