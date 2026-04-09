import fitz
import logging
import json
from io import BytesIO
from django.core.files.base import ContentFile
from apps.documents.models import Page

logger = logging.getLogger(__name__)

class PDFBakeService:
    @staticmethod
    def bake_page_edits(page: Page):
        """
        Takes a Page and its edited Blocks, and produces a 'baked' PDF page version.
        This overwrites the original text on the page with user edits.
        """
        try:
            # 1. Get source PDF path
            # If it's a split page, use its own content_file. Otherwise use the document's file.
            if page.content_file:
                source_pdf = page.content_file
            else:
                source_pdf = page.document.file
                
            if not source_pdf:
                raise ValueError(f"No source PDF found for page {page.id}")

            # Open from bytes to avoid path issues with split files in memory
            # Ensure the file pointer is at the beginning
            source_pdf.seek(0)
            doc = fitz.open(stream=source_pdf.read(), filetype="pdf")
            
            # Determine which page in the PDF we are targeting
            page_index = 0
            if len(doc) > 1:
                # page.page_number is 1-indexed, fitz is 0-indexed
                page_index = page.page_number - 1
                if page_index < 0 or page_index >= len(doc):
                    logger.warning(f"Page number {page.page_number} for page {page.id} is out of bounds for PDF with {len(doc)} pages. Defaulting to page 1.")
                    page_index = 0
            
            source_page = doc[page_index]
            page_rect = source_page.rect
            doc.close()

            # 2. Create a NEW BLANK PDF for pure reconstruction
            # "The final PDF should not use the originally uploaded PDF. 
            # It must be generated only from the edited and submitted blocks."
            new_doc = fitz.open()
            pdf_page = new_doc.new_page(width=page_rect.width, height=page_rect.height)

            # 3. Build elements to bake.
            #
            # PRIORITY ORDER:
            # 1st - Block model  (has BOTH bbox coordinates AND resource-edited current_text)
            # 2nd - Page.text_content HTML (fallback for pages without extracted Blocks,
            #       e.g. if block extraction failed after OCR)
            #
            # WHY: When a resource edits text in the workspace, only Block.current_text is
            # updated (via BlockUpdateView.patch). Page.text_content is NEVER updated after
            # initial OCR.  Using text_content would output the original, unedited text.
            elements = []
            import re

            # --- Primary: Block model ---
            blocks_qs = page.blocks.all().order_by('y', 'x')
            if blocks_qs.exists():
                for block in blocks_qs:
                    raw_bbox = block.bbox
                    if isinstance(raw_bbox, str):
                        try:
                            raw_bbox = json.loads(raw_bbox)
                        except Exception:
                            # Reconstruct from individual coordinate fields
                            if block.x is not None and block.y is not None:
                                raw_bbox = [block.x, block.y,
                                            block.x + (block.width or 0),
                                            block.y + (block.height or 0)]
                            else:
                                continue
                    elif raw_bbox is None:
                        if block.x is not None and block.y is not None:
                            raw_bbox = [block.x, block.y,
                                        block.x + (block.width or 0),
                                        block.y + (block.height or 0)]
                        else:
                            continue

                    if not isinstance(raw_bbox, (list, tuple)) or len(raw_bbox) < 4:
                        continue

                    text_to_bake = (block.current_text or block.extracted_text or '').strip()
                    if not text_to_bake:
                        continue

                    elements.append({
                        'bbox': [float(c) for c in raw_bbox],
                        'text': text_to_bake,
                        'font_size': block.font_size or 10.0
                    })

            # --- Fallback: Page.text_content HTML (when no Blocks exist) ---
            if not elements and page.text_content:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(page.text_content, 'html.parser')
                # span/td = native PDF path; p = scanned/OCR path
                for el in soup.find_all(['span', 'td', 'p'], attrs={'data-bbox': True}):
                    try:
                        bbox_str = el['data-bbox'].strip()
                        bbox_str = bbox_str.replace('(', '[').replace(')', ']')
                        bbox = json.loads(bbox_str)
                        if not isinstance(bbox, (list, tuple)) or len(bbox) < 4:
                            continue

                        text = el.get_text(separator=' ').strip()
                        if not text:
                            continue

                        font_size = 10.0
                        style = el.get('style', '')
                        fs_match = re.search(r'font-size:\s*([\d\.]+)', style)
                        if fs_match:
                            try:
                                font_size = float(fs_match.group(1))
                            except ValueError:
                                pass

                        elements.append({
                            'bbox': [float(c) for c in bbox],
                            'text': text,
                            'font_size': font_size
                        })
                    except Exception:
                        continue

            # 4. Insert User Text onto the BLANK page
            for el in elements:
                if not el['text']:
                    continue

                try:
                    bbox = el['bbox']
                    x0, y0, x1, y1 = bbox[0], bbox[1], bbox[2], bbox[3]
                    box_width  = max(x1 - x0, 1.0)
                    box_height = max(y1 - y0, 1.0)

                    # Auto-scale font to fit the bbox height (leave ~20% for descenders)
                    font_size = min(el['font_size'], box_height * 0.85)
                    font_size = max(font_size, 4.0)  # never go below 4pt

                    # insert_text places text at the baseline.
                    # Baseline = top of box + font_size (approximate ascender height)
                    # Use a small left margin (2pt) to avoid clipping.
                    baseline_y = y0 + font_size

                    # Wrap long lines using insert_textbox with auto-fit to avoid overflow,
                    # but first try insert_text for single-line blocks (faster, never fails).
                    lines = el['text'].splitlines()
                    for line_idx, line in enumerate(lines):
                        line = line.strip()
                        if not line:
                            continue
                        y_pos = baseline_y + line_idx * (font_size * 1.2)
                        # Don't draw outside the page
                        if y_pos > page_rect.height:
                            break
                        pdf_page.insert_text(
                            fitz.Point(x0 + 2, y_pos),
                            line,
                            fontsize=font_size,
                            fontname="helv",
                            color=(0, 0, 0),
                        )
                except Exception as e:
                    logger.error(f"Failed to insert text at bbox {el.get('bbox')} on page {page.id}: {e}", exc_info=True)


            # 5. Save to buffer
            result_buffer = BytesIO()
            new_doc.save(result_buffer)
            new_doc.close()

            return result_buffer.getvalue()
        except Exception as e:
            logger.error(f"Failed to bake page {page.id}: {e}", exc_info=True)
            raise e

    @staticmethod
    def sync_baked_file(page: Page):
        """Generates and saves the baked PDF to a temporary preview field or replaces content_file if appropriate."""
        # For now, let's create a naming convention for baked previews
        content = PDFBakeService.bake_page_edits(page)
        filename = f"baked_p{page.page_number}_{page.id}.pdf"
        
        # We could save this to a 'preview_file' field if we had one, 
        # or just return it for real-time serving.
        return content
