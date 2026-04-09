import fitz
import json
import os

def debug_pdf(path, page_num):
    if not os.path.exists(path):
        print(f"Error: File {path} not found.")
        return

    doc = fitz.open(path)
    if page_num >= len(doc):
        print(f"Error: Page {page_num} not found. Document has {len(doc)} pages.")
        return
        
    page = doc[page_num]
    
    print(f"--- Debugging Page {page_num} of {path} ---")
    
    # Check drawings
    drawings = page.get_drawings()
    print(f"Found {len(drawings)} drawings")
    for i, d in enumerate(drawings):
         b = d['rect']
         w, h = b[2]-b[0], b[3]-b[1]
         if (h < 5 and w > 20) or (w < 5 and h > 20):
             print(f"Candidate Line {i}: rect={d['rect']}, width={d.get('width', 'N/A')}, type={d.get('type', 'N/A')}")
         
    # Check dict blocks
    layout = page.get_text("dict")
    blocks = layout.get("blocks", [])
    print(f"Found {len(blocks)} dict blocks")
    for i, b in enumerate(blocks):
        if b['type'] != 0:
            print(f"Block {i}: type={b['type']}, bbox={b['bbox']}")

if __name__ == "__main__":
    path = r"S:\dp004\docpro copy 2\docpro copy 2\media\documents\originals\2026\03\21\Haystack_Pte_Ltd__FS_FYE_31_10_2023_1_LemV3G0.pdf"
    debug_pdf(path, 8) # Page 9 (0-indexed 8)
