#!/usr/bin/env python
"""
Fix: Extract EXACT col_widths and row_heights from the PDF 
and save them to all existing PageTable records.

This makes the LHS grid match the RHS table exactly.
"""
import os, sys
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django
django.setup()

import pdfplumber
from apps.documents.models import PageTable, Page

def fix():
    tables = PageTable.objects.select_related('page').all()
    print(f"Found {tables.count()} tables to fix\n")
    
    fixed = 0
    for tbl in tables:
        page = tbl.page
        if not page.content_file:
            print(f"  SKIP {tbl.table_ref}: no content file")
            continue
        
        pdf_path = page.content_file.path
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                p = pdf.pages[0]  # Each page has its own file
                
                # High-sensitivity table detection (same settings as layout_engine)
                tbl_settings = {
                    'vertical_strategy':   'lines',
                    'horizontal_strategy': 'lines',
                    'snap_tolerance':      10,
                    'join_tolerance':      10,
                    'intersection_tolerance': 5,
                }
                
                found_tables = p.find_tables(tbl_settings)
                if not found_tables:
                    # Try text-based
                    tbl_settings['vertical_strategy'] = 'text'
                    tbl_settings['horizontal_strategy'] = 'text'
                    found_tables = p.find_tables(tbl_settings)
                
                if not found_tables:
                    print(f"  SKIP {tbl.table_ref}: no table detected in PDF")
                    continue
                
                # Use the largest table (most likely the main one)
                best = max(found_tables, key=lambda t: (t.bbox[2]-t.bbox[0]) * (t.bbox[3]-t.bbox[1]))
                
                t_x0, t_y0, t_x1, t_y1 = best.bbox
                t_w = t_x1 - t_x0
                t_h = t_y1 - t_y0
                
                # --- Extract EXACT col_widths from cell geometry ---
                col_widths = []
                row_heights = []
                
                # pdfplumber table object has .rows with .cells
                if best.rows:
                    # Use first row for column widths
                    first_row = best.rows[0]
                    for cell in first_row.cells:
                        if cell:  # cell can be None for merged cells
                            cw = cell[2] - cell[0]
                            col_widths.append(round((cw / t_w) * 100, 2))
                        else:
                            col_widths.append(0.0)
                    
                    # Use each row for row heights
                    for row in best.rows:
                        rb = row.bbox
                        rh = rb[3] - rb[1]
                        row_heights.append(round((rh / t_h) * 100, 2))
                
                # Fallback to uniform if something went wrong
                nc = tbl.col_count or 2
                nr = tbl.row_count or 1
                if not col_widths:
                    col_widths = [round(100/nc, 2)] * nc
                if not row_heights:
                    row_heights = [round(100/nr, 2)] * nr
                
                # Normalize to sum=100 exactly
                if col_widths:
                    s = sum(col_widths)
                    if s > 0: col_widths = [round(w*100/s, 2) for w in col_widths]
                if row_heights:
                    s = sum(row_heights)
                    if s > 0: row_heights = [round(h*100/s, 2) for h in row_heights]
                
                # Save to DB
                tbl.col_widths  = col_widths
                tbl.row_heights = row_heights
                tbl.col_count   = len(col_widths)
                tbl.row_count   = len(row_heights)
                tbl.save(update_fields=['col_widths', 'row_heights', 'col_count', 'row_count'])
                
                print(f"  FIXED {tbl.table_ref}: {tbl.row_count}x{tbl.col_count} grid")
                print(f"    col_widths  = {col_widths}")
                print(f"    row_heights = {[round(h,1) for h in row_heights[:5]]}...")
                fixed += 1
                
        except Exception as e:
            print(f"  ERROR {tbl.table_ref}: {e}")
    
    print(f"\n✓ Fixed {fixed}/{tables.count()} tables")

if __name__ == '__main__':
    fix()
