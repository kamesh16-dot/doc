import pdfplumber
import sys

pdf_path = r"s:\dp004\docpro copy 2\docpro copy 2\media\documents\originals\2026\03\21\Haystack_Pte_Ltd__FS_FYE_31_10_2023_1.pdf"
with pdfplumber.open(pdf_path) as pdf:
    for p_idx in range(len(pdf.pages)):
        page = pdf.pages[p_idx]
        print(f"--- PAGE {p_idx+1} ---")
        
        table_settings_lines = {
            'vertical_strategy':   'lines',
            'horizontal_strategy': 'lines',
            'snap_tolerance':      10,
            'join_tolerance':      10,
            'intersection_tolerance': 5,
            'text_x_tolerance':    3,
        }
        table_settings_text = {
            'vertical_strategy':   'text',
            'horizontal_strategy': 'text',
            'snap_tolerance':      10,
            'join_tolerance':      10,
        }
        
        tables = page.find_tables(table_settings_lines)
        extracted = page.extract_tables(table_settings_lines)
        strategy = 'lines'
        
        if not extracted:
            tables = page.find_tables(table_settings_text)
            extracted = page.extract_tables(table_settings_text)
            strategy = 'text'
        
        for i, (t_obj, t_data) in enumerate(zip(tables, extracted)):
            bbox = t_obj.bbox
            t_w = bbox[2] - bbox[0]
            t_h = bbox[3] - bbox[1]
            p_w = float(page.width)
            p_h = float(page.height)
            area_ratio = (t_w * t_h) / (p_w * p_h)
            
            col_count = max(len(row) for row in t_data) if t_data else 0
            row_count = len(t_data)
            
            total_cells = row_count * col_count
            empty_cells = sum(1 for row in t_data for cell in row if not (cell or '').strip())
            sparsity = empty_cells / total_cells if total_cells > 0 else 1.0
            
            # Numeric cell count
            numeric_cells = 0
            total_non_empty = 0
            for row in t_data:
                for str_cell in row:
                    val = (str_cell or '').strip()
                    if val:
                        total_non_empty += 1
            num_ratio = numeric_cells / max(1, total_non_empty)
            action = "KEEP"
            if col_count >= 5 and num_ratio < 0.12:
                action = "SKIP"
            
            print(f"Table {i} [{strategy}]: Area={area_ratio:.2f}, Cols={col_count}, Rows={row_count}, Num={num_ratio:.2f}, Sparsity={sparsity:.2f} -> {action}")

