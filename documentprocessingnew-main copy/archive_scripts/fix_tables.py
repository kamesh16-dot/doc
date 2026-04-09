import os
import django
import json

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from apps.documents.models import Page, PageTable, Block

def fix():
    tables = PageTable.objects.all()
    for t in tables:
        print(f"Table {t.table_ref} (Page {t.page.page_number}) - Col Count: {t.col_count}")
        blocks = Block.objects.filter(page=t.page, table_id=t.table_ref)
        if not blocks.exists():
            # Initialize empty tables to even distribution
            if t.col_count > 0: t.col_widths = [round(100/t.col_count, 2)] * t.col_count
            if t.row_count > 0: t.row_heights = [round(100/t.row_count, 2)] * t.row_count
            t.save()
            continue

        # 1. Cluster X to find column boundaries
        x_positions = sorted(list(set([b.x for b in blocks])))
        cols = []
        if x_positions:
            current_cluster = [x_positions[0]]
            for x in x_positions[1:]:
                if x - current_cluster[0] < 15: # 15px tolerance
                    current_cluster.append(x)
                else:
                    cols.append(min(current_cluster)) # left edge
                    current_cluster = [x]
            cols.append(min(current_cluster))
        
        # 2. Cluster Y to find row boundaries
        y_positions = sorted(list(set([b.y for b in blocks])))
        rows = []
        if y_positions:
            current_cluster = [y_positions[0]]
            for y in y_positions[1:]:
                if y - current_cluster[0] < 8: # 8px tolerance for rows
                    current_cluster.append(y)
                else:
                    rows.append(min(current_cluster))
                    current_cluster = [y]
            rows.append(min(current_cluster))

        # Calculate relative widths from cluster starts
        if cols:
            # We treat the space between clusters as columns
            # Append table right edge
            boundaries = cols + [t.x + t.width]
            widths = [(boundaries[i+1] - boundaries[i]) for i in range(len(boundaries)-1)]
            # Normalize
            total_w = sum(widths)
            if total_w > 0:
                t.col_widths = [round((w/total_w)*100, 2) for w in widths]
                t.col_count = len(widths)
        
        if rows:
            boundaries = rows + [t.y + t.height]
            heights = [(boundaries[i+1] - boundaries[i]) for i in range(len(boundaries)-1)]
            total_h = sum(heights)
            if total_h > 0:
                t.row_heights = [round((h/total_h)*100, 2) for h in heights]
                t.row_count = len(heights)
        
        print(f"  Fixed: {t.col_count}x{t.row_count} Grid")
        print(f"  Widths: {t.col_widths}")
        t.save()

if __name__ == "__main__":
    fix()
