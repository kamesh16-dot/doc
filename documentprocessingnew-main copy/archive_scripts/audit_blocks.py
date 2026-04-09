import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()
from apps.documents.models import PageTable, Block

def audit():
    t = PageTable.objects.filter(page__page_number=2).first()
    if t:
        print(f"Table {t.table_ref} cells count: {t.col_count}x{t.row_count}")
        blocks = Block.objects.filter(page=t.page, table_id=t.table_ref)
        print(f"Blocks in table: {blocks.count()}")
        for b in blocks[:5]:
            print(f"  Block {b.id}: x={b.x}, y={b.y}, w={b.width}, h={b.height}, r={b.row_index}, c={b.col_index}")

if __name__ == "__main__":
    audit()
