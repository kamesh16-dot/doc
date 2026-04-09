import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from apps.processing.tasks import extract_page_blocks_task
try:
    extract_page_blocks_task(2119)
    print("SUCCESS")
except Exception as e:
    import traceback
    traceback.print_exc()
