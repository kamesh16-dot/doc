import django, os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()
from django.contrib.auth import get_user_model
try:
    u = get_user_model().objects.get(username='admin')
    print(f"User: {u.username}, Role: {getattr(u, 'role', 'N/A')}, Staff: {u.is_staff}, IsAdmin: {u.is_superuser}")
except Exception as e:
    print(f"Error: {e}")
