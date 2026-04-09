import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from django.contrib.auth import get_user_model
from common.enums import UserRole

User = get_user_model()
username = 'admin'
email = 'admin@psd.com'
password = '12345678'
role = UserRole.ADMIN

user, created = User.objects.get_or_create(
    username=username, 
    defaults={
        'email': email, 
        'role': UserRole.ADMIN, 
        'is_staff': True
    }
)

user.set_password(password)
user.save()

print(f"User '{username}' created: {created}")
