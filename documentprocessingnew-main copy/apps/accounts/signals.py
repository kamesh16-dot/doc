from django.contrib.auth.signals import user_logged_in, user_logged_out
from django.dispatch import receiver
from django.utils import timezone
from .models import User

@receiver(user_logged_in)
def on_user_login(sender, request, user, **kwargs):
    now = timezone.now()
    if hasattr(user, 'resource_profile'):
        profile = user.resource_profile
        profile.last_login_at = now
        profile.last_active_at = now
        profile.status = 'ACTIVE'
        profile.is_available = True
        profile.save(update_fields=['last_login_at', 'last_active_at', 'status', 'is_available'])
        
        # Add to redis tracking explicitly (TTL 90 seconds)
        from django.core.cache import cache
        cache.set(f"user:{user.id}:online", "true", 90)

@receiver(user_logged_out)
def on_user_logout(sender, request, user, **kwargs):
    if user:
        User.objects.filter(pk=user.pk).update(last_activity=None)
        if hasattr(user, 'resource_profile'):
            profile = user.resource_profile
            profile.status = 'INACTIVE'
            profile.is_available = False
            profile.save(update_fields=['status', 'is_available'])
            
        from django.core.cache import cache
        cache.delete(f"user:{user.id}:online")
