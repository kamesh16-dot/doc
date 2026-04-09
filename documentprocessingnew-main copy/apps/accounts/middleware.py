from django.utils import timezone
from .models import User

class ActiveUserMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.user.is_authenticated:
            # Skip activity update for heartbeats and static assets to reduce DB noise and terminal clutter
            if any(request.path.startswith(prefix) for prefix in ['/static/', '/media/', '/heartbeat/']):
                return self.get_response(request)

            now = timezone.now()
            user = request.user
            
            from django.core.cache import cache
            
            # Redis-based presence tracking: heartbeat updates TTL to 30s (Industrial Upgrade)
            cache_key = f"user:{user.id}:online"
            was_online = cache.get(cache_key)
            cache.set(cache_key, "true", timeout=90)
            
            # Smart Trigger for Assignment Engine
            if user.role == 'RESOURCE':
                from apps.processing.tasks import assign_pages_task
                
                # Throttle assignment checks to once per 60 seconds per active resource
                # OR trigger immediately if they were "offline" (fresh login/activity)
                throttle_key = f"assignment_trigger_{user.id}"
                last_trigger = cache.get(throttle_key)
                
                if not was_online or not last_trigger:
                    assign_pages_task.delay()
                    cache.set(throttle_key, now.timestamp(), 60)
        
        response = self.get_response(request)
        return response
