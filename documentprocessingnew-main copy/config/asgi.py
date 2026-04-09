import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django_asgi_app = get_asgi_application()

from channels.security.websocket import AllowedHostsOriginValidator
from apps.processing.routing import websocket_urlpatterns as processing_urlpatterns
from apps.documents.routing import websocket_urlpatterns as documents_urlpatterns

combined_urlpatterns = processing_urlpatterns + documents_urlpatterns

application = ProtocolTypeRouter({
    "http": django_asgi_app,
    "websocket": AllowedHostsOriginValidator(
        AuthMiddlewareStack(
            URLRouter(
                combined_urlpatterns
            )
        )
    ),
})
