from django.urls import path
from apps.processing import consumers

websocket_urlpatterns = [
    path('ws/workspace/<str:doc_ref>/<int:page_number>/', consumers.WorkspaceConsumer.as_asgi()),
    path('ws/notifications/', consumers.NotificationConsumer.as_asgi()),
]
