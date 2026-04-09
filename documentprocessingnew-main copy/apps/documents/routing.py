from django.urls import path
from apps.documents import consumers

websocket_urlpatterns = [
    path('ws/conversion/<str:document_id>/', consumers.ConversionStatusConsumer.as_asgi()),
]
