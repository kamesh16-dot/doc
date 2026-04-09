from django.urls import path, include
from rest_framework.routers import SimpleRouter
from apps.audit.views import AuditLogViewSet

router = SimpleRouter()
router.register(r'logs', AuditLogViewSet, basename='auditlog')

urlpatterns = [
    path('', include(router.urls)),
]
