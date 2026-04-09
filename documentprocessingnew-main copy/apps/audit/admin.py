
from django.contrib import admin
from .models import AuditLog

@admin.register(AuditLog)
class AuditLogAdmin(admin.ModelAdmin):
    list_display = ('created_at', 'action', 'document_id', 'assignment_id', 'actor', 'new_status')
    list_filter = ('action', 'new_status')
    search_fields = ('document_id', 'actor__username', 'metadata')
    readonly_fields = [field.name for field in AuditLog._meta.get_fields()]
    
    def has_add_permission(self, request):
        return False
    
    def has_change_permission(self, request, obj=None):
        return False
    
    def has_delete_permission(self, request, obj=None):
        return False
