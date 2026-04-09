from django.contrib import admin
from .models import DesktopDevice, AssignmentBundle

@admin.register(DesktopDevice)
class DesktopDeviceAdmin(admin.ModelAdmin):
    list_display = ("device_name", "user", "machine_id", "last_heartbeat_at", "is_active")
    search_fields = ("device_name", "machine_id", "user__username")
    list_filter = ("is_active", "user")

@admin.register(AssignmentBundle)
class AssignmentBundleAdmin(admin.ModelAdmin):
    list_display = ("id", "document", "bundle_index", "page_range", "status", "leased_to", "lease_expires_at")
    list_filter = ("status", "document")
    search_fields = ("id", "document__doc_ref", "document__name")
    readonly_fields = ("lease_token", "source_sha256", "result_sha256")
    
    def page_range(self, obj):
        return f"{obj.page_start} - {obj.page_end}"
    page_range.short_description = "Pages"
