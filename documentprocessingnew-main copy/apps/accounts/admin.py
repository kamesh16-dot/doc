
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import User, ResourceProfile, AdminProfile, ClientProfile

@admin.register(User)
class CustomUserAdmin(UserAdmin):
    list_display = ('username', 'email', 'role', 'is_active', 'date_joined')
    list_filter = ('role', 'is_active')
    fieldsets = UserAdmin.fieldsets + (
        ('Role Information', {'fields': ('role',)}),
    )

@admin.register(AdminProfile)
class AdminProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'managed_by', 'created_at')
    search_fields = ('user__username', 'user__email')

@admin.register(ClientProfile)
class ClientProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'company_name', 'created_at')
    search_fields = ('user__username', 'user__email', 'company_name')

@admin.register(ResourceProfile)
class ResourceProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'priority', 'max_capacity', 'is_active_for_assignment', 'status_badge', 'current_load', 'last_active_at')
    list_editable = ('priority', 'max_capacity', 'is_active_for_assignment')
    search_fields = ('user__username', 'user__email')
    list_filter = ('is_active_for_assignment', 'status', 'is_available')
    readonly_fields = ('status', 'is_available', 'last_login_at', 'last_active_at', 'current_load')
    
    def status_badge(self, obj):
        from django.utils.html import format_html
        colors = {
            'ACTIVE': 'green',
            'INACTIVE': 'gray',
            'BUSY': 'red'
        }
        color = colors.get(obj.status, 'gray')
        return format_html(
            '<span style="color: white; background-color: {}; padding: 3px 8px; border-radius: 10px; font-weight: bold;">{}</span>',
            color,
            obj.status
        )
    status_badge.short_description = 'Availability Status'
