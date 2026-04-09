
from django.contrib import admin
from .models import Document, Page

class PageInline(admin.TabularInline):
    model = Page
    extra = 0
    readonly_fields = ('page_number', 'status', 'current_assignee', 'locked_at', 'processed_file')
    can_delete = False
    ordering = ('page_number',)

@admin.register(Document)
class DocumentAdmin(admin.ModelAdmin):
    list_display = ('id', 'client', 'status', 'total_pages', 'created_at', 'completed_at')
    list_filter = ('status', 'client')
    search_fields = ('id', 'client__username')
    readonly_fields = ('id', 'created_at', 'updated_at', 'completed_at', 'total_pages')
    inlines = [PageInline]

@admin.register(Page)
class PageAdmin(admin.ModelAdmin):
    list_display = ('id', 'document_link', 'page_number', 'status', 'current_assignee', 'locked_at')
    list_filter = ('status', 'created_at')
    search_fields = ('document__id', 'current_assignee__username')
    readonly_fields = ('document', 'page_number', 'content_file', 'processed_file', 'created_at', 'updated_at')

    def document_link(self, obj):
        return obj.document.id
    document_link.short_description = 'Document'
