from django.contrib import admin
from .models import Assignment, DocumentQueue, PageAssignment, SubmittedPage, RejectedPage, ReassignmentLog, MergedDocument, ApprovedDocument
from django.utils.html import format_html

# Legacy Block Assignment
@admin.register(Assignment)
class AssignmentAdmin(admin.ModelAdmin):
    list_display = ('id', 'resource', 'document', 'start_page', 'end_page', 'status', 'assigned_at', 'expires_at')
    list_filter = ('status', 'assigned_at', 'resource')
    search_fields = ('resource__username', 'document__doc_ref')

@admin.register(DocumentQueue)
class DocumentQueueAdmin(admin.ModelAdmin):
    list_display = ('id', 'document', 'get_priority', 'status', 'created_at')
    list_filter = ('status', 'document__priority')

    def get_priority(self, obj):
        return obj.document.get_priority_display()
    get_priority.short_description = 'Priority'

@admin.register(PageAssignment)
class PageAssignmentAdmin(admin.ModelAdmin):
    list_display = ('id', 'document', 'page', 'resource', 'status', 'get_complexity_weight', 'assigned_at', 'timed_out')
    list_filter = ('status', 'timed_out', 'is_reassigned')
    search_fields = ('resource__user__username', 'document__doc_ref')

    def get_complexity_weight(self, obj):
        return obj.page.complexity_weight
    get_complexity_weight.short_description = 'Weight'

    # Example of inline reassignment action for Admins
    actions = ['force_timeout_reassign']

    def force_timeout_reassign(self, request, queryset):
        from apps.processing.services.core import AssignmentService
        count = 0
        for assignment in queryset:
            new_a = AssignmentService.reassign_rejected_assignment(assignment.id, request.user)
            if new_a:
                count += 1
        self.message_user(request, f"Successfully reassigned {count} assignments.")
    force_timeout_reassign.short_description = "Force Timeout & Reassign"

@admin.register(SubmittedPage)
class SubmittedPageAdmin(admin.ModelAdmin):
    list_display = ('id', 'document', 'page_number', 'submitted_by', 'review_status', 'submitted_at')
    list_filter = ('review_status',)
    search_fields = ('document__doc_ref', 'submitted_by__username')

    actions = ['approve_submissions']

    def approve_submissions(self, request, queryset):
        from common.enums import ReviewStatus
        with transaction.atomic():
            updated = queryset.update(
                review_status=ReviewStatus.APPROVED, 
                reviewed_by=request.user, 
                reviewed_at=timezone.now()
            )
            # Signal will handle checking if we need to merge
            # but bulk update doesn't trigger signals. We must save individually or trigger manually.
            for sub in queryset:
                # Retrigger the specific save signal logic
                sub.save(update_fields=['review_status'])
        
        self.message_user(request, f"Approved {updated} submissions.")
    approve_submissions.short_description = "Approve Selected Submissions"

@admin.register(RejectedPage)
class RejectedPageAdmin(admin.ModelAdmin):
    list_display = ('id', 'document', 'page', 'rejected_by', 'rejection_reason', 'reassign_status')
    list_filter = ('rejection_reason', 'reassign_status')

@admin.register(ReassignmentLog)
class ReassignmentLogAdmin(admin.ModelAdmin):
    list_display = ('id', 'reassigned_by', 'previous_resource', 'new_resource', 'reassigned_at')
    
@admin.register(MergedDocument)
class MergedDocumentAdmin(admin.ModelAdmin):
    list_display = ('id', 'document', 'merge_status', 'merged_by', 'merge_completed_at')

@admin.register(ApprovedDocument)
class ApprovedDocumentAdmin(admin.ModelAdmin):
    list_display = ('id', 'document', 'approved_by', 'approved_at')
