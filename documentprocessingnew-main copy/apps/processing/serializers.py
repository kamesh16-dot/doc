from rest_framework import serializers
from apps.documents.models import Document, Page
from apps.processing.models import PageAssignment, SubmittedPage, RejectedPage, ReassignmentLog
from apps.documents.serializers import PageSerializer
from apps.accounts.serializers import UserSerializer

class PageAssignmentSerializer(serializers.ModelSerializer):
    document_ref = serializers.CharField(source='document.doc_ref', read_only=True)
    document_title = serializers.CharField(source='document.title', read_only=True)
    page_number = serializers.IntegerField(source='page.page_number', read_only=True)
    complexity_weight = serializers.FloatField(source='page.complexity_weight', read_only=True)
    
    status_display = serializers.CharField(source='get_status_display', read_only=True)
    
    class Meta:
        model = PageAssignment
        fields = (
            'id', 'document_ref', 'document_title', 'page_number',
            'status', 'status_display', 'complexity_weight',
            'assigned_at', 'processing_start_at', 'processing_end_at',
            'max_processing_time', 'time_warning_sent', 'timed_out',
            'is_reassigned', 'reassignment_count'
        )

class SubmittedPageSerializer(serializers.ModelSerializer):
    document_ref = serializers.CharField(source='document.doc_ref', read_only=True)
    document_title = serializers.CharField(source='document.title', read_only=True)
    submitted_by_username = serializers.CharField(source='submitted_by.username', read_only=True)
    review_status_display = serializers.CharField(source='get_review_status_display', read_only=True)
    
    class Meta:
        model = SubmittedPage
        fields = (
            'id', 'document_ref', 'document_title', 'page_number',
            'submitted_by_username', 'output_page_file', 'final_text',
            'processing_duration', 'review_status', 'review_status_display',
            'reviewed_at', 'review_notes', 'submitted_at'
        )

class RejectedPageSerializer(serializers.ModelSerializer):
    document_ref = serializers.CharField(source='document.doc_ref', read_only=True)
    rejected_by_username = serializers.CharField(source='rejected_by.username', read_only=True)
    rejection_reason_display = serializers.CharField(source='get_rejection_reason_display', read_only=True)
    
    class Meta:
        model = RejectedPage
        fields = (
            'id', 'document_ref', 'page_number', 'rejected_by_username',
            'rejection_reason', 'rejection_reason_display', 'rejection_notes', 'rejected_at',
            'reassign_status', 'is_reassigned' # Corrected field names from model
        )

class ReassignmentLogSerializer(serializers.ModelSerializer):
    reassigned_by_username = serializers.CharField(source='reassigned_by.username', read_only=True)
    previous_resource_username = serializers.CharField(source='previous_resource.user.username', read_only=True)
    new_resource_username = serializers.CharField(source='new_resource.user.username', read_only=True)
    
    class Meta:
        model = ReassignmentLog
        fields = (
            'id', 'reassigned_by_username', 'reason', 'notes',
            'previous_resource_username', 'new_resource_username', 'created_at'
        )

class AdminWorkspaceActionSerializer(serializers.Serializer):
    action = serializers.ChoiceField(choices=['approve', 'reject'])
    reason = serializers.CharField(required=False, allow_blank=True) # E.g., QUALITY_FAIL
    notes = serializers.CharField(required=False, allow_blank=True)
    
class StartProcessingSerializer(serializers.Serializer):
    """Empty serializer for starting processing (POST only)"""
    pass

class SubmitProcessingSerializer(serializers.Serializer):
    """Empty because the actual payload is sent via WebSockets in this system, 
       this just triggers the state transition."""
    pass
