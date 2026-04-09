
from rest_framework import serializers
from common.enums import DocumentStatus, AssignmentStatus

class StatusTransitionValidator:
    """
    Centralized validator for Document and Assignment status transitions.
    Exposes industrial-grade guard rules.
    """
    
    # Document State Machine
    DOCUMENT_TRANSITIONS = {
        DocumentStatus.UPLOADED: [DocumentStatus.READY, DocumentStatus.FAILED, DocumentStatus.SPLITTING],
        DocumentStatus.READY: [DocumentStatus.SPLITTING, DocumentStatus.FAILED],
        DocumentStatus.SPLITTING: [DocumentStatus.ASSIGNED, DocumentStatus.FAILED],
        DocumentStatus.ASSIGNED: [DocumentStatus.IN_PROGRESS, DocumentStatus.UPLOADED],
        DocumentStatus.IN_PROGRESS: [DocumentStatus.REVIEWING, DocumentStatus.FAILED_VALIDATION, DocumentStatus.UPLOADED, DocumentStatus.COMPLETED],
        DocumentStatus.REVIEWING: [DocumentStatus.COMPLETED, DocumentStatus.FAILED_VALIDATION, DocumentStatus.IN_PROGRESS],
        DocumentStatus.FAILED_VALIDATION: [DocumentStatus.IN_PROGRESS, DocumentStatus.REVIEWING, DocumentStatus.COMPLETED],
        DocumentStatus.COMPLETED: [DocumentStatus.IN_PROGRESS], # Re-opening if rejected
    }

    # Assignment State Machine
    ASSIGNMENT_TRANSITIONS = {
        AssignmentStatus.WAITING: [AssignmentStatus.PROCESSING, AssignmentStatus.REVOKED, AssignmentStatus.EXPIRED],
        AssignmentStatus.PROCESSING: [AssignmentStatus.COMPLETED, AssignmentStatus.REVOKED, AssignmentStatus.EXPIRED, AssignmentStatus.REJECTED],
        AssignmentStatus.COMPLETED: [AssignmentStatus.REJECTED, AssignmentStatus.REVOKED],
        AssignmentStatus.REJECTED: [AssignmentStatus.WAITING], # When reassigned
        AssignmentStatus.REVOKED: [],
        AssignmentStatus.EXPIRED: [AssignmentStatus.WAITING], # When reassigned
    }

    @staticmethod
    def validate_document_transition(old_status, new_status):
        if old_status == new_status:
            return True
            
        allowed_next = StatusTransitionValidator.DOCUMENT_TRANSITIONS.get(old_status, [])
        if new_status not in allowed_next:
            raise serializers.ValidationError(
                f"Invalid Document transition from {old_status} to {new_status}"
            )
        return True

    @staticmethod
    def validate_assignment_transition(old_status, new_status):
        if old_status == new_status:
            return True
            
        allowed_next = StatusTransitionValidator.ASSIGNMENT_TRANSITIONS.get(old_status, [])
        if new_status not in allowed_next:
            raise serializers.ValidationError(
                f"Invalid Assignment transition from {old_status} to {new_status}"
            )
        return True
