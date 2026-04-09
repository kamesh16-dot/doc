from rest_framework import serializers
from apps.audit.models import AuditLog

class AuditLogSerializer(serializers.ModelSerializer):
    actor_name = serializers.ReadOnlyField(source='actor.username')
    action_display = serializers.CharField(source='get_action_display', read_only=True)

    class Meta:
        model = AuditLog
        fields = [
            'id', 'action', 'action_display', 'document_id', 'assignment_id', 
            'actor', 'actor_name', 'old_status', 'new_status', 'created_at', 'metadata'
        ]
