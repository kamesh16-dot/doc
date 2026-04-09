from rest_framework import serializers
from .models import DesktopDevice, AssignmentBundle


class DesktopDeviceSerializer(serializers.ModelSerializer):
    class Meta:
        model = DesktopDevice
        fields = [
            "id",
            "device_name",
            "machine_id",
            "agent_version",
            "last_heartbeat_at",
            "is_active",
        ]


class AssignmentBundleSerializer(serializers.ModelSerializer):
    """
    Serializes an assignment bundle for the Desktop Agent API with strict-flow metadata.
    """
    page_count = serializers.SerializerMethodField()
    bundle_page_map = serializers.SerializerMethodField()
    document_id = serializers.PrimaryKeyRelatedField(source='document', read_only=True)
    job_id = serializers.PrimaryKeyRelatedField(source='job', read_only=True)
    user_id = serializers.PrimaryKeyRelatedField(source='user', read_only=True)

    class Meta:
        model = AssignmentBundle
        fields = [
            "id",
            "document_id",
            "job_id",
            "bundle_index",
            "page_start",
            "page_end",
            "page_numbers",
            "page_count",
            "bundle_page_map",
            "status",
            "user_id",
            "lease_token",
            "lease_expires_at",
            "manifest",
            "source_sha256",
        ]

    def get_page_count(self, obj):
        return len(obj.page_numbers) if obj.page_numbers else 0

    def get_bundle_page_map(self, obj):
        # Generate identity map for standard bundles
        if not obj.page_numbers:
            return {}
        return {i + 1: p for i, p in enumerate(obj.page_numbers)}
