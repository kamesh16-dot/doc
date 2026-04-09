
from rest_framework import serializers
from django.contrib.auth import get_user_model
from apps.accounts.models import ResourceProfile, AdminProfile, ClientProfile

User = get_user_model()

class AdminProfileSerializer(serializers.ModelSerializer):
    class Meta:
        model = AdminProfile
        fields = ('id', 'managed_by', 'created_at')

class ClientProfileSerializer(serializers.ModelSerializer):
    class Meta:
        model = ClientProfile
        fields = ('id', 'company_name', 'created_at')

class UserSerializer(serializers.ModelSerializer):
    admin_profile = AdminProfileSerializer(read_only=True)
    client_profile = ClientProfileSerializer(read_only=True)
    resource_profile = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = ('id', 'username', 'email', 'role', 'is_active', 'is_online', 'last_activity', 'admin_profile', 'client_profile', 'resource_profile')
        read_only_fields = ('role',)

    def get_resource_profile(self, obj):
        if obj.role == 'RESOURCE' and hasattr(obj, 'resource_profile'):
            return ResourceProfileSerializer(obj.resource_profile).data
        return None

class ResourceProfileSerializer(serializers.ModelSerializer):
    username = serializers.CharField(source='user.username', read_only=True)
    is_online = serializers.BooleanField(source='user.is_online', read_only=True)
    
    class Meta:
        model = ResourceProfile
        fields = ('id', 'username', 'priority', 'max_capacity', 'is_active_for_assignment', 'active_load', 'is_online')
        read_only_fields = ('active_load', 'is_online')

class RegisterSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True, min_length=8)
    password_confirm = serializers.CharField(write_only=True)
    default_page_capacity = serializers.IntegerField(write_only=True, required=False, min_value=1)

    class Meta:
        model = User
        fields = ('username', 'password', 'password_confirm', 'email', 'role', 'default_page_capacity')

    def validate(self, data):
        if data.get('password') != data.get('password_confirm'):
            raise serializers.ValidationError({"password_confirm": "Passwords do not match."})
        return data

    def create(self, validated_data):
        password_confirm = validated_data.pop('password_confirm', None)
        capacity = validated_data.pop('default_page_capacity', 10)
        role = validated_data.get('role', 'CLIENT')
        
        user = User.objects.create_user(
            username=validated_data['username'],
            password=validated_data['password'],
            email=validated_data.get('email', ''),
            role=validated_data['role']
        )
        
        # If it's a resource, update the profile created by signal
        if user.role == 'RESOURCE':
            profile = user.resource_profile
            profile.max_capacity = capacity
            profile.save()
            
        return user

class PasswordChangeSerializer(serializers.Serializer):
    old_password = serializers.CharField(required=True)
    new_password = serializers.CharField(required=True, min_length=8)
    confirm_password = serializers.CharField(required=True)

    def validate(self, data):
        if data['new_password'] != data['confirm_password']:
            raise serializers.ValidationError({"confirm_password": "New passwords do not match."})
        return data
