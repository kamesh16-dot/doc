from django.db import models
from django.contrib.auth.models import AbstractUser
from django.db.models import Sum, Q
from django.db.models.functions import Coalesce
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _
from common.enums import UserRole, AssignmentStatus, ResourceStatus
import logging

logger = logging.getLogger(__name__)

class Organization(models.Model):
    name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'docpro_organization'

    def __str__(self):
        return self.name

class User(AbstractUser):
    role = models.CharField(
        max_length=20,
        choices=UserRole.choices,
        default=UserRole.CLIENT,
        help_text=_("Role of the user in the system")
    )
    last_activity = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        db_table = 'docpro_user'

    def __str__(self):
        return f"{self.username} ({self.role})"
    
    @property
    def is_resource(self):
        return self.role == UserRole.RESOURCE

    @property
    def is_client(self):
        return self.role == UserRole.CLIENT

    @property
    def is_online(self):
        if hasattr(self, 'resource_profile'):
             return self.resource_profile.is_online
        from django.core.cache import cache
        return cache.get(f"user:{self.id}:online") == "true"

    @property
    def is_working(self):
        """Returns True if the user has any PENDING, ASSIGNED, or IN_PROGRESS assignments."""
        if hasattr(self, '_is_working'):
            return self._is_working
            
        if not self.is_resource:
            return False
        from apps.processing.models import PageAssignment
        from common.enums import PageAssignmentStatus
        return PageAssignment.objects.filter(
            resource__user=self,
            status__in=[
                PageAssignmentStatus.ASSIGNED,
                PageAssignmentStatus.IN_PROGRESS
            ]
        ).exists()

class AdminProfile(models.Model):
    user = models.OneToOneField(
        User,
        on_delete=models.CASCADE,
        related_name='admin_profile',
        limit_choices_to={'role': UserRole.ADMIN}
    )
    managed_by = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='sub_admins'
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'docpro_admin'
        verbose_name = _("Admin Profile")
        verbose_name_plural = _("Admin Profiles")

    def __str__(self):
        return f"Admin: {self.user.username}"

class ClientProfile(models.Model):
    user = models.OneToOneField(
        User,
        on_delete=models.CASCADE,
        related_name='client_profile',
        limit_choices_to={'role': UserRole.CLIENT}
    )
    company_name = models.CharField(max_length=255, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'docpro_client'
        verbose_name = _("Client Profile")
        verbose_name_plural = _("Client Profiles")

    def __str__(self):
        return f"Client: {self.user.username}"

class ResourceProfile(models.Model):
    user = models.OneToOneField(
        User, 
        on_delete=models.CASCADE, 
        related_name='resource_profile',
        limit_choices_to={'role': UserRole.RESOURCE}
    )
    organization = models.ForeignKey(
        'Organization', 
        on_delete=models.CASCADE, 
        null=True, 
        blank=True, 
        related_name='resources'
    )
    priority = models.PositiveIntegerField(
        default=1,
        help_text=_("Higher number means higher priority for assignment")
    )
    max_capacity = models.PositiveIntegerField(
        default=10,
        help_text=_("Max pages per cycle")
    )
    avg_processing_time = models.FloatField(
        default=0.0,
        help_text=_("Average processing time in seconds")
    )
    is_active_for_assignment = models.BooleanField(
        default=True,
        help_text=_("If False, no new pages will be assigned")
    )
    is_active = models.BooleanField(default=True)
    rejection_count = models.PositiveIntegerField(default=0, help_text=_("Number of assignments rejected for this resource"))
    reliability_score = models.FloatField(default=100.0, help_text=_("Resource quality score (0-100)"))
    created_at = models.DateTimeField(auto_now_add=True)
    
    # Heartbeat and Online Status
    last_seen = models.DateTimeField(null=True, blank=True)
    last_seen_page = models.CharField(max_length=255, blank=True)

    # Thresholds for online detection
    ONLINE_THRESHOLD_SECONDS = 30
    AWAY_THRESHOLD_SECONDS = 120

    # ── Section 1: Login Availability & Active Status ──────────
    is_available = models.BooleanField(
        default=False,
        help_text=_("True when resource is online and available for assignment")
    )
    last_login_at = models.DateTimeField(
        null=True, blank=True,
        help_text=_("Timestamp of last login")
    )
    last_active_at = models.DateTimeField(
        null=True, blank=True,
        help_text=_("Updated every 5 min via heartbeat")
    )
    status = models.CharField(
        max_length=10,
        choices=ResourceStatus.choices,
        default=ResourceStatus.INACTIVE,
        db_index=True,
        help_text=_("ACTIVE=online, INACTIVE=offline, BUSY=at capacity")
    )

    @property
    def online_status(self):
        """
        Returns: 'online' | 'away' | 'offline'
        online  → heartbeat within 30s
        away    → heartbeat within 2min
        offline → no heartbeat > 2min
        """
        if self.status == ResourceStatus.INACTIVE or not self.last_seen:
            return 'offline'
        from django.utils import timezone
        now = timezone.now()
        diff = (now - self.last_seen).total_seconds()
        if diff <= self.ONLINE_THRESHOLD_SECONDS:
            return 'online'
        if diff <= self.AWAY_THRESHOLD_SECONDS:
            return 'away'
        return 'offline'

    @property
    def is_online(self):
        return self.online_status == 'online'

    def get_current_load(self):
        return self.current_load

    def get_remaining_capacity(self):
        return self.remaining_capacity
    # ✅ Computed properties (always fresh from DB)
    @property
    def current_load(self):
        """
        Always compute from actual active assignments.
        Never rely on a stored counter that can drift.
        """
        from apps.processing.models import PageAssignment
        from common.enums import PageAssignmentStatus
        
        result = PageAssignment.objects.filter(
            resource=self,
            status__in=[
                PageAssignmentStatus.ASSIGNED, 
                PageAssignmentStatus.IN_PROGRESS
            ]
        ).aggregate(
            total_weight=Coalesce(Sum('page__complexity_weight'), 0, output_field=models.PositiveIntegerField())
        )
        return int(result['total_weight'])

    @property
    def remaining_capacity(self):
        return max(0, int(self.max_capacity) - self.current_load)

    @property
    def assigned_page_count(self):
        from apps.processing.models import PageAssignment
        from common.enums import PageAssignmentStatus
        return PageAssignment.objects.filter(
            resource=self,
            status__in=[
                PageAssignmentStatus.ASSIGNED, 
                PageAssignmentStatus.IN_PROGRESS
            ]
        ).count()

    @property
    def can_accept_work(self):
        return (
            self.status == ResourceStatus.ACTIVE
            and self.is_available
            and self.remaining_capacity > 0
        )

    def refresh_status(self):
        """
        Recalculate and save status based on current load.
        Call this after any capacity or assignment change.
        """
        if self.status == ResourceStatus.INACTIVE:
            return  # Don't change offline resources member status

        if self.remaining_capacity <= 0:
            new_status = ResourceStatus.BUSY
        else:
            new_status = ResourceStatus.ACTIVE

        if self.status != new_status:
            ResourceProfile.objects.filter(pk=self.pk).update(status=new_status)
            self.status = new_status
    
    @property
    def active_load(self):
        """
        Dynamically calculates active load (assigned page count) for this resource.
        NOTE: This is used by the frontend dashboard.
        """
        from apps.processing.models import PageAssignment
        from common.enums import PageAssignmentStatus
        
        return PageAssignment.objects.filter(
            resource=self,
            status__in=[
                PageAssignmentStatus.ASSIGNED, 
                PageAssignmentStatus.IN_PROGRESS
            ]
        ).count()
    
    class Meta:
        db_table = 'docpro_resource'
        ordering = ['-priority', 'id']
        indexes = [
            models.Index(fields=['is_active_for_assignment', '-priority', 'id']),
        ]

    def __str__(self):
        return f"Resource: {self.user.username} (Pri: {self.priority})"

# Signals for Profile Creation
from django.db.models.signals import post_save
from django.dispatch import receiver

@receiver(post_save, sender=User)
def create_user_profile(sender, instance, created, **kwargs):
    if created:
        if instance.role == UserRole.ADMIN or instance.is_superuser:
            AdminProfile.objects.get_or_create(user=instance)
        elif instance.role == UserRole.CLIENT:
            ClientProfile.objects.get_or_create(user=instance)
        elif instance.role == UserRole.RESOURCE:
            ResourceProfile.objects.get_or_create(user=instance)

@receiver(post_save, sender=User)
def save_user_profile(sender, instance, **kwargs):
    if instance.role == UserRole.ADMIN or instance.is_superuser:
        if hasattr(instance, 'admin_profile'):
            instance.admin_profile.save()
    elif instance.role == UserRole.CLIENT:
        if hasattr(instance, 'client_profile'):
            instance.client_profile.save()
    elif instance.role == UserRole.RESOURCE:
        if hasattr(instance, 'resource_profile'):
            instance.resource_profile.save()
