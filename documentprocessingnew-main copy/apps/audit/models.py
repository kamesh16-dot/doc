from django.db import models
from django.conf import settings
from django.utils.translation import gettext_lazy as _
from common.enums import AuditEventType

class AuditLog(models.Model):
    organization = models.ForeignKey('accounts.Organization', on_delete=models.SET_NULL, null=True, blank=True)
    document_id = models.UUIDField(null=True, blank=True)
    assignment_id = models.BigIntegerField(null=True, blank=True)
    actor = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='audit_logs'
    )
    
    action = models.CharField(
        max_length=50,
        choices=AuditEventType.choices
    )
    old_status = models.CharField(max_length=50, null=True, blank=True)
    new_status = models.CharField(max_length=50, null=True, blank=True)
    
    metadata = models.JSONField(
        default=dict, 
        blank=True,
        help_text=_("Additional context for the event")
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'docpro_audit'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['document_id', '-created_at']),
        ]

    def __str__(self):
        return f"{self.action} on doc: {self.document_id}"
