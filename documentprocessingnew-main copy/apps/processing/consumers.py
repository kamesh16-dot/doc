import json
from channels.generic.websocket import AsyncWebsocketConsumer
from asgiref.sync import sync_to_async
from django.utils import timezone
from common.enums import UserRole

class WorkspaceConsumer(AsyncWebsocketConsumer):
    """
    Handles real-time document editing sync for a specific page.
    """
    async def connect(self):
        self.doc_ref = self.scope['url_route']['kwargs']['doc_ref']
        self.user = self.scope["user"]
        
        if not self.user.is_authenticated:
            await self.close(code=4003)
            return

        # Room name is now specific to the user-document block
        # This allows all pages assigned to this user for this doc to sync in one room
        self.room_group_name = f'workspace_{self.doc_ref}_{self.user.id}'

        # Join room group
        await self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name
        )
        await self.accept()

    async def disconnect(self, close_code):
        if hasattr(self, 'room_group_name'):
            await self.channel_layer.group_discard(
                self.room_group_name,
                self.channel_name
            )

    async def receive(self, text_data):
        text_data_json = json.loads(text_data)
        content = text_data_json.get('content', '')
        action = text_data_json.get('action')

        if action == 'save':
            # Save to Database (Bulk)
            await self.save_content_bulk(content)
            
            # Broadcast confirmation
            await self.send(text_data=json.dumps({
                'type': 'save_confirmation',
                'status': 'success'
            }))
            
            # Broadcast to others (e.g., if an admin is watching)
            await self.channel_layer.group_send(
                self.room_group_name,
                {
                    'type': 'workspace_update',
                    'content': content,
                    'sender_channel': self.channel_name
                }
            )

    async def workspace_update(self, event):
        if self.channel_name != event.get('sender_channel'):
            await self.send(text_data=json.dumps({
                'type': 'remote_update',
                'content': event['content']
            }))

    @sync_to_async
    def save_content_bulk(self, consolidated_content):
        """
        Parses the consolidated HTML string from the frontend workspace 
        and updates individual Page records.
        """
        from apps.documents.models import Page
        from django.db.models import F
        from django.utils import timezone
        from bs4 import BeautifulSoup
        import logging
        
        logger = logging.getLogger(__name__)

        try:
            soup = BeautifulSoup(consolidated_content, 'html.parser')
            # Extract individual page blocks (<div class="editor-page" data-page-id="...">)
            page_blocks = soup.find_all('div', class_='editor-page')
            
            is_admin = self.user.role == UserRole.ADMIN or self.user.is_superuser or self.user.is_staff
            from apps.processing.models import PageAssignment
            from common.enums import PageAssignmentStatus
            
            for block in page_blocks:
                p_id = block.get('data-page-id')
                if not p_id: continue
                
                # Security Check: Resource must have an active assignment for this page
                if not is_admin:
                    has_permission = PageAssignment.objects.filter(
                        page_id=p_id,
                        resource__user=self.user,
                        status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
                    ).exists()
                    if not has_permission:
                        logger.warning(f"User {self.user.username} attempted to bulk-save unassigned/inactive page {p_id}. Skipping.")
                        continue

                # Update the specific page text content
                Page.objects.filter(
                    id=p_id,
                    document__doc_ref=self.doc_ref
                ).update(
                    text_content=str(block),
                    version=F('version') + 1,
                    updated_at=timezone.now()
                )
                
        except Exception as e:
            logger.error(f"WebSocket Bulk Save Error: {e}")


class NotificationConsumer(AsyncWebsocketConsumer):
    """
    Handles system-wide notifications (Time warnings, new assignments, admin alerts).
    Groups:
    - 'user_{user_id}': Personal notifications
    - 'admin_broadcast': Global admin alerts
    """
    async def connect(self):
        # 1. Grab user from middleware scope
        self.user = self.scope.get("user")
        
        # 2. Safety Check: Must be logged in
        if not self.user or not self.user.is_authenticated:
            # Reject immediately with 4003 for Unauthorized
            await self.close(code=4003) 
            return

        # 3. Accept early to finish WS handshake and prevent client timeouts
        await self.accept()

        # 4. Join Personal Notification Group
        self.personal_group = f'user_{self.user.id}'
        await self.channel_layer.group_add(
            self.personal_group,
            self.channel_name
        )

        # 5. Join Admin Broadcast if applicable
        if (self.user.role == UserRole.ADMIN or self.user.is_superuser):
            await self.channel_layer.group_add('admin_broadcast', self.channel_name)

    async def disconnect(self, close_code):
        # Cleanup groups
        try:
            if hasattr(self, 'personal_group'):
                await self.channel_layer.group_discard(self.personal_group, self.channel_name)
            
            # Check role status carefully for cleanup
            user = getattr(self, 'user', None)
            if user and user.is_authenticated:
                if user.role == UserRole.ADMIN or user.is_superuser:
                    await self.channel_layer.group_discard('admin_broadcast', self.channel_name)
                
                # Immediate Offline Status Tracking
                await self.mark_user_offline()
        except Exception as e:
            # Prevent disconnect logic from being fatal or noisy
            pass

    @sync_to_async
    def mark_user_offline(self):
        from django.core.cache import cache
        from apps.accounts.models import User, ResourceProfile
        
        # Guard: Ensure we have a valid User ID
        user_id = getattr(self.user, 'id', None)
        if not user_id: return

        # Clear online status from cache
        cache.delete(f"user:{user_id}:online")
        
        # Update User last_activity and Resource status atomically
        try:
            # We use filter(...).update() because it's safer against concurrent disconnects
            User.objects.filter(pk=user_id).update(last_activity=None)
            
            # If resource, update profile status to INACTIVE
            # Using filter(...).update() to avoid fetching and potentially hitting a stale object
            ResourceProfile.objects.filter(user_id=user_id).update(
                status='INACTIVE',
                is_available=False
            )
        except Exception:
            pass # Silent fail inside disconnect state is standard for stability

    async def system_notification(self, event):
        """
        Generic handler for sending JSON notifications to the client.
        Event should strictly contain a 'payload' dict.
        """
        await self.send(text_data=json.dumps(event['payload']))

def broadcast_admin_update(message):
    """
    Synchronous helper to broadcast a notification to the admin_broadcast group.
    """
    from asgiref.sync import async_to_sync
    from channels.layers import get_channel_layer
    
    channel_layer = get_channel_layer()
    if channel_layer:
        async_to_sync(channel_layer.group_send)(
            'admin_broadcast',
            {
                'type': 'system_notification',
                'payload': {
                    'type': 'admin_update',
                    'message': message,
                    'timestamp': str(timezone.now())
                }
            }
        )

def send_timeout_notification(user_id, page_number):
    """
    Notifies a resource that their assignment has been revoked due to timeout.
    """
    from asgiref.sync import async_to_sync
    from channels.layers import get_channel_layer
    
    channel_layer = get_channel_layer()
    if channel_layer:
        async_to_sync(channel_layer.group_send)(
            f'user_{user_id}',
            {
                'type': 'system_notification',
                'payload': {
                    'type': 'assignment_timeout',
                    'message': f"Assignment for Page {page_number} has timed out and been reassigned.",
                    'page_number': page_number
                }
            }
        )

def send_time_warning(user_id, page_number, remaining_seconds):
    """
    Sends a warning when an assignment is approaching its SLA limit.
    """
    from asgiref.sync import async_to_sync
    from channels.layers import get_channel_layer
    
    channel_layer = get_channel_layer()
    if channel_layer:
        async_to_sync(channel_layer.group_send)(
            f'user_{user_id}',
            {
                'type': 'system_notification',
                'payload': {
                    'type': 'time_warning',
                    'message': f"Page {page_number} SLA warning: {int(remaining_seconds / 60)} minutes remaining.",
                    'page_number': page_number,
                    'remaining_seconds': remaining_seconds
                }
            }
        )
