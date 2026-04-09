import json
from channels.generic.websocket import AsyncWebsocketConsumer

class ConversionStatusConsumer(AsyncWebsocketConsumer):
    """
    WebSocket consumer for real-time conversion progress.
    Frontend connects to: ws://host/ws/conversion/{document_id}/
    """

    async def connect(self):
        self.document_id = self.scope['url_route']['kwargs']['document_id']
        self.group_name = f'conversion_{self.document_id}'

        # Verify authentication
        user = self.scope.get('user')
        if not user or not user.is_authenticated:
            await self.close(code=4001)
            return

        await self.channel_layer.group_add(
            self.group_name,
            self.channel_name
        )
        await self.accept()

        # Send initial state (Crucial for CELERY_TASK_ALWAYS_EAGER mode)
        from apps.documents.models import Document
        from common.enums import ConversionStatus
        try:
            from asgiref.sync import sync_to_async
            doc = await sync_to_async(Document.objects.get)(pk=self.document_id)
            
            # Map model state to event types
            event_type = 'conversion.progress'
            stage = doc.conversion_status
            if doc.conversion_status == ConversionStatus.CONVERTED:
                event_type = 'conversion.completed'
                stage = 'COMPLETED'
            elif doc.conversion_status == ConversionStatus.CONVERSION_FAILED:
                event_type = 'conversion.failed'
                stage = 'FAILED'
            elif doc.conversion_status == ConversionStatus.CONVERTING:
                 stage = 'CONVERTING'

            await self.send(text_data=json.dumps({
                'type': 'connection.established',
                'document_id': str(doc.id),
                'current_status': doc.conversion_status,
                'message': 'Connected to conversion status stream',
            }))

            # Send current progress/status immediately
            payload = {
                'document_id': str(doc.id),
                'doc_ref': doc.doc_ref,
                'stage': stage,
                'message': doc.conversion_error or 'Checking status...',
                'progress': 100 if doc.conversion_status == ConversionStatus.CONVERTED else 0
            }
            
            await self.send(text_data=json.dumps({
                'type': event_type,
                'payload': payload
            }))

        except Document.DoesNotExist:
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': 'Document not found'
            }))

    async def disconnect(self, close_code):
        if hasattr(self, 'group_name'):
            await self.channel_layer.group_discard(
                self.group_name,
                self.channel_name
            )

    # ── Event handlers (called by Celery task) ──────────

    async def conversion_started(self, event):
        await self.send(text_data=json.dumps({
            'type': 'conversion.started',
            'payload': event['payload'],
        }))

    async def conversion_progress(self, event):
        await self.send(text_data=json.dumps({
            'type': 'conversion.progress',
            'payload': event['payload'],
        }))

    async def conversion_completed(self, event):
        await self.send(text_data=json.dumps({
            'type': 'conversion.completed',
            'payload': event['payload'],
        }))

    async def conversion_failed(self, event):
        await self.send(text_data=json.dumps({
            'type': 'conversion.failed',
            'payload': event['payload'],
        }))
